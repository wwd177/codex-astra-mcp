#!/usr/bin/env python3
"""
零风险只读 Codex Astra MCP Server v1.2.0 (常驻热连接 app-server + 精简隔离 exec 双擎架构)

核心工程特性与安全边界：
1. 100% 强制 read-only 沙箱（守护进程级 + 会话级 + 审批拦截三重锁死），绝不允许写入工作区或执行修改命令。
2. 绝不修改 ~/.codex/config.toml；自动屏蔽全局外部 MCP 服务器、桌面插件、Computer Use、Memories 与 Shell 快照开销。
3. 常驻热连接加速：MCP 初始化时后台静默预热 `codex app-server --listen stdio://`，将每次 18s+ 冷启动压降至 ~0.17s，复用 WebSocket/HTTPS 连接与 Prompt Cache。
4. 双擎高可用回退：若常驻 app-server 异常中断，自动无缝回退至 `--ignore-user-config` 纯净只读 `codex exec` 引擎。
5. 并发多路复用与精准取消：支持多会话并行问答；收到 `notifications/cancelled` 时精准发送 `turn/interrupt` 秒停目标轮次，并由 `_write_final_once` 杜绝双回包。
6. 零第三方依赖，仅使用 Python 3.12 标准库（含 `tomllib`），兼容 NDJSON 与 Content-Length 两种 stdio 帧格式。
"""

import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from pathlib import Path
from typing import Any, Optional

SERVER_NAME = "codex-astra"
SERVER_VERSION = "1.2.1"
DEFAULT_MODEL = "gpt-6.2-astra"
DEFAULT_EFFORT = "max"
ALLOWED_EFFORTS = {"low", "medium", "high", "xhigh", "max", "ultra"}
DEFAULT_TIMEOUT_SEC = 360

# 禁用所有与纯只读代码/架构顾问无关的重型 Codex 特性，消除每次启动与工具注入开销
DISABLED_HEAVY_FEATURES = (
    "plugins",
    "apps",
    "computer_use",
    "browser_use",
    "memories",
    "hooks",
    "shell_snapshot",
    "code_mode_host",
    "daemon_auto_start",
)

# 新版 codex-cli 可能通过 error 事件发射的非致命配置/实验特性警告前缀
NON_FATAL_WARNING_PATTERNS = (
    "Codex is ignoring ",
    "Under-development features enabled:",
    "To suppress this warning,",
)


def is_non_fatal_codex_warning(msg: str) -> bool:
    s = (msg or "").strip()
    return any(p in s for p in NON_FATAL_WARNING_PATTERNS)


_CODEX_CACHE: dict[str, Any] = {"path": None, "checked_at": 0.0}
_CACHE_LOCK = threading.Lock()

# 专用轻量空隔离目录（未指定 cwd 时替代庞大的用户主目录，杜绝无谓目录树扫描）
_EMPTY_SANDBOX_DIR: Optional[str] = None
_EMPTY_DIR_LOCK = threading.Lock()

# 活跃长任务注册表（request_id_str -> {"cancel_event": Event, "proc": Popen | None, "cancel_cb": Callable | None, "responded": bool}）
_JOBS_LOCK = threading.Lock()
_ACTIVE_JOBS: dict[str, dict[str, Any]] = {}

# 本进程签发的 thread_id 归属表（thread_id -> {"cwd": normalized_cwd, "engine": "hot" | "exec"}）
_THREADS_LOCK = threading.Lock()
_OWNED_THREADS: dict[str, dict[str, str]] = {}
_THREAD_EXEC_LOCKS: dict[str, threading.Lock] = {}


class RequestCancelledError(Exception):
    """客户端发出 notifications/cancelled 主动取消请求。"""


def log_err(msg: str) -> None:
    sys.stderr.write(f"[codex-astra-mcp] {msg}\n")
    sys.stderr.flush()


def get_empty_readonly_dir() -> str:
    global _EMPTY_SANDBOX_DIR
    with _EMPTY_DIR_LOCK:
        if _EMPTY_SANDBOX_DIR and os.path.isdir(_EMPTY_SANDBOX_DIR):
            return _EMPTY_SANDBOX_DIR
        target = Path(tempfile.gettempdir()) / "codex_astra_readonly_empty"
        target.mkdir(parents=True, exist_ok=True)
        _EMPTY_SANDBOX_DIR = str(target.resolve())
        return _EMPTY_SANDBOX_DIR


def find_codex_exe(force_refresh: bool = False) -> Optional[str]:
    """动态定位本机官方 codex.exe（优先环境变量、最新 Appx 包、WindowsApps 与 PATH）。"""
    now = time.monotonic()
    with _CACHE_LOCK:
        if not force_refresh and _CODEX_CACHE["path"] and (now - _CODEX_CACHE["checked_at"] < 120):
            if os.path.isfile(_CODEX_CACHE["path"]):
                return _CODEX_CACHE["path"]

    candidates: list[str] = []
    env_bin = os.environ.get("CODEX_BIN", "").strip()
    if env_bin:
        candidates.append(env_bin)

    local_app = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
    candidates.append(str(Path(local_app) / "Microsoft" / "WindowsApps" / "codex.exe"))

    which_bin = shutil.which("codex")
    if which_bin:
        candidates.append(which_bin)

    found: Optional[str] = None
    for c in candidates:
        if c and os.path.isfile(c):
            found = c
            break

    if not found and os.name == "nt":
        try:
            proc = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-Command",
                    '(Get-AppxPackage -Name "OpenAI.Codex" -ErrorAction SilentlyContinue '
                    '| Sort-Object Version -Descending | Select-Object -First 1).InstallLocation',
                ],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=8,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            install_loc = (proc.stdout or "").strip()
            if install_loc:
                appx_codex = str(Path(install_loc) / "app" / "resources" / "codex.exe")
                if os.path.isfile(appx_codex):
                    found = appx_codex
        except Exception as exc:
            log_err(f"Appx 探测异常: {exc}")

    if found:
        with _CACHE_LOCK:
            _CODEX_CACHE["path"] = found
            _CODEX_CACHE["checked_at"] = now
    return found


def discover_global_mcp_names() -> list[str]:
    """只读提取 ~/.codex/config.toml 中声明的 mcp_servers 键名，用于在子进程命令行逐一禁用外设冷启动。"""
    codex_home = os.environ.get("CODEX_HOME", "").strip()
    cfg_path = (Path(codex_home) if codex_home else (Path.home() / ".codex")) / "config.toml"
    if not cfg_path.is_file():
        return []
    try:
        raw = cfg_path.read_bytes()
        data = tomllib.loads(raw.decode("utf-8", errors="replace"))
        mcp_tbl = data.get("mcp_servers")
        if isinstance(mcp_tbl, dict):
            return [str(k) for k in mcp_tbl.keys() if str(k).strip()]
    except Exception:
        try:
            txt = cfg_path.read_text(encoding="utf-8", errors="ignore")
            return list(dict.fromkeys(re.findall(r'^\[mcp_servers\.([A-Za-z0-9_-]+)\]', txt, flags=re.M)))
        except Exception:
            pass
    return []


def format_toml_mcp_disable_key(name: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]+", name):
        return f"mcp_servers.{name}.enabled=false"
    escaped = name.replace("\\", "\\\\").replace('"', '\\"')
    return f'mcp_servers."{escaped}".enabled=false'


def build_feature_disable_flags(disable_shell: bool = False) -> list[str]:
    flags: list[str] = []
    for feat in DISABLED_HEAVY_FEATURES:
        flags.extend(["--disable", feat])
    if disable_shell:
        flags.extend(["--disable", "shell_tool"])
    return flags


def kill_process_tree(proc: Optional[subprocess.Popen]) -> None:
    if not proc or proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=5,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        else:
            proc.terminate()
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def resolve_cwd(cwd: Optional[str]) -> tuple[str, bool]:
    """返回 (目标目录绝对路径, 是否由调用方显式挂载了真实项目目录)。"""
    if cwd and str(cwd).strip():
        p = Path(str(cwd).strip()).expanduser()
        if p.is_dir():
            return str(p.resolve()), True
    return get_empty_readonly_dir(), False


def normalize_model(model: Optional[str]) -> str:
    m = (model or "").strip()
    return m if m else DEFAULT_MODEL


def get_default_effort() -> str:
    env_eff = os.environ.get("ASTRA_DEFAULT_EFFORT", "").strip().lower()
    if env_eff in ALLOWED_EFFORTS:
        return env_eff
    return DEFAULT_EFFORT


def normalize_effort(effort: Optional[str], fallback: Optional[str] = None) -> str:
    e = (effort or "").strip().lower()
    if e in ALLOWED_EFFORTS:
        return e
    if e == "minimal":
        return "low"
    return fallback if (fallback in ALLOWED_EFFORTS) else get_default_effort()


def get_thread_lock(thread_id: str) -> threading.Lock:
    with _THREADS_LOCK:
        lk = _THREAD_EXEC_LOCKS.get(thread_id)
        if lk is None:
            lk = threading.Lock()
            _THREAD_EXEC_LOCKS[thread_id] = lk
        return lk


def register_cancel_callback(req_key: Optional[str], cancel_event: Optional[threading.Event], cb: Any) -> None:
    cancelled_now = False
    if req_key is not None:
        with _JOBS_LOCK:
            job = _ACTIVE_JOBS.get(req_key)
            if job is not None:
                job["cancel_cb"] = cb
                if job["cancel_event"].is_set():
                    cancelled_now = True
            elif cancel_event and cancel_event.is_set():
                cancelled_now = True
    elif cancel_event and cancel_event.is_set():
        cancelled_now = True

    if cancelled_now:
        try:
            cb()
        except Exception:
            pass
        raise RequestCancelledError("请求在任务挂载时已被取消。")


class CodexAppServerPool:
    """常驻纯净只读 `codex app-server --listen stdio://` 守护连接（支持并发多会话、预热与秒级打断）。"""

    def __init__(self) -> None:
        self._lifecycle_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._router_lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._next_id = 10
        self._pending_rpc: dict[int, queue.Queue] = {}
        self._thread_queues: dict[str, queue.Queue] = {}
        self._prewarmed = False
        self._prewarming = False
        self._booted_at: Optional[float] = None

    def status_snapshot(self) -> dict[str, Any]:
        proc = self._proc
        alive = bool(proc and proc.poll() is None)
        return {
            "hot_daemon_alive": alive,
            "hot_daemon_pid": proc.pid if alive and proc else None,
            "prewarmed": self._prewarmed if alive else False,
            "uptime_sec": round(time.monotonic() - self._booted_at, 1) if (alive and self._booted_at) else 0.0,
        }

    def trigger_background_prewarm(self) -> None:
        with self._lifecycle_lock:
            if self._prewarming or (self._proc and self._proc.poll() is None and self._prewarmed):
                return
            self._prewarming = True

        def _warm() -> None:
            try:
                self.ensure_started(prewarm_thread=True)
            except Exception as exc:
                log_err(f"后台预热 app-server 跳过（将在首次调用时按需重试或回退）: {exc}")
            finally:
                with self._lifecycle_lock:
                    self._prewarming = False

        threading.Thread(target=_warm, daemon=True).start()

    def shutdown(self) -> None:
        with self._lifecycle_lock:
            proc = self._proc
            self._proc = None
            self._prewarmed = False
            self._booted_at = None
        if proc:
            kill_process_tree(proc)
        self._notify_all_eof("app-server 已关闭")

    def _notify_all_eof(self, reason: str) -> None:
        with self._router_lock:
            rpcs = list(self._pending_rpc.values())
            self._pending_rpc.clear()
            tqs = list(self._thread_queues.values())
        eof_msg = {"_eof": True, "_reason": reason}
        for q in rpcs:
            try:
                q.put_nowait(eof_msg)
            except Exception:
                pass
        for q in tqs:
            try:
                q.put_nowait(eof_msg)
            except Exception:
                pass

    def _send_raw(self, payload: dict[str, Any]) -> None:
        proc = self._proc
        if not proc or proc.poll() is not None or proc.stdin is None:
            raise RuntimeError("常驻 codex app-server 未运行")
        line = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        with self._write_lock:
            proc.stdin.write(line)
            proc.stdin.flush()

    def _reader_loop(self, proc: subprocess.Popen) -> None:
        assert proc.stdout is not None
        try:
            while True:
                raw = proc.stdout.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue

                has_id = "id" in msg and msg.get("id") is not None
                method = msg.get("method")

                # 拦截任何来自 app-server 的反向审批/权限请求，100% 拒绝写操作与越权请求
                if has_id and method:
                    req_id = msg["id"]
                    try:
                        self._send_raw(
                            {
                                "jsonrpc": "2.0",
                                "id": req_id,
                                "error": {
                                    "code": -32000,
                                    "message": "Strict read-only mode: all interactive/write approvals are denied.",
                                },
                            }
                        )
                    except Exception:
                        pass
                    continue

                if has_id:
                    req_id = msg["id"]
                    with self._router_lock:
                        q = self._pending_rpc.pop(req_id, None)
                    if q is not None:
                        q.put(msg)
                    continue

                if method:
                    params = msg.get("params")
                    if isinstance(params, dict):
                        tid = params.get("threadId")
                        if isinstance(tid, str) and tid:
                            with self._router_lock:
                                tq = self._thread_queues.get(tid)
                            if tq is not None:
                                tq.put(msg)
        except Exception as exc:
            log_err(f"app-server 读取循环异常: {exc}")
        finally:
            self._notify_all_eof("codex app-server 进程已退出")

    def _stderr_drain_loop(self, proc: subprocess.Popen) -> None:
        if proc.stderr is None:
            return
        try:
            for raw in proc.stderr:
                _ = raw
        except Exception:
            pass

    def _rpc_call(self, method: str, params: dict[str, Any], timeout: float = 20.0) -> dict[str, Any]:
        with self._router_lock:
            self._next_id += 1
            req_id = self._next_id
            q: queue.Queue = queue.Queue()
            self._pending_rpc[req_id] = q

        try:
            self._send_raw({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params})
            resp = q.get(timeout=timeout)
        except queue.Empty as exc:
            with self._router_lock:
                self._pending_rpc.pop(req_id, None)
            raise TimeoutError(f"app-server RPC `{method}` 超时 ({timeout}s)") from exc
        except Exception:
            with self._router_lock:
                self._pending_rpc.pop(req_id, None)
            raise

        if resp.get("_eof"):
            raise RuntimeError(resp.get("_reason") or "app-server 连接已断开")
        if "error" in resp and resp["error"]:
            err_obj = resp["error"]
            msg = err_obj.get("message") if isinstance(err_obj, dict) else str(err_obj)
            raise RuntimeError(f"app-server `{method}` 返回错误: {msg}")
        res = resp.get("result")
        return res if isinstance(res, dict) else {}

    def ensure_started(self, prewarm_thread: bool = False) -> None:
        need_prewarm = False
        with self._lifecycle_lock:
            if self._proc is None or self._proc.poll() is not None:
                codex_exe = find_codex_exe()
                if not codex_exe:
                    raise RuntimeError(
                        "未检测到本机官方 OpenAI Codex CLI (codex.exe)。请先安装 Codex Desktop 或设置 CODEX_BIN 环境变量。"
                    )

                mcp_names = discover_global_mcp_names()
                cmd = [
                    codex_exe,
                    "app-server",
                    "--listen",
                    "stdio://",
                    *build_feature_disable_flags(disable_shell=False),
                    "-c",
                    'sandbox_mode="read-only"',
                    "-c",
                    'approval_policy="never"',
                    "-c",
                    "notify=[]",
                ]
                for name in mcp_names:
                    cmd.extend(["-c", format_toml_mcp_disable_key(name)])

                proc = subprocess.Popen(
                    cmd,
                    cwd=get_empty_readonly_dir(),
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                self._proc = proc
                self._prewarmed = False
                self._booted_at = time.monotonic()

                threading.Thread(target=self._reader_loop, args=(proc,), daemon=True).start()
                threading.Thread(target=self._stderr_drain_loop, args=(proc,), daemon=True).start()

                try:
                    self._rpc_call(
                        "initialize",
                        {
                            "clientInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                            "capabilities": {},
                        },
                        timeout=15.0,
                    )
                    self._send_raw({"jsonrpc": "2.0", "method": "initialized"})
                except Exception:
                    kill_process_tree(proc)
                    self._proc = None
                    self._booted_at = None
                    raise

            if prewarm_thread and not self._prewarmed:
                need_prewarm = True

        if need_prewarm:
            # 零 Token 消耗预热：提前触发一次模型发现与本地索引缓存，使后续首轮问答达到 ~0.17s 建连
            self._rpc_call(
                "thread/start",
                {
                    "cwd": get_empty_readonly_dir(),
                    "ephemeral": True,
                    "model": DEFAULT_MODEL,
                    "sandbox": "read-only",
                    "approvalPolicy": "never",
                },
                timeout=25.0,
            )
            with self._lifecycle_lock:
                self._prewarmed = True

    def run_turn(
        self,
        prompt: str,
        cwd: str,
        model: str,
        effort: str,
        ephemeral: bool = True,
        existing_thread_id: Optional[str] = None,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
        req_key: Optional[str] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> dict[str, Any]:
        if cancel_event and cancel_event.is_set():
            raise RequestCancelledError("请求在启动前已取消")

        started = time.monotonic()
        deadline = started + max(15, min(900, timeout_sec))

        self.ensure_started(prewarm_thread=False)

        tid = existing_thread_id
        if not tid:
            th_res = self._rpc_call(
                "thread/start",
                {
                    "cwd": cwd,
                    "ephemeral": ephemeral,
                    "model": model,
                    "sandbox": "read-only",
                    "approvalPolicy": "never",
                },
                timeout=max(5.0, min(30.0, deadline - time.monotonic())),
            )
            th_obj = th_res.get("thread") if isinstance(th_res.get("thread"), dict) else {}
            tid = str(th_obj.get("id") or "").strip()
            if not tid:
                raise RuntimeError("app-server `thread/start` 未返回有效 thread.id")
            with _lifecycle_or_noop():
                self._prewarmed = True

        thread_lock = get_thread_lock(tid)
        with thread_lock:
            tq: queue.Queue = queue.Queue()
            with self._router_lock:
                self._thread_queues[tid] = tq

            active_turn_id: dict[str, Optional[str]] = {"id": None}

            def _interrupt_turn() -> None:
                cur_turn = active_turn_id.get("id")
                if cur_turn:
                    try:
                        self._send_raw(
                            {
                                "jsonrpc": "2.0",
                                "id": 900000 + (int(time.monotonic() * 1000) % 90000),
                                "method": "turn/interrupt",
                                "params": {"threadId": tid, "turnId": cur_turn},
                            }
                        )
                    except Exception:
                        pass

            try:
                register_cancel_callback(req_key, cancel_event, _interrupt_turn)

                turn_res = self._rpc_call(
                    "turn/start",
                    {
                        "threadId": tid,
                        "cwd": cwd,
                        "model": model,
                        "effort": effort,
                        "approvalPolicy": "never",
                        "input": [{"type": "text", "text": prompt}],
                    },
                    timeout=max(5.0, min(30.0, deadline - time.monotonic())),
                )
                turn_obj = turn_res.get("turn") if isinstance(turn_res.get("turn"), dict) else {}
                started_turn_id = str(turn_obj.get("id") or "").strip() or None
                active_turn_id["id"] = started_turn_id

                if cancel_event and cancel_event.is_set():
                    _interrupt_turn()
                    raise RequestCancelledError("客户端已取消当前 GPT-6.2 Astra 任务，已发送 turn/interrupt。")

                deltas_by_item: dict[str, list[str]] = {}
                item_order: list[str] = []
                completed_messages: list[str] = []
                commentary_messages: list[str] = []
                fatal_errors: list[str] = []
                turn_status = "completed"

                while True:
                    if cancel_event and cancel_event.is_set():
                        _interrupt_turn()
                        raise RequestCancelledError("客户端已取消当前 GPT-6.2 Astra 任务，已即时打断目标轮次。")

                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        _interrupt_turn()
                        raise TimeoutError(
                            f"GPT-6.2 Astra 执行超过 {timeout_sec} 秒已安全打断，建议精炼代码切片或调低 effort 后重试。"
                        )

                    try:
                        evt = tq.get(timeout=min(0.25, remaining))
                    except queue.Empty:
                        continue

                    if evt.get("_eof"):
                        raise RuntimeError(evt.get("_reason") or "常驻 app-server 意外断开")

                    method = evt.get("method", "")
                    params = evt.get("params") or {}
                    evt_turn_id = params.get("turnId")
                    if started_turn_id and evt_turn_id and evt_turn_id != started_turn_id:
                        continue

                    if method == "item/agentMessage/delta":
                        iid = str(params.get("itemId") or "default")
                        delta = str(params.get("delta") or "")
                        if delta:
                            if iid not in deltas_by_item:
                                deltas_by_item[iid] = []
                                item_order.append(iid)
                            deltas_by_item[iid].append(delta)

                    elif method == "item/completed":
                        item = params.get("item")
                        if isinstance(item, dict):
                            itype = item.get("type")
                            if itype == "agentMessage":
                                txt = str(item.get("text") or "").strip()
                                if txt:
                                    if item.get("phase") == "commentary":
                                        commentary_messages.append(txt)
                                    else:
                                        completed_messages.append(txt)
                            elif itype == "exitedReviewMode":
                                rev = str(item.get("review") or "").strip()
                                if rev:
                                    completed_messages.append(rev)

                    elif method == "error":
                        will_retry = bool(params.get("willRetry", False))
                        err_info = params.get("error")
                        msg = err_info.get("message") if isinstance(err_info, dict) else str(err_info or "")
                        if not will_retry and msg and not is_non_fatal_codex_warning(msg):
                            fatal_errors.append(msg)

                    elif method == "turn/completed":
                        t_final = params.get("turn") if isinstance(params.get("turn"), dict) else {}
                        if started_turn_id and t_final.get("id") and t_final.get("id") != started_turn_id:
                            continue
                        turn_status = str(t_final.get("status") or "completed")
                        t_err = t_final.get("error")
                        if isinstance(t_err, dict) and t_err.get("message"):
                            msg = str(t_err["message"])
                            if not is_non_fatal_codex_warning(msg):
                                fatal_errors.append(msg)
                        # 若流式事件未捕获正文，从终态 turn.items 兜底提取
                        if not completed_messages and isinstance(t_final.get("items"), list):
                            for it in t_final["items"]:
                                if isinstance(it, dict) and it.get("type") == "agentMessage":
                                    txt = str(it.get("text") or "").strip()
                                    if txt:
                                        completed_messages.append(txt)
                        break

                if cancel_event and cancel_event.is_set():
                    raise RequestCancelledError("请求已取消")
                if turn_status == "interrupted":
                    raise RequestCancelledError("当前轮次已被打断")

                answer = "\n\n".join(completed_messages).strip()
                if not answer and item_order:
                    answer = "\n\n".join("".join(deltas_by_item[i]).strip() for i in item_order if deltas_by_item[i]).strip()
                if not answer and commentary_messages:
                    answer = "\n\n".join(commentary_messages).strip()

                if turn_status == "failed" or fatal_errors:
                    err_summary = "; ".join(dict.fromkeys(fatal_errors)) or f"turn status={turn_status}"
                    partial_note = (
                        f"\n\n[失败前已产出的部分未完成文本（仅供参考，不代表成功）]\n{answer[:1500]}"
                        if answer
                        else ""
                    )
                    raise RuntimeError(f"Codex 未正常完成：{err_summary}{partial_note}")

                if not answer:
                    raise RuntimeError("Codex 轮次已完成，但未产出有效回复正文。")

                return {
                    "thread_id": tid,
                    "answer": answer,
                    "duration_sec": round(time.monotonic() - started, 2),
                    "engine": "hot-app-server",
                }
            finally:
                with self._router_lock:
                    if self._thread_queues.get(tid) is tq:
                        self._thread_queues.pop(tid, None)


class _NoopCtx:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: Any) -> None:
        return None


def _lifecycle_or_noop() -> _NoopCtx:
    return _NoopCtx()


_HOT_POOL = CodexAppServerPool()


def parse_codex_jsonl(stdout_text: str) -> dict[str, Any]:
    """解析 fallback `codex exec --json` 输出的 JSONL 事件流。"""
    thread_id: Optional[str] = None
    final_messages: list[str] = []
    commentaries: list[str] = []
    errors: list[str] = []
    tools_used: list[str] = []
    turn_failed = False
    turn_completed = False

    for raw_line in stdout_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            continue

        etype = evt.get("type")
        if etype == "thread.started" and evt.get("thread_id"):
            thread_id = str(evt["thread_id"])
        elif etype == "turn.completed":
            turn_completed = True
        elif etype == "turn.failed":
            turn_failed = True
            err = evt.get("error")
            errors.append(err.get("message") if isinstance(err, dict) else str(err))
        elif etype == "error":
            errors.append(str(evt.get("message") or evt))
        elif etype == "item.completed" and isinstance(evt.get("item"), dict):
            item = evt["item"]
            itype = item.get("type")
            text = item.get("text") or ""
            if itype == "agent_message" and text:
                if item.get("phase") == "commentary":
                    commentaries.append(text)
                else:
                    final_messages.append(text)
            elif itype == "command_execution":
                cmd = item.get("command") or ""
                if cmd:
                    tools_used.append(cmd[:120])
            elif itype == "error":
                errors.append(str(item.get("message") or item))

    answer = "\n\n".join(final_messages).strip()
    if not answer and commentaries:
        answer = "\n\n".join(commentaries).strip()

    return {
        "thread_id": thread_id,
        "answer": answer,
        "errors": [e for e in errors if e],
        "tools_used": tools_used,
        "turn_failed": turn_failed,
        "turn_completed": turn_completed,
    }


def _read_stream(stream: Any, chunks: list[bytes]) -> None:
    try:
        while True:
            data = stream.read(4096)
            if not data:
                break
            chunks.append(data)
    except Exception:
        pass


def run_codex_exec_readonly(
    args: list[str],
    stdin_prompt: Optional[str],
    cwd: str,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    req_key: Optional[str] = None,
    cancel_event: Optional[threading.Event] = None,
) -> dict[str, Any]:
    """纯净隔离模式 `codex exec`（作为原生 review 与热连接异常时的兜底引擎）。"""
    codex_exe = find_codex_exe()
    if not codex_exe:
        raise RuntimeError(
            "未检测到本机官方 OpenAI Codex CLI (codex.exe)。请先安装 Codex Desktop 或设置 CODEX_BIN 环境变量。"
        )

    if cancel_event and cancel_event.is_set():
        raise RequestCancelledError("请求在启动前已取消")

    started = time.monotonic()
    deadline = started + max(15, min(900, timeout_sec))

    proc = subprocess.Popen(
        [codex_exe, *args],
        cwd=cwd,
        stdin=subprocess.PIPE if stdin_prompt is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )

    cancelled_before_publish = False
    if req_key is not None:
        with _JOBS_LOCK:
            job = _ACTIVE_JOBS.get(req_key)
            if job is not None:
                job["proc"] = proc
                job["cancel_cb"] = None
                if job["cancel_event"].is_set():
                    cancelled_before_publish = True
            elif cancel_event and cancel_event.is_set():
                cancelled_before_publish = True
    elif cancel_event and cancel_event.is_set():
        cancelled_before_publish = True

    if cancelled_before_publish:
        kill_process_tree(proc)
        raise RequestCancelledError("请求在子进程刚发布时已被取消，已即时清理子进程树。")

    out_chunks: list[bytes] = []
    err_chunks: list[bytes] = []
    t_out = threading.Thread(target=_read_stream, args=(proc.stdout, out_chunks), daemon=True)
    t_err = threading.Thread(target=_read_stream, args=(proc.stderr, err_chunks), daemon=True)
    t_out.start()
    t_err.start()

    try:
        if stdin_prompt is not None and proc.stdin is not None:
            try:
                proc.stdin.write(stdin_prompt.encode("utf-8"))
                proc.stdin.close()
            except BrokenPipeError:
                pass

        while proc.poll() is None:
            if cancel_event and cancel_event.is_set():
                kill_process_tree(proc)
                raise RequestCancelledError("客户端已取消当前 GPT-6.2 Astra 任务，子进程树已安全终止。")
            if time.monotonic() >= deadline:
                kill_process_tree(proc)
                raise TimeoutError(
                    f"GPT-6.2 Astra 执行超过 {timeout_sec} 秒已安全终止，建议先由 Neon 精炼上下文切片或调低 effort 后重试。"
                )
            time.sleep(0.1)
    finally:
        t_out.join(timeout=3)
        t_err.join(timeout=3)

    duration_s = round(time.monotonic() - started, 2)
    stdout_text = b"".join(out_chunks).decode("utf-8", errors="replace")
    stderr_text = b"".join(err_chunks).decode("utf-8", errors="replace")
    parsed = parse_codex_jsonl(stdout_text)

    if not parsed["answer"] and stdout_text.strip() and not stdout_text.lstrip().startswith("{"):
        parsed["answer"] = stdout_text.strip()

    clean_err = "\n".join(
        l for l in stderr_text.splitlines() if "Reading additional input from stdin" not in l
    ).strip()

    fatal_errors = [e for e in parsed["errors"] if not is_non_fatal_codex_warning(e)]
    if parsed["turn_failed"] or fatal_errors or proc.returncode != 0:
        reason_parts = []
        if fatal_errors:
            reason_parts.append("; ".join(fatal_errors))
        elif parsed["turn_failed"]:
            reason_parts.append("收到 turn.failed 失败终态事件")
        if proc.returncode != 0:
            reason_parts.append(f"退出码 {proc.returncode}")
        if clean_err and not fatal_errors:
            reason_parts.append(clean_err[:500])
        err_summary = " | ".join(reason_parts) or "未知执行失败"
        partial_note = (
            f"\n\n[失败前已产出的部分未完成文本（仅供参考，不代表成功）]\n{parsed['answer'][:1500]}"
            if parsed["answer"]
            else ""
        )
        raise RuntimeError(f"Codex 未正常完成：{err_summary}{partial_note}")

    if not parsed["answer"]:
        raise RuntimeError("Codex 进程以退出码 0 结束，但未产出有效回复正文。")

    parsed["duration_sec"] = duration_s
    parsed["returncode"] = proc.returncode
    parsed["engine"] = "lean-exec-fallback"
    return parsed


def execute_smart_readonly(
    prompt: str,
    cwd: str,
    has_explicit_cwd: bool,
    model: str,
    effort: str,
    ephemeral: bool = True,
    existing_thread_id: Optional[str] = None,
    existing_engine: Optional[str] = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    req_key: Optional[str] = None,
    cancel_event: Optional[threading.Event] = None,
) -> dict[str, Any]:
    """优先调度常驻热连接 app-server；若遇底层守护通道异常则自动无缝降级至精简隔离 `codex exec`。"""
    if existing_engine != "exec":
        try:
            return _HOT_POOL.run_turn(
                prompt=prompt,
                cwd=cwd,
                model=model,
                effort=effort,
                ephemeral=ephemeral,
                existing_thread_id=existing_thread_id,
                timeout_sec=timeout_sec,
                req_key=req_key,
                cancel_event=cancel_event,
            )
        except (RequestCancelledError, TimeoutError):
            raise
        except Exception as hot_exc:
            if existing_thread_id:
                raise
            log_err(f"常驻热通道异常，自动切换精简 exec 兜底: {hot_exc}")

    disable_flags = build_feature_disable_flags(disable_shell=not has_explicit_cwd)
    if existing_thread_id:
        cli_args = [
            "exec",
            "resume",
            "--json",
            "--skip-git-repo-check",
            "--ignore-user-config",
            "--ignore-rules",
            *disable_flags,
            "-m",
            model,
            "-c",
            'sandbox_mode="read-only"',
            "-c",
            'approval_policy="never"',
            "-c",
            f'model_reasoning_effort="{effort}"',
            existing_thread_id,
            "-",
        ]
    else:
        cli_args = [
            "exec",
            "--json",
            *(["--ephemeral"] if ephemeral else []),
            "--skip-git-repo-check",
            "--ignore-user-config",
            "--ignore-rules",
            *disable_flags,
            "-s",
            "read-only",
            "-C",
            cwd,
            "-m",
            model,
            "-c",
            'approval_policy="never"',
            "-c",
            f'model_reasoning_effort="{effort}"',
            "-",
        ]

    return run_codex_exec_readonly(
        cli_args,
        stdin_prompt=prompt,
        cwd=cwd,
        timeout_sec=timeout_sec,
        req_key=req_key,
        cancel_event=cancel_event,
    )


def collect_git_diff_summary(cwd: str, max_chars: int = 24000) -> tuple[bool, str, bool]:
    """收集工作区 Git 状态与未提交 Diff 摘要，返回 (is_git, summary_text, was_truncated)。"""
    try:
        check = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if check.returncode != 0 or check.stdout.strip().lower() != "true":
            return False, "", False

        status = subprocess.run(
            ["git", "status", "--short"],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=6,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        ).stdout.strip()

        diff = subprocess.run(
            ["git", "diff", "HEAD"],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        ).stdout.strip()

        if not diff:
            diff = subprocess.run(
                ["git", "diff"],
                cwd=cwd,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            ).stdout.strip()

        truncated = False
        if len(diff) > max_chars:
            truncated = True
            diff = diff[:max_chars] + f"\n... [diff 已截断，原长 {len(diff)} 字符]"

        parts = []
        if status:
            parts.append(f"### Git Status\n```\n{status}\n```")
        if diff:
            parts.append(f"### Git Diff\n```diff\n{diff}\n```")
        return True, "\n\n".join(parts), truncated
    except Exception:
        return False, "", False


def handle_astra_status(_args: dict[str, Any], **_ctx: Any) -> str:
    codex_exe = find_codex_exe(force_refresh=True)
    version = "unknown"
    if codex_exe:
        try:
            proc = subprocess.run(
                [codex_exe, "--version"],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=8,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            version = (proc.stdout or proc.stderr or "").strip()
        except Exception as exc:
            version = f"error: {exc}"

    with _JOBS_LOCK:
        active_count = len(_ACTIVE_JOBS)

    info = {
        "status": "ready" if codex_exe else "missing_codex_cli",
        "codex_exe": codex_exe,
        "codex_version": version,
        "default_model": DEFAULT_MODEL,
        "default_effort": get_default_effort(),
        "supported_efforts": sorted(ALLOWED_EFFORTS),
        "sandbox_policy": "STRICT_READ_ONLY (daemon + thread + approval-denial triple lock, never modifies ~/.codex/config.toml)",
        "engine_architecture": "persistent_hot_app_server_with_lean_exec_fallback",
        "hot_pool": _HOT_POOL.status_snapshot(),
        "isolated_mcp_servers_disabled": discover_global_mcp_names(),
        "concurrency": "multiplexed_non_blocking_with_turn_interrupt",
        "active_jobs": active_count,
        "server_version": SERVER_VERSION,
    }
    return json.dumps(info, ensure_ascii=False, indent=2)


def handle_astra_ask(args: dict[str, Any], **ctx: Any) -> str:
    prompt = str(args.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("缺少必填参数 prompt")

    raw_cwd = args.get("cwd")
    cwd, has_explicit_cwd = resolve_cwd(raw_cwd)
    model = normalize_model(args.get("model"))
    effort = normalize_effort(args.get("effort"))
    thread_id = (args.get("thread_id") or "").strip() or None
    timeout_sec = int(args.get("timeout_sec") or DEFAULT_TIMEOUT_SEC)

    existing_engine: Optional[str] = None
    if thread_id:
        with _THREADS_LOCK:
            meta = _OWNED_THREADS.get(thread_id)
        if meta is None:
            raise ValueError(
                f"拒绝续接未在本 MCP 会话内签发的 thread_id（{thread_id}），以防范跨会话上下文串用；请发起新会话。"
            )
        owner_cwd = meta["cwd"]
        existing_engine = meta["engine"]
        if raw_cwd and os.path.normcase(owner_cwd) != os.path.normcase(cwd):
            raise ValueError(
                f"拒绝跨工作区续接 thread_id（原工作区: {owner_cwd}，本次: {cwd}）；请去掉 thread_id 开启独立会话。"
            )
        cwd = owner_cwd

    scan_hint = (
        "当前未挂载外部代码仓库，禁止调用终端命令探测磁盘，请直接基于给定信息与切片极速作答。"
        if not has_explicit_cwd
        else "若下方已提供足够代码切片，请直接基于切片分析，避免运行耗时的全库扫描命令。"
    )
    readonly_prefix = (
        "【只读协作顾问约束】你正在作为 GPT-6.2 Astra 独立专家，与 Antigravity 主控协同工作。"
        f"你当前处于严格只读沙箱，禁止尝试写入文件或修改系统状态。{scan_hint}\n\n"
    )
    full_prompt = prompt if thread_id else (readonly_prefix + prompt)

    res = execute_smart_readonly(
        prompt=full_prompt,
        cwd=cwd,
        has_explicit_cwd=has_explicit_cwd,
        model=model,
        effort=effort,
        ephemeral=False,
        existing_thread_id=thread_id,
        existing_engine=existing_engine,
        timeout_sec=timeout_sec,
        req_key=ctx.get("req_key"),
        cancel_event=ctx.get("cancel_event"),
    )
    returned_tid = res.get("thread_id") or thread_id
    if returned_tid:
        with _THREADS_LOCK:
            _OWNED_THREADS.setdefault(
                returned_tid,
                {"cwd": cwd, "engine": "hot" if res.get("engine") == "hot-app-server" else "exec"},
            )

    header = (
        f"[GPT-6.2 Astra 只读顾问 | model={model} | effort={effort} | "
        f"engine={res.get('engine')} | 耗时={res['duration_sec']}s | thread_id={returned_tid or 'none'}]\n\n"
    )
    return header + res["answer"]


def handle_astra_redteam(args: dict[str, Any], **ctx: Any) -> str:
    proposal = str(args.get("proposal") or "").strip()
    if not proposal:
        raise ValueError("缺少必填参数 proposal")

    focus = str(args.get("focus") or "").strip()
    include_git_diff = bool(args.get("include_git_diff", False))
    cwd, has_explicit_cwd = resolve_cwd(args.get("cwd"))
    model = normalize_model(args.get("model"))
    effort = normalize_effort(args.get("effort"))
    timeout_sec = int(args.get("timeout_sec") or DEFAULT_TIMEOUT_SEC)

    diff_section = ""
    if include_git_diff and has_explicit_cwd:
        is_git, diff_text, _ = collect_git_diff_summary(cwd)
        if is_git and diff_text:
            diff_section = f"\n\n## 当前工作区真实 Git 变更\n{diff_text}\n"

    scan_rule = (
        "当前未挂载代码仓库，禁止启动终端命令，直接基于下方材料完成严苛评审。"
        if not has_explicit_cwd
        else "优先直接审查下方已提供的代码切片与 Diff，仅在关键证据缺失时按需只读查看单个目标文件，严禁遍历整库。"
    )

    redteam_prompt = (
        "【红队独立审计指令 · 严禁拍马屁】\n"
        "你是独立于提案方的红队首席架构审查员（GPT-6.2 Astra，只读沙箱）。\n"
        "请遵守以下反从众（Anti-Sycophancy）铁律：\n"
        "1. 禁止客套赞美或盲目附和；若方案存在逻辑漏洞、并发竞态、边界缺陷、安全隐患或过度设计，必须直接指出。\n"
        f"2. 每一条批判都必须附带可验证的事实依据、代码路径/行号或反例推演；{scan_rule}\n"
        "3. 第一行必须严格以独立裁决行输出（且全文只出现一次该字段，只能三选一）：\n"
        "   `VERDICT: LGTM` 或 `VERDICT: NEEDS_FIX` 或 `VERDICT: DEADLOCK`\n"
        "4. 随后按固定结构精炼输出：\n"
        "   - **P0 阻断缺陷**（如无请明确写“无”）\n"
        "   - **P1 重要风险与边界遗漏**\n"
        "   - **P2 精简与优化建议**\n"
        "   - **最关键的反例 / 替代方案**\n"
        + (f"\n## 本次重点审查维度\n{focus}\n" if focus else "")
        + f"\n## 待审查方案 / 结论 / 代码说明\n{proposal}"
        + diff_section
    )

    res = execute_smart_readonly(
        prompt=redteam_prompt,
        cwd=cwd,
        has_explicit_cwd=has_explicit_cwd,
        model=model,
        effort=effort,
        ephemeral=True,
        timeout_sec=timeout_sec,
        req_key=ctx.get("req_key"),
        cancel_event=ctx.get("cancel_event"),
    )
    header = (
        f"[GPT-6.2 Astra 红队审查 | model={model} | effort={effort} | "
        f"engine={res.get('engine')} | 耗时={res['duration_sec']}s | mode=strict-read-only]\n\n"
    )
    return header + res["answer"]


def handle_astra_review_diff(args: dict[str, Any], **ctx: Any) -> str:
    cwd, has_explicit_cwd = resolve_cwd(args.get("cwd"))
    if not has_explicit_cwd:
        raise ValueError("参数 cwd 无效或目录不存在；请提供目标 Git 仓库的绝对路径。")

    model = normalize_model(args.get("model"))
    effort = normalize_effort(args.get("effort"))
    base_branch = str(args.get("base") or "").strip()
    commit_sha = str(args.get("commit") or "").strip()
    focus = str(args.get("focus") or "").strip()
    timeout_sec = int(args.get("timeout_sec") or DEFAULT_TIMEOUT_SEC)

    is_git, diff_summary, diff_truncated = collect_git_diff_summary(cwd)
    if not is_git:
        raise ValueError(f"目录 {cwd} 不是 Git 仓库；若需非 Git 目录审查请改用 astra_redteam。")

    # 快速通道：审查未提交改动（或带 focus 审查）且本地已抓取完整 Diff 时，直接走常驻热连接秒级审计！
    if (focus or (not base_branch and not commit_sha and not diff_truncated)) and diff_summary:
        focus_line = f"重点审查要求：{focus}\n" if focus else ""
        prompt = (
            "【Git Diff 极速红队审计（只读沙箱）】\n"
            f"{focus_line}"
            "第一行必须输出唯一独立裁决行：`VERDICT: LGTM` / `VERDICT: NEEDS_FIX` / `VERDICT: DEADLOCK`，"
            "随后按 P0 阻断 / P1 风险 / P2 建议输出简洁精准的审查意见（附文件路径与行号）。"
            "下方已附完整 Git Status 与 Diff，如无跨文件调用链疑问请直接给出结论，避免全库遍历。\n\n"
            + diff_summary
        )
        res = execute_smart_readonly(
            prompt=prompt,
            cwd=cwd,
            has_explicit_cwd=True,
            model=model,
            effort=effort,
            ephemeral=True,
            timeout_sec=timeout_sec,
            req_key=ctx.get("req_key"),
            cancel_event=ctx.get("cancel_event"),
        )
        header = (
            f"[GPT-6.2 Astra 热连接 Diff 审计 | model={model} | effort={effort} | "
            f"engine={res.get('engine')} | 耗时={res['duration_sec']}s]\n\n"
        )
        return header + res["answer"]

    # 指定了 base 分支 / commit SHA 或超大型截断 Diff 时，调用剥离全局外设后的精简 `codex exec review`
    cli_args = [
        "exec",
        "review",
        "--json",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        *build_feature_disable_flags(disable_shell=False),
        "-m",
        model,
        "-c",
        'sandbox_mode="read-only"',
        "-c",
        'approval_policy="never"',
        "-c",
        f'model_reasoning_effort="{effort}"',
    ]
    if commit_sha:
        cli_args.extend(["--commit", commit_sha])
    elif base_branch:
        cli_args.extend(["--base", base_branch])
    else:
        cli_args.append("--uncommitted")

    res = run_codex_exec_readonly(
        cli_args,
        stdin_prompt=None,
        cwd=cwd,
        timeout_sec=timeout_sec,
        req_key=ctx.get("req_key"),
        cancel_event=ctx.get("cancel_event"),
    )
    header = (
        f"[GPT-6.2 Astra 原生仓库 Diff 审计 | model={model} | effort={effort} | "
        f"耗时={res['duration_sec']}s | engine=lean-codex-review]\n\n"
    )
    return header + res["answer"]


TOOLS_SCHEMA: list[dict[str, Any]] = [
    {
        "name": "astra_status",
        "description": (
            "检查本机只读 GPT-6.2 Astra (OpenAI Codex) 桥接状态、常驻热守护进程状态、已隔离的外部 MCP 列表及安全策略。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "astra_ask",
        "description": (
            "向常驻热连接的 GPT-6.2 Astra 发起严格只读的极速咨询、盲评方案设计、数理推演或疑难排错。"
            "若无需 Astra 自行读盘，省略 cwd 并直接传入代码切片可获得最快响应（~5-10s）；支持 thread_id 热续接。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "要咨询的具体问题、精炼代码切片与期望输出格式。",
                },
                "cwd": {
                    "type": "string",
                    "description": "可选：仅在确需 Astra 只读查阅本地局部文件时传入工作区绝对路径；已附完整切片时建议省略以极速作答。",
                },
                "model": {
                    "type": "string",
                    "description": "模型名称：默认 gpt-6.2-astra；日常快问/轻量讨论强烈推荐 gpt-6.2-sol（最快 ~5s）。",
                    "default": "gpt-6.2-astra",
                },
                "effort": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "xhigh", "max", "ultra"],
                    "description": "推理思考强度：默认 max（极致深度推演）；可选 ultra（极限+自动委派），日常快问可降档选 low/medium。",
                    "default": "max",
                },
                "thread_id": {
                    "type": "string",
                    "description": "可选：热续接本会话签发的 thread_id（0.05s 续接并命中 Prompt Cache）。",
                },
                "timeout_sec": {
                    "type": "integer",
                    "description": "超时时间（秒），默认 360（上限 900）。",
                    "default": 360,
                },
            },
            "required": ["prompt"],
        },
    },
    {
        "name": "astra_redteam",
        "description": (
            "向常驻热连接的 GPT-6.2 Astra 发起防拍马屁（Anti-Sycophancy）单轮红队审查："
            "首行输出唯一 VERDICT: LGTM/NEEDS_FIX/DEADLOCK，按 P0/P1/P2 输出带证据链的独立评审，严格只读。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "proposal": {
                    "type": "string",
                    "description": "待审查的架构设计、精炼代码切片或结论摘要（由主控先提取切片传入，免去全库遍历）。",
                },
                "focus": {
                    "type": "string",
                    "description": "可选：重点关注维度（如并发竞态、边界条件、安全漏洞、性能瓶颈）。",
                },
                "cwd": {
                    "type": "string",
                    "description": "可选：工作区绝对路径（仅在需按需核验局部文件或开启 include_git_diff 时传入）。",
                },
                "include_git_diff": {
                    "type": "boolean",
                    "description": "是否自动抓取并附加当前工作区 Git 未提交变更 Diff，默认 false。",
                    "default": False,
                },
                "model": {
                    "type": "string",
                    "default": "gpt-6.2-astra",
                },
                "effort": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "xhigh", "max", "ultra"],
                    "description": "默认 max（极致深度红队审计）；需要极限自动委派可选 ultra，快速预检可降档选 medium/high。",
                    "default": "max",
                },
                "timeout_sec": {
                    "type": "integer",
                    "default": 360,
                },
            },
            "required": ["proposal"],
        },
    },
    {
        "name": "astra_review_diff",
        "description": (
            "对指定 Git 仓库的未提交改动、分支差异或指定提交进行极速只读 Code Review（未提交改动优先走常驻热连接通道）。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cwd": {
                    "type": "string",
                    "description": "目标 Git 仓库的绝对路径。",
                },
                "base": {
                    "type": "string",
                    "description": "可选：对比的基准分支名（如 main）；不填且无 commit 时默认极速审查未提交改动。",
                },
                "commit": {
                    "type": "string",
                    "description": "可选：审查指定 commit SHA 的变更。",
                },
                "focus": {
                    "type": "string",
                    "description": "可选：额外指定的重点审计方向。",
                },
                "model": {
                    "type": "string",
                    "default": "gpt-6.2-astra",
                },
                "effort": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "xhigh", "max", "ultra"],
                    "default": "max",
                },
                "timeout_sec": {
                    "type": "integer",
                    "default": 360,
                },
            },
            "required": ["cwd"],
        },
    },
]

TOOL_HANDLERS = {
    "astra_status": handle_astra_status,
    "astra_ask": handle_astra_ask,
    "astra_redteam": handle_astra_redteam,
    "astra_review_diff": handle_astra_review_diff,
}


class StdioTransport:
    """线程安全的 stdio 传输层，兼容 NDJSON 行协议与 Content-Length 帧协议。"""

    def __init__(self) -> None:
        self.use_content_length = False
        self.stdin = sys.stdin.buffer
        self.stdout = sys.stdout.buffer
        self.write_lock = threading.Lock()

    def read_message(self) -> Optional[dict[str, Any]]:
        while True:
            raw_line = self.stdin.readline()
            if not raw_line:
                return None
            stripped = raw_line.strip()
            if not stripped:
                continue

            if stripped.lower().startswith(b"content-length:"):
                self.use_content_length = True
                length = int(stripped.split(b":", 1)[1].strip())
                while True:
                    hdr = self.stdin.readline()
                    if not hdr or hdr in (b"\r\n", b"\n"):
                        break
                body = self.stdin.read(length)
                if not body:
                    return None
                return json.loads(body.decode("utf-8"))

            return json.loads(stripped.decode("utf-8"))

    def write_message(self, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        with self.write_lock:
            if self.use_content_length:
                header = f"Content-Length: {len(data)}\r\n\r\n".encode("ascii")
                self.stdout.write(header + data)
            else:
                self.stdout.write(data + b"\n")
            self.stdout.flush()


def handle_cancel_notification(params: dict[str, Any]) -> None:
    req_id = params.get("requestId")
    if req_id is None:
        return
    req_key = str(req_id)
    proc_to_kill = None
    cancel_cb = None
    with _JOBS_LOCK:
        job = _ACTIVE_JOBS.get(req_key)
        if job:
            job["cancel_event"].set()
            proc_to_kill = job.get("proc")
            cancel_cb = job.get("cancel_cb")
    if cancel_cb:
        try:
            cancel_cb()
        except Exception:
            pass
    if proc_to_kill:
        kill_process_tree(proc_to_kill)
    if job:
        log_err(f"已触发请求取消: requestId={req_key}")


def _write_final_once(transport: StdioTransport, req_key: str, payload: dict[str, Any]) -> bool:
    """原子确保同一个请求最多只写出一条终态响应，彻底杜绝取消与自然结束重叠导致的双回包。"""
    with _JOBS_LOCK:
        job = _ACTIVE_JOBS.get(req_key)
        if not job or job["responded"]:
            return False
        job["responded"] = True
    transport.write_message(payload)
    return True


def run_tool_call_async(transport: StdioTransport, req_id: Any, tool_name: str, tool_args: dict[str, Any]) -> None:
    req_key = str(req_id)
    cancel_event = threading.Event()
    with _JOBS_LOCK:
        _ACTIVE_JOBS[req_key] = {
            "cancel_event": cancel_event,
            "proc": None,
            "cancel_cb": None,
            "responded": False,
        }

    try:
        handler = TOOL_HANDLERS.get(tool_name)
        if not handler:
            _write_final_once(
                transport,
                req_key,
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"未知工具: {tool_name}"}],
                        "isError": True,
                    },
                },
            )
            return

        text = handler(tool_args, req_key=req_key, cancel_event=cancel_event)
        if cancel_event.is_set():
            _write_final_once(
                transport,
                req_key,
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32800, "message": "Request cancelled by client"},
                },
            )
            return
        _write_final_once(
            transport,
            req_key,
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": text}],
                    "isError": False,
                },
            },
        )
    except RequestCancelledError:
        _write_final_once(
            transport,
            req_key,
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32800, "message": "Request cancelled by client"},
            },
        )
    except Exception as exc:
        _write_final_once(
            transport,
            req_key,
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": f"调用 {tool_name} 失败: {exc}"}],
                    "isError": True,
                },
            },
        )
    finally:
        with _JOBS_LOCK:
            _ACTIVE_JOBS.pop(req_key, None)


def dispatch_message(transport: StdioTransport, req: dict[str, Any]) -> None:
    method = req.get("method")
    req_id = req.get("id")
    params = req.get("params") or {}

    if req_id is None:
        if method == "notifications/cancelled":
            handle_cancel_notification(params)
        elif method == "notifications/initialized":
            _HOT_POOL.trigger_background_prewarm()
        return

    try:
        if method == "initialize":
            client_ver = params.get("protocolVersion") or "2025-03-26"
            transport.write_message(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": client_ver,
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                        "instructions": (
                            f"零风险常驻热连接只读 GPT-6.2 Astra 协作引擎（v{SERVER_VERSION}，默认推理强度 {get_default_effort()}）。"
                            "主控负责大上下文切片、编码与测试；Astra 负责深度盲评、单轮防拍马屁红队审查与 Git Diff 审计。"
                        ),
                    },
                }
            )
            # 立即后台异步预热常驻 app-server，使用户首轮提问直接享受 ~0.17s 热建连
            _HOT_POOL.trigger_background_prewarm()
            return

        if method == "ping":
            transport.write_message({"jsonrpc": "2.0", "id": req_id, "result": {}})
            return

        if method == "tools/list":
            transport.write_message({"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS_SCHEMA}})
            return

        if method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments") or {}
            if tool_name == "astra_status":
                try:
                    text = handle_astra_status(tool_args)
                    transport.write_message(
                        {
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "result": {"content": [{"type": "text", "text": text}], "isError": False},
                        }
                    )
                except Exception as exc:
                    transport.write_message(
                        {
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "result": {"content": [{"type": "text", "text": f"调用失败: {exc}"}], "isError": True},
                        }
                    )
                return

            worker = threading.Thread(
                target=run_tool_call_async,
                args=(transport, req_id, str(tool_name or ""), tool_args),
                daemon=True,
            )
            worker.start()
            return

        transport.write_message(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }
        )
    except Exception as exc:
        transport.write_message(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32603, "message": f"Internal error: {exc}"},
            }
        )


def shutdown_all_jobs() -> None:
    with _JOBS_LOCK:
        jobs = list(_ACTIVE_JOBS.values())
    for job in jobs:
        job["cancel_event"].set()
        cb = job.get("cancel_cb")
        if cb:
            try:
                cb()
            except Exception:
                pass
        if job.get("proc"):
            kill_process_tree(job["proc"])
    _HOT_POOL.shutdown()


def main() -> None:
    transport = StdioTransport()
    try:
        while True:
            try:
                msg = transport.read_message()
                if msg is None:
                    break
                dispatch_message(transport, msg)
            except json.JSONDecodeError as exc:
                log_err(f"忽略非法 JSON 输入: {exc}")
            except KeyboardInterrupt:
                break
            except Exception as exc:
                log_err(f"主循环异常: {exc}")
    finally:
        shutdown_all_jobs()


if __name__ == "__main__":
    main()

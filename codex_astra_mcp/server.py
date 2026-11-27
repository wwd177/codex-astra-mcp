#!/usr/bin/env python3
"""
零风险只读 Codex Astra MCP Server v1.2.2 (常驻热连接 app-server + 精简隔离 exec 双擎架构)

核心工程特性与安全边界：
1. 100% 强制 read-only 沙箱（守护进程级 + 会话级 + 审批拦截三重锁死），绝不允许写入工作区或执行修改命令。
2. 绝不修改 ~/.codex/config.toml；自动屏蔽全局外部 MCP 服务器、桌面插件、Computer Use、Memories 与 Shell 快照开销。
3. 常驻热连接加速：MCP 初始化时后台静默预热 `codex app-server --listen stdio://`，代次隔离防误杀，复用长连接与 Prompt Cache。
4. 双擎高可用回退：仅在推理未发出前的守护通道故障时无缝回退至 `--ignore-user-config` 纯净只读 `codex exec`，杜绝业务失败或中途断连的盲目重跑。
5. 并发多路复用与精准取消：主线程预登记任务、异步非阻塞取消与 `turn/interrupt` 秒停目标轮次，并由 `_write_final_once` 杜绝双回包。
6. 零第三方依赖，仅使用 Python 3.11+ 标准库（含 `tomllib`），完整兼容 NDJSON 与 Content-Length 两种 stdio 帧格式。
"""

import collections
import contextlib
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
from typing import Any, Callable, Iterator, Optional

SERVER_NAME = "codex-astra"
SERVER_VERSION = "1.2.2"
DEFAULT_MODEL = "gpt-6.2-astra"
DEFAULT_EFFORT = "max"
ALLOWED_EFFORTS = {"low", "medium", "high", "xhigh", "max", "ultra"}
DEFAULT_TIMEOUT_SEC = 360
MIN_TIMEOUT_SEC = 15
MAX_TIMEOUT_SEC = 900

# 传输与子进程输出安全上限，防止恶意大包或异常子进程刷屏导致 OOM
MAX_CONTENT_LENGTH_BYTES = 32 * 1024 * 1024  # 32 MB
MAX_EXEC_STDOUT_BYTES = 8 * 1024 * 1024      # 8 MB
MAX_EXEC_STDERR_BYTES = 512 * 1024           # 512 KB
MAX_OWNED_THREADS = 256

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
_VERSION_CACHE: dict[str, Any] = {"sig": None, "version": "unknown"}
_MCP_NAMES_CACHE: dict[str, Any] = {"sig": None, "names": []}
_CACHE_LOCK = threading.Lock()

# 专用轻量空隔离目录（未指定 cwd 时替代庞大的用户主目录，杜绝无谓目录树扫描）
_EMPTY_SANDBOX_DIR: Optional[str] = None
_EMPTY_DIR_LOCK = threading.Lock()

# 活跃长任务注册表（req_key -> {"cancel_event": Event, "proc": Popen | None, "cancel_cb": Callable | None, "responded": bool, "count_as_active": bool}）
_JOBS_LOCK = threading.Lock()
_ACTIVE_JOBS: dict[str, dict[str, Any]] = {}

# 本进程签发的 thread_id 归属表与引用计数线程锁（有界容量，污染标记与归属同生命周期）
_THREADS_LOCK = threading.Lock()
_OWNED_THREADS: "collections.OrderedDict[str, dict[str, Any]]" = collections.OrderedDict()
_THREAD_EXEC_LOCKS: dict[str, dict[str, Any]] = {}

# 全局退出/断管通知事件
_TRANSPORT_BROKEN_EVENT = threading.Event()


class RequestCancelledError(Exception):
    """客户端发出 notifications/cancelled 主动取消请求。"""


class HotPoolPreDispatchError(RuntimeError):
    """常驻热连接在推理请求实际发出前失败（可安全无缝降级至精简 codex exec）。"""


class HotPoolMidTurnError(RuntimeError):
    """常驻热连接在 turn/start 发出后中途断开（执行状态未知，不盲目重跑）。"""


def log_err(msg: str) -> None:
    try:
        sys.stderr.write(f"[codex-astra-mcp] {msg}\n")
        sys.stderr.flush()
    except Exception:
        pass


def make_req_key(req_id: Any) -> str:
    """区分 JSON-RPC ID 类型，避免 int(1) 与 str('1') 碰撞。"""
    return f"{type(req_id).__name__}:{req_id}"


def parse_timeout_sec(raw: Any, default: int = DEFAULT_TIMEOUT_SEC) -> int:
    """严格解析并约束 timeout_sec，拒绝 bool、float 与非法非整数值。"""
    if raw is None or raw == "":
        return default
    if isinstance(raw, (bool, float)):
        raise ValueError(f"参数 timeout_sec 必须为正整数（收到 {type(raw).__name__}: {raw!r}）。")
    if isinstance(raw, int):
        val = raw
    elif isinstance(raw, str) and re.fullmatch(r"[+-]?\d+", raw.strip()):
        val = int(raw.strip())
    else:
        raise ValueError(f"参数 timeout_sec 无效（{raw!r}），必须为正整数。")
    if val <= 0:
        raise ValueError(f"参数 timeout_sec 必须大于 0（当前: {val}）。")
    return max(MIN_TIMEOUT_SEC, min(MAX_TIMEOUT_SEC, val))


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
    """动态定位本机官方 codex.exe（优先环境变量、WindowsApps、PATH 与最新 Appx 包）。"""
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CODEX_CACHE["path"]
        if not force_refresh and cached and (now - _CODEX_CACHE["checked_at"] < 120):
            if os.path.isfile(cached):
                return cached

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

    with _CACHE_LOCK:
        _CODEX_CACHE["path"] = found
        _CODEX_CACHE["checked_at"] = now
    return found


def get_codex_version_cached(codex_exe: Optional[str]) -> str:
    """按二进制路径与文件指纹缓存 `codex --version`，避免每次状态检查阻塞数秒。"""
    if not codex_exe:
        return "unknown"
    try:
        st = os.stat(codex_exe)
        sig = (codex_exe, st.st_mtime_ns, st.st_size)
    except OSError:
        sig = (codex_exe, 0, 0)

    with _CACHE_LOCK:
        if _VERSION_CACHE["sig"] == sig and _VERSION_CACHE["version"]:
            return _VERSION_CACHE["version"]

    try:
        proc = subprocess.run(
            [codex_exe, "--version"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=8,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        version = (proc.stdout or proc.stderr or "").strip() or "unknown"
    except Exception as exc:
        version = f"error: {exc}"

    with _CACHE_LOCK:
        _VERSION_CACHE["sig"] = sig
        _VERSION_CACHE["version"] = version
    return version


def discover_global_mcp_names() -> list[str]:
    """只读提取 ~/.codex/config.toml 中声明的 mcp_servers 键名（按文件 mtime_ns 缓存）。"""
    codex_home = os.environ.get("CODEX_HOME", "").strip()
    cfg_path = (Path(codex_home) if codex_home else (Path.home() / ".codex")) / "config.toml"
    if not cfg_path.is_file():
        return []
    try:
        st = cfg_path.stat()
        sig = (str(cfg_path), st.st_mtime_ns, st.st_size)
    except OSError:
        sig = None

    if sig is not None:
        with _CACHE_LOCK:
            if _MCP_NAMES_CACHE["sig"] == sig:
                return list(_MCP_NAMES_CACHE["names"])

    names: list[str] = []
    try:
        raw = cfg_path.read_bytes()
        data = tomllib.loads(raw.decode("utf-8", errors="replace"))
        mcp_tbl = data.get("mcp_servers")
        if isinstance(mcp_tbl, dict):
            names = [str(k) for k in mcp_tbl.keys() if str(k).strip()]
    except Exception:
        try:
            txt = cfg_path.read_text(encoding="utf-8", errors="ignore")
            names = list(dict.fromkeys(re.findall(r'^\[mcp_servers\.([A-Za-z0-9_-]+)\]', txt, flags=re.M)))
        except Exception:
            names = []

    if sig is not None:
        with _CACHE_LOCK:
            _MCP_NAMES_CACHE["sig"] = sig
            _MCP_NAMES_CACHE["names"] = list(names)
    return names


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
    if not proc:
        return
    if proc.poll() is None:
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
                try:
                    proc.wait(timeout=1.5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    try:
        proc.wait(timeout=2)
    except Exception:
        pass


def resolve_cwd(cwd: Optional[str], strict: bool = True) -> tuple[str, bool]:
    """返回 (目标目录绝对路径, 是否由调用方显式挂载了真实项目目录)。"""
    if cwd is not None and str(cwd).strip():
        raw_str = str(cwd).strip()
        p = Path(raw_str).expanduser()
        if p.is_dir():
            return str(p.resolve()), True
        if strict:
            raise ValueError(f"指定的 cwd 目录不存在或不可访问: {raw_str}")
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


@contextlib.contextmanager
def acquire_thread_lock(
    thread_id: str,
    cancel_event: Optional[threading.Event] = None,
    deadline: Optional[float] = None,
) -> Iterator[None]:
    """引用计数保护的线程级互斥锁，支持取消与超时感知，杜绝回收破坏互斥或内存泄漏。"""
    with _THREADS_LOCK:
        entry = _THREAD_EXEC_LOCKS.get(thread_id)
        if entry is None:
            entry = {"lock": threading.Lock(), "refs": 0}
            _THREAD_EXEC_LOCKS[thread_id] = entry
        entry["refs"] += 1
    lk: threading.Lock = entry["lock"]

    acquired = False
    try:
        while True:
            if cancel_event and cancel_event.is_set():
                raise RequestCancelledError("等待同一 thread_id 互斥锁期间请求已被取消。")
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"等待同一 thread_id（{thread_id}）并发执行锁超时。")
                wait_slice = min(0.1, remaining)
            else:
                wait_slice = 0.1
            if lk.acquire(timeout=wait_slice):
                acquired = True
                break
        yield
    finally:
        if acquired:
            lk.release()
        with _THREADS_LOCK:
            entry["refs"] -= 1
            if entry["refs"] <= 0 and _THREAD_EXEC_LOCKS.get(thread_id) is entry:
                _THREAD_EXEC_LOCKS.pop(thread_id, None)


def record_owned_thread(thread_id: str, cwd: str, engine: str, daemon_gen: Optional[int] = None) -> None:
    """写入本会话签发的 thread_id 归属记录并做 LRU 有界淘汰（清除已确认完成的污染状态）。"""
    with _THREADS_LOCK:
        _OWNED_THREADS[thread_id] = {
            "cwd": cwd,
            "engine": engine,
            "daemon_gen": daemon_gen,
            "tainted_reason": None,
        }
        _OWNED_THREADS.move_to_end(thread_id)
        while len(_OWNED_THREADS) > MAX_OWNED_THREADS:
            # 仅淘汰最久未用且当前未被锁持有的旧记录；污染标记随归属记录一并回收
            evicted = False
            for old_tid in list(_OWNED_THREADS.keys()):
                if old_tid == thread_id:
                    continue
                if old_tid not in _THREAD_EXEC_LOCKS:
                    _OWNED_THREADS.pop(old_tid, None)
                    evicted = True
                    break
            if not evicted:
                break


def mark_thread_tainted(thread_id: str, reason: str) -> None:
    """将污染标记直接写入归属记录，确保不会出现保留续接资格却丢失污染标记的不一致。"""
    with _THREADS_LOCK:
        meta = _OWNED_THREADS.get(thread_id)
        if meta is not None:
            meta["tainted_reason"] = reason


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


def clear_cancel_callback(req_key: Optional[str], cb: Any) -> None:
    if req_key is None:
        return
    with _JOBS_LOCK:
        job = _ACTIVE_JOBS.get(req_key)
        if job is not None and job.get("cancel_cb") is cb:
            job["cancel_cb"] = None


class CodexAppServerPool:
    """
    常驻纯净只读 `codex app-server --listen stdio://` 守护连接（支持并发多会话、预热与秒级打断）。

    锁顺序铁律（严格单向，零死锁）：
    - `_lifecycle_lock` -> `_router_lock` / `_write_lock`（允许）
    - `_router_lock` 与 `_write_lock` 互相独立、绝不嵌套，也绝不反向获取 `_lifecycle_lock`
    - `_reader_loop.finally` 先在 `_router_lock` 下按代次唤醒等待者，释放后再独立获取 `_lifecycle_lock` 清理句柄
    """

    def __init__(self) -> None:
        self._lifecycle_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._router_lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._gen: int = 0
        self._next_id: int = 10
        # req_id -> (gen, queue)
        self._pending_rpc: dict[int, tuple[int, queue.Queue]] = {}
        # req_id -> callback(msg)（用于取消飞行中 turn/start 后自动补发 turn/interrupt）
        self._late_rpc_hooks: dict[int, tuple[int, Callable[[dict[str, Any]], None]]] = {}
        # thread_id -> (gen, queue)
        self._thread_queues: dict[str, tuple[int, queue.Queue]] = {}
        self._prewarmed = False
        self._prewarming = False
        self._booted_at: Optional[float] = None

    def status_snapshot(self) -> dict[str, Any]:
        with self._lifecycle_lock:
            proc = self._proc
            alive = bool(proc and proc.poll() is None)
            prewarmed = self._prewarmed if alive else False
            booted_at = self._booted_at if alive else None
            gen = self._gen if alive else 0
        return {
            "hot_daemon_alive": alive,
            "hot_daemon_pid": proc.pid if alive and proc else None,
            "hot_daemon_gen": gen,
            "prewarmed": prewarmed,
            "uptime_sec": round(time.monotonic() - booted_at, 1) if (alive and booted_at) else 0.0,
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
            gen = self._gen
            self._proc = None
            self._prewarmed = False
            self._booted_at = None
        if proc:
            kill_process_tree(proc)
        if gen:
            self._notify_gen_eof(gen, "app-server 已关闭")

    def _next_rpc_id(self) -> int:
        with self._router_lock:
            self._next_id += 1
            return self._next_id

    def _notify_gen_eof(self, gen: int, reason: str) -> None:
        """仅唤醒归属于指定代次 gen 的等待队列，绝不误杀已重启的新一代进程 RPC。"""
        with self._router_lock:
            rpcs: list[queue.Queue] = []
            for rid in [k for k, (g, _) in self._pending_rpc.items() if g == gen]:
                _, q = self._pending_rpc.pop(rid)
                rpcs.append(q)
            for rid in [k for k, (g, _) in self._late_rpc_hooks.items() if g == gen]:
                self._late_rpc_hooks.pop(rid, None)
            tqs = [q for g, q in self._thread_queues.values() if g == gen]

        eof_msg = {"_eof": True, "_reason": reason, "_gen": gen}
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

    def _send_raw(self, payload: dict[str, Any], target_proc: Optional[subprocess.Popen] = None) -> None:
        line = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        with self._write_lock:
            proc = target_proc or self._proc
            if not proc or proc.poll() is not None or proc.stdin is None:
                raise HotPoolPreDispatchError("常驻 codex app-server 未运行")
            try:
                proc.stdin.write(line)
                proc.stdin.flush()
            except (BrokenPipeError, OSError, ValueError) as exc:
                raise HotPoolPreDispatchError(f"写入常驻 app-server 管道失败: {exc}") from exc

    def _extract_thread_id(self, params: dict[str, Any]) -> Optional[str]:
        tid = params.get("threadId") or params.get("thread_id")
        if isinstance(tid, str) and tid:
            return tid
        turn_obj = params.get("turn")
        if isinstance(turn_obj, dict):
            tid2 = turn_obj.get("threadId") or turn_obj.get("thread_id")
            if isinstance(tid2, str) and tid2:
                return tid2
        return None

    def _reader_loop(self, proc: subprocess.Popen, gen: int) -> None:
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
                if not isinstance(msg, dict):
                    continue

                has_id = "id" in msg and msg.get("id") is not None
                method = msg.get("method")

                # 拦截任何来自 app-server 的反向审批/权限请求，异步回绝，100% 拒绝写操作且避免双向管道死锁
                if has_id and method:
                    req_id = msg["id"]
                    deny_payload = {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {
                            "code": -32000,
                            "message": "Strict read-only mode: all interactive/write approvals are denied.",
                        },
                    }

                    def _send_deny(p: dict[str, Any] = deny_payload, tp: subprocess.Popen = proc) -> None:
                        try:
                            self._send_raw(p, target_proc=tp)
                        except Exception:
                            pass

                    threading.Thread(target=_send_deny, daemon=True).start()
                    continue

                if has_id:
                    req_id = msg["id"]
                    hook: Optional[Callable[[dict[str, Any]], None]] = None
                    q: Optional[queue.Queue] = None
                    with self._router_lock:
                        item = self._pending_rpc.get(req_id)
                        if item is not None and item[0] == gen:
                            self._pending_rpc.pop(req_id, None)
                            q = item[1]
                        else:
                            h_item = self._late_rpc_hooks.get(req_id)
                            if h_item is not None and h_item[0] == gen:
                                self._late_rpc_hooks.pop(req_id, None)
                                hook = h_item[1]
                    if q is not None:
                        q.put(msg)
                    elif hook is not None:
                        threading.Thread(target=hook, args=(msg,), daemon=True).start()
                    continue

                if method:
                    params = msg.get("params")
                    if isinstance(params, dict):
                        tid = self._extract_thread_id(params)
                        if tid:
                            with self._router_lock:
                                t_item = self._thread_queues.get(tid)
                            if t_item is not None and t_item[0] == gen:
                                t_item[1].put(msg)
        except Exception as exc:
            log_err(f"app-server 读取循环异常 (gen={gen}): {exc}")
        finally:
            # 1) 必须先不持 _lifecycle_lock 唤醒本代等待者，防止与 ensure_started(initialize) 发生 AB-BA 死锁
            self._notify_gen_eof(gen, "codex app-server 进程已退出")
            # 2) 再独立获取 _lifecycle_lock 清理本代句柄（绝不覆盖新一代进程）
            with self._lifecycle_lock:
                if self._gen == gen and self._proc is proc:
                    self._proc = None
                    self._prewarmed = False
                    self._booted_at = None

    def _stderr_drain_loop(self, proc: subprocess.Popen) -> None:
        if proc.stderr is None:
            return
        try:
            for raw in proc.stderr:
                _ = raw
        except Exception:
            pass

    def _rpc_call(
        self,
        method: str,
        params: dict[str, Any],
        timeout: float = 20.0,
        gen: Optional[int] = None,
        target_proc: Optional[subprocess.Popen] = None,
        cancel_event: Optional[threading.Event] = None,
        on_late_reply_if_cancelled: Optional[Callable[[dict[str, Any]], None]] = None,
        mid_turn_on_eof: bool = False,
    ) -> dict[str, Any]:
        active_gen = gen if gen is not None else self._gen
        with self._router_lock:
            self._next_id += 1
            req_id = self._next_id
            q: queue.Queue = queue.Queue()
            self._pending_rpc[req_id] = (active_gen, q)

        def _handoff_late_reply() -> None:
            if on_late_reply_if_cancelled is None:
                with self._router_lock:
                    self._pending_rpc.pop(req_id, None)
                return
            with self._router_lock:
                existed = self._pending_rpc.pop(req_id, None)
                if existed is not None:
                    # reader 尚未取走该 req_id：直接交接给 late_rpc_hooks，确保到达时恰好触发一次
                    self._late_rpc_hooks[req_id] = (active_gen, on_late_reply_if_cancelled)
                    return
            # existed is None 表示 reader 刚刚已 pop 并正在/已经向 q 投递回复：后台读取一次触发补发
            def _consume_already_routed() -> None:
                try:
                    late_msg = q.get(timeout=2.0)
                except queue.Empty:
                    return
                if isinstance(late_msg, dict) and not late_msg.get("_eof"):
                    on_late_reply_if_cancelled(late_msg)

            threading.Thread(target=_consume_already_routed, daemon=True).start()

        deadline = time.monotonic() + max(0.5, timeout)
        try:
            self._send_raw(
                {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params},
                target_proc=target_proc,
            )
            while True:
                if cancel_event and cancel_event.is_set():
                    _handoff_late_reply()
                    raise RequestCancelledError(f"等待 app-server `{method}` 期间请求已取消。")

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    _handoff_late_reply()
                    raise TimeoutError(f"app-server RPC `{method}` 超时 ({round(timeout, 1)}s)")

                try:
                    resp = q.get(timeout=min(0.15, remaining))
                    break
                except queue.Empty:
                    continue
        except (RequestCancelledError, TimeoutError):
            raise
        except Exception:
            with self._router_lock:
                self._pending_rpc.pop(req_id, None)
            raise

        if resp.get("_eof"):
            reason = resp.get("_reason") or "app-server 连接已断开"
            if mid_turn_on_eof:
                raise HotPoolMidTurnError(f"热连接在 `{method}` 发出后断开（执行状态未知）: {reason}")
            raise HotPoolPreDispatchError(reason)
        if "error" in resp and resp["error"]:
            err_obj = resp["error"]
            msg = err_obj.get("message") if isinstance(err_obj, dict) else str(err_obj)
            raise RuntimeError(f"app-server `{method}` 返回错误: {msg}")
        res = resp.get("result")
        return res if isinstance(res, dict) else {}

    def ensure_started(self, prewarm_thread: bool = False) -> tuple[subprocess.Popen, int]:
        need_prewarm = False
        with self._lifecycle_lock:
            if self._proc is None or self._proc.poll() is not None:
                codex_exe = find_codex_exe()
                if not codex_exe:
                    raise HotPoolPreDispatchError(
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

                try:
                    proc = subprocess.Popen(
                        cmd,
                        cwd=get_empty_readonly_dir(),
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                except OSError as exc:
                    raise HotPoolPreDispatchError(f"启动 codex app-server 失败: {exc}") from exc

                self._gen += 1
                gen = self._gen
                self._proc = proc
                self._prewarmed = False
                self._booted_at = time.monotonic()

                threading.Thread(target=self._reader_loop, args=(proc, gen), daemon=True).start()
                threading.Thread(target=self._stderr_drain_loop, args=(proc,), daemon=True).start()

                try:
                    self._rpc_call(
                        "initialize",
                        {
                            "clientInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                            "capabilities": {},
                        },
                        timeout=15.0,
                        gen=gen,
                        target_proc=proc,
                    )
                    self._send_raw({"jsonrpc": "2.0", "method": "initialized"}, target_proc=proc)
                except Exception as exc:
                    kill_process_tree(proc)
                    if self._gen == gen and self._proc is proc:
                        self._proc = None
                        self._booted_at = None
                    if isinstance(exc, HotPoolPreDispatchError):
                        raise
                    raise HotPoolPreDispatchError(f"初始化 codex app-server 失败: {exc}") from exc

            proc = self._proc
            gen = self._gen
            if prewarm_thread and not self._prewarmed:
                need_prewarm = True

        if need_prewarm:
            # 零 Token 消耗预热：提前触发一次模型发现与本地索引缓存，并按代次校验再置位 _prewarmed
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
                gen=gen,
                target_proc=proc,
            )
            with self._lifecycle_lock:
                if self._gen == gen and self._proc is proc:
                    self._prewarmed = True

        return proc, gen

    def _send_turn_interrupt_async(
        self,
        tid: str,
        turn_id: str,
        target_proc: subprocess.Popen,
    ) -> None:
        req_id = self._next_rpc_id()
        payload = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "turn/interrupt",
            "params": {"threadId": tid, "turnId": turn_id},
        }
        try:
            self._send_raw(payload, target_proc=target_proc)
        except Exception:
            pass

    def _drain_after_interrupt(
        self,
        tq: queue.Queue,
        tid: str,
        started_turn_id: Optional[str],
        ephemeral: bool,
        max_wait: float = 0.35,
    ) -> None:
        """打断后短暂等待 turn/completed，避免迟到事件串入下一轮；若未收到则标记线程不可复用。"""
        if ephemeral:
            return
        end_at = time.monotonic() + max_wait
        confirmed = False
        while True:
            rem = end_at - time.monotonic()
            if rem <= 0:
                break
            try:
                evt = tq.get(timeout=min(0.08, rem))
            except queue.Empty:
                continue
            if evt.get("_eof"):
                confirmed = True
                break
            if evt.get("method") == "turn/completed":
                params = evt.get("params") or {}
                t_final = params.get("turn") if isinstance(params.get("turn"), dict) else {}
                ev_tid = t_final.get("id") or params.get("turnId")
                if not started_turn_id or not ev_tid or ev_tid == started_turn_id:
                    confirmed = True
                    break
        if not confirmed:
            mark_thread_tainted(
                tid,
                "上一轮任务在被打断后尚未完全排空终态事件，为防迟到结果串轮，请去掉 thread_id 开启新会话。",
            )

    def run_turn(
        self,
        prompt: str,
        cwd: str,
        model: str,
        effort: str,
        ephemeral: bool = True,
        existing_thread_id: Optional[str] = None,
        existing_daemon_gen: Optional[int] = None,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
        req_key: Optional[str] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> dict[str, Any]:
        if cancel_event and cancel_event.is_set():
            raise RequestCancelledError("请求在启动前已取消")

        started = time.monotonic()
        deadline = started + parse_timeout_sec(timeout_sec)

        proc, gen = self.ensure_started(prewarm_thread=False)

        tid = existing_thread_id
        if not tid:
            try:
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
                    gen=gen,
                    target_proc=proc,
                    cancel_event=cancel_event,
                )
            except (RequestCancelledError, TimeoutError, HotPoolPreDispatchError):
                raise
            except Exception as exc:
                raise HotPoolPreDispatchError(f"创建热会话 thread/start 失败: {exc}") from exc

            th_obj = th_res.get("thread") if isinstance(th_res.get("thread"), dict) else {}
            tid = str(th_obj.get("id") or "").strip()
            if not tid:
                raise HotPoolPreDispatchError("app-server `thread/start` 未返回有效 thread.id")
            with self._lifecycle_lock:
                if self._gen == gen and self._proc is proc:
                    self._prewarmed = True

        with acquire_thread_lock(tid, cancel_event=cancel_event, deadline=deadline):
            # 若续接会话且守护进程已发生跨代重启，先在线程锁内执行 thread/resume
            if existing_thread_id and existing_daemon_gen != gen:
                try:
                    self._rpc_call(
                        "thread/resume",
                        {
                            "threadId": tid,
                            "cwd": cwd,
                            "model": model,
                            "sandbox": "read-only",
                            "approvalPolicy": "never",
                        },
                        timeout=max(5.0, min(25.0, deadline - time.monotonic())),
                        gen=gen,
                        target_proc=proc,
                        cancel_event=cancel_event,
                    )
                except (RequestCancelledError, TimeoutError):
                    raise
                except Exception as exc:
                    raise RuntimeError(
                        f"常驻 app-server 重启后恢复会话 thread_id={tid} 失败（{exc}）；请去掉 thread_id 开启新会话。"
                    ) from exc

            tq: queue.Queue = queue.Queue()
            with self._router_lock:
                self._thread_queues[tid] = (gen, tq)

            active_turn_id: dict[str, Optional[str]] = {"id": None}
            interrupt_sent = threading.Event()

            def _interrupt_turn() -> None:
                cur_turn = active_turn_id.get("id")
                if cur_turn and not interrupt_sent.is_set():
                    interrupt_sent.set()
                    self._send_turn_interrupt_async(tid, cur_turn, proc)

            def _late_turn_start_hook(reply_msg: dict[str, Any]) -> None:
                """若 turn/start 在取消或超时瞬间才返回 turn.id，立即补发 turn/interrupt 杜绝孤儿推理。"""
                res_obj = reply_msg.get("result") if isinstance(reply_msg.get("result"), dict) else {}
                t_obj = res_obj.get("turn") if isinstance(res_obj.get("turn"), dict) else {}
                late_id = str(t_obj.get("id") or "").strip()
                if late_id:
                    self._send_turn_interrupt_async(tid, late_id, proc)

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
                    gen=gen,
                    target_proc=proc,
                    cancel_event=cancel_event,
                    on_late_reply_if_cancelled=_late_turn_start_hook,
                    mid_turn_on_eof=True,
                )
                turn_obj = turn_res.get("turn") if isinstance(turn_res.get("turn"), dict) else {}
                started_turn_id = str(turn_obj.get("id") or "").strip() or None
                active_turn_id["id"] = started_turn_id

                if cancel_event and cancel_event.is_set():
                    _interrupt_turn()
                    self._drain_after_interrupt(tq, tid, started_turn_id, ephemeral)
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
                        self._drain_after_interrupt(tq, tid, started_turn_id, ephemeral)
                        raise RequestCancelledError("客户端已取消当前 GPT-6.2 Astra 任务，已即时打断目标轮次。")

                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        _interrupt_turn()
                        self._drain_after_interrupt(tq, tid, started_turn_id, ephemeral)
                        raise TimeoutError(
                            f"GPT-6.2 Astra 执行超过 {timeout_sec} 秒已安全打断，建议精炼代码切片或调低 effort 后重试。"
                        )

                    try:
                        evt = tq.get(timeout=min(0.25, remaining))
                    except queue.Empty:
                        continue

                    if evt.get("_eof"):
                        raise HotPoolMidTurnError(
                            f"常驻 app-server 在推理中途断开（执行状态未知）: {evt.get('_reason') or 'EOF'}"
                        )

                    method = evt.get("method", "")
                    params = evt.get("params") or {}
                    evt_turn_id = params.get("turnId") or params.get("turn_id")
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
                        final_tid = t_final.get("id") or evt_turn_id
                        if started_turn_id and final_tid and final_tid != started_turn_id:
                            continue
                        turn_status = str(t_final.get("status") or "completed")
                        t_err = t_final.get("error")
                        if isinstance(t_err, dict) and t_err.get("message"):
                            msg = str(t_err["message"])
                            if not is_non_fatal_codex_warning(msg):
                                fatal_errors.append(msg)
                        # 若流式事件未捕获正文，从终态 turn.items 兜底提取（与流式一致区分 commentary）
                        if not completed_messages and isinstance(t_final.get("items"), list):
                            for it in t_final["items"]:
                                if isinstance(it, dict) and it.get("type") == "agentMessage":
                                    txt = str(it.get("text") or "").strip()
                                    if txt:
                                        if it.get("phase") == "commentary":
                                            commentary_messages.append(txt)
                                        else:
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
                    "daemon_gen": gen,
                }
            finally:
                clear_cancel_callback(req_key, _interrupt_turn)
                with self._router_lock:
                    cur = self._thread_queues.get(tid)
                    if cur is not None and cur[1] is tq:
                        self._thread_queues.pop(tid, None)


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
        if not isinstance(evt, dict):
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


def _read_stream_bounded(stream: Any, chunks: list[bytes], max_bytes: int, state: dict[str, bool]) -> None:
    """持续排空子进程管道以防死锁，同时将内存保留量限制在 max_bytes 以内防 OOM。"""
    kept = 0
    try:
        while True:
            data = stream.read(8192)
            if not data:
                break
            if kept < max_bytes:
                remain = max_bytes - kept
                if len(data) <= remain:
                    chunks.append(data)
                    kept += len(data)
                else:
                    chunks.append(data[:remain])
                    kept = max_bytes
                    state["truncated"] = True
            else:
                state["truncated"] = True
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
    """纯净隔离模式 `codex exec`（作为原生 review 与热连接预发失败时的兜底引擎）。"""
    codex_exe = find_codex_exe()
    if not codex_exe:
        raise RuntimeError(
            "未检测到本机官方 OpenAI Codex CLI (codex.exe)。请先安装 Codex Desktop 或设置 CODEX_BIN 环境变量。"
        )

    if cancel_event and cancel_event.is_set():
        raise RequestCancelledError("请求在启动前已取消")

    started = time.monotonic()
    effective_timeout = parse_timeout_sec(timeout_sec)
    deadline = started + effective_timeout

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
    out_state = {"truncated": False}
    err_state = {"truncated": False}
    t_out = threading.Thread(
        target=_read_stream_bounded,
        args=(proc.stdout, out_chunks, MAX_EXEC_STDOUT_BYTES, out_state),
        daemon=True,
    )
    t_err = threading.Thread(
        target=_read_stream_bounded,
        args=(proc.stderr, err_chunks, MAX_EXEC_STDERR_BYTES, err_state),
        daemon=True,
    )
    t_out.start()
    t_err.start()

    try:
        if stdin_prompt is not None and proc.stdin is not None:
            try:
                proc.stdin.write(stdin_prompt.encode("utf-8"))
                proc.stdin.close()
            except (BrokenPipeError, OSError, ValueError):
                pass

        while proc.poll() is None:
            if cancel_event and cancel_event.is_set():
                kill_process_tree(proc)
                raise RequestCancelledError("客户端已取消当前 GPT-6.2 Astra 任务，子进程树已安全终止。")
            if time.monotonic() >= deadline:
                kill_process_tree(proc)
                raise TimeoutError(
                    f"GPT-6.2 Astra 执行超过 {effective_timeout} 秒已安全终止，建议先由 Neon 精炼上下文切片或调低 effort 后重试。"
                )
            time.sleep(0.1)
    finally:
        if proc.poll() is None:
            kill_process_tree(proc)
        t_out.join(timeout=3)
        t_err.join(timeout=3)
        for s in (proc.stdin, proc.stdout, proc.stderr):
            try:
                if s is not None:
                    s.close()
            except Exception:
                pass
        if req_key is not None:
            with _JOBS_LOCK:
                job = _ACTIVE_JOBS.get(req_key)
                if job is not None and job.get("proc") is proc:
                    job["proc"] = None

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
        if out_state["truncated"]:
            raise RuntimeError("Codex 输出超过 8MB 上限被截断，未能提取最终回复正文。")
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
    existing_daemon_gen: Optional[int] = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    req_key: Optional[str] = None,
    cancel_event: Optional[threading.Event] = None,
) -> dict[str, Any]:
    """
    优先调度常驻热连接 app-server：
    - 仅在请求发出前的守护通道故障（HotPoolPreDispatchError）且非续接会话时，自动无缝降级至精简 `codex exec`；
    - 模型业务终态错误、鉴权/限流失败、取消/超时及中途断连均直接透传，绝不盲目重跑。
    """
    if existing_engine != "exec":
        try:
            return _HOT_POOL.run_turn(
                prompt=prompt,
                cwd=cwd,
                model=model,
                effort=effort,
                ephemeral=ephemeral,
                existing_thread_id=existing_thread_id,
                existing_daemon_gen=existing_daemon_gen,
                timeout_sec=timeout_sec,
                req_key=req_key,
                cancel_event=cancel_event,
            )
        except HotPoolPreDispatchError as hot_exc:
            if existing_thread_id:
                raise RuntimeError(f"续接热会话失败（常驻守护未就绪）: {hot_exc}") from hot_exc
            log_err(f"常驻热通道未就绪，自动无缝切换精简 exec 引擎: {hot_exc}")
        except (RequestCancelledError, TimeoutError, HotPoolMidTurnError):
            raise

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


def _run_git_cmd(cwd: str, args: list[str], timeout: float = 10.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _validate_git_ref_arg(value: str, param_name: str) -> str:
    ref = value.strip()
    if ref.startswith("-"):
        raise ValueError(f"参数 {param_name} 不合法（不能以 '-' 开头）: {ref}")
    if any(ch in ref for ch in ("\x00", "\r", "\n")):
        raise ValueError(f"参数 {param_name} 包含非法控制字符。")
    return ref


def _collect_untracked_text_snippets(cwd: str, max_total_chars: int = 12000, max_per_file: int = 4000) -> str:
    """采集新建未跟踪小体积文本文件内容，避免新文件在未提交审查中被遗漏。"""
    ls = _run_git_cmd(cwd, ["ls-files", "--others", "--exclude-standard"], timeout=6)
    if ls.returncode != 0 or not ls.stdout.strip():
        return ""

    base_path = Path(cwd).resolve()
    parts: list[str] = []
    used = 0
    for rel in ls.stdout.splitlines():
        rel = rel.strip()
        if not rel:
            continue
        cand = base_path / rel
        if cand.is_symlink():
            parts.append(f"#### {rel} (符号链接，为防越界已跳过正文)")
            continue
        full = cand.resolve()
        try:
            full.relative_to(base_path)
        except ValueError:
            continue
        if not full.is_file():
            continue
        try:
            raw = full.read_bytes()[: max_per_file + 512]
        except OSError:
            continue
        # 跳过包含 NUL 的二进制文件
        if b"\x00" in raw:
            parts.append(f"#### {rel} (二进制新文件，已略过正文)")
            continue
        text = raw.decode("utf-8", errors="replace")
        if len(text) > max_per_file:
            text = text[:max_per_file] + "\n... [单文件预览已截断]"
        block = f"#### 新建未跟踪文件: {rel}\n```\n{text}\n```"
        if used + len(block) > max_total_chars:
            parts.append("... [其余未跟踪文件因总长度上限已省略]")
            break
        parts.append(block)
        used += len(block)
    return "\n\n".join(parts)


def collect_git_diff_summary(
    cwd: str,
    base_branch: str = "",
    commit_sha: str = "",
    max_chars: int = 24000,
) -> tuple[bool, str, bool]:
    """
    精准收集目标 Git 差异，支持三种模式：
    - commit_sha: 指定 commit 的变更 (`git show`)
    - base_branch: 分支对比 (`git diff <base>...HEAD`)
    - 默认: 工作区未提交改动（兼容无 HEAD 初始仓库的暂存区 + 未暂存改动 + 新建小文本文件）
    返回 (is_git, summary_text, was_truncated)。
    """
    try:
        check = _run_git_cmd(cwd, ["rev-parse", "--is-inside-work-tree"], timeout=5)
        if check.returncode != 0 or check.stdout.strip().lower() != "true":
            return False, "", False

        truncated = False
        parts: list[str] = []

        if commit_sha:
            ref = _validate_git_ref_arg(commit_sha, "commit")
            verify = _run_git_cmd(cwd, ["rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], timeout=6)
            resolved_sha = (verify.stdout or "").strip()
            if verify.returncode != 0 or not re.fullmatch(r"[0-9a-fA-F]{7,64}", resolved_sha):
                raise ValueError(f"参数 commit `{ref}` 必须解析为单个有效提交（拒绝空范围或非法引用）。")
            show = _run_git_cmd(cwd, ["show", "--stat", "--patch", "--no-color", resolved_sha, "--"], timeout=12)
            if show.returncode != 0:
                raise ValueError(f"无法读取指定 commit `{resolved_sha}`: {(show.stderr or '').strip()[:300]}")
            diff = show.stdout.strip()
            if len(diff) > max_chars:
                truncated = True
                diff = diff[:max_chars] + f"\n... [commit diff 已截断，原长 {len(diff)} 字符]"
            if diff:
                parts.append(f"### Git Commit `{resolved_sha}`\n```diff\n{diff}\n```")
            return True, "\n\n".join(parts), truncated

        if base_branch:
            ref = _validate_git_ref_arg(base_branch, "base")
            verify = _run_git_cmd(cwd, ["rev-parse", "--verify", f"{ref}^{{commit}}"], timeout=6)
            if verify.returncode != 0:
                raise ValueError(f"基准引用 `{ref}` 不存在或无效: {(verify.stderr or '').strip()[:300]}")
            diff_proc = _run_git_cmd(cwd, ["diff", "--no-color", f"{ref}...HEAD", "--"], timeout=12)
            if diff_proc.returncode != 0:
                diff_proc = _run_git_cmd(cwd, ["diff", "--no-color", ref, "HEAD", "--"], timeout=12)
            diff = diff_proc.stdout.strip()
            stat = _run_git_cmd(cwd, ["diff", "--stat", "--no-color", f"{ref}...HEAD", "--"], timeout=8).stdout.strip()
            if stat:
                parts.append(f"### Branch Diff Stat (`{ref}...HEAD`)\n```\n{stat}\n```")
            if len(diff) > max_chars:
                truncated = True
                diff = diff[:max_chars] + f"\n... [branch diff 已截断，原长 {len(diff)} 字符]"
            if diff:
                parts.append(f"### Branch Diff (`{ref}...HEAD`)\n```diff\n{diff}\n```")
            return True, "\n\n".join(parts), truncated

        # 默认：未提交变更（兼容空 HEAD 初始仓）
        status = _run_git_cmd(cwd, ["status", "--short"], timeout=6).stdout.strip()
        has_head = _run_git_cmd(cwd, ["rev-parse", "--verify", "HEAD"], timeout=5).returncode == 0
        if has_head:
            diff = _run_git_cmd(cwd, ["diff", "--no-color", "HEAD", "--"], timeout=10).stdout.strip()
        else:
            staged = _run_git_cmd(cwd, ["diff", "--no-color", "--cached", "--"], timeout=10).stdout.strip()
            unstaged = _run_git_cmd(cwd, ["diff", "--no-color", "--"], timeout=10).stdout.strip()
            diff_blocks = []
            if staged:
                diff_blocks.append("# Staged changes (initial repository)\n" + staged)
            if unstaged:
                diff_blocks.append("# Unstaged changes\n" + unstaged)
            diff = "\n\n".join(diff_blocks)

        if len(diff) > max_chars:
            truncated = True
            diff = diff[:max_chars] + f"\n... [diff 已截断，原长 {len(diff)} 字符]"

        untracked_section = _collect_untracked_text_snippets(cwd)

        if status:
            parts.append(f"### Git Status\n```\n{status}\n```")
        if diff:
            parts.append(f"### Git Diff\n```diff\n{diff}\n```")
        if untracked_section:
            parts.append(f"### Untracked New Files\n{untracked_section}")
        return True, "\n\n".join(parts), truncated
    except ValueError:
        raise
    except Exception:
        return False, "", False


def handle_astra_status(_args: dict[str, Any], **_ctx: Any) -> str:
    codex_exe = find_codex_exe(force_refresh=False)
    version = get_codex_version_cached(codex_exe)

    with _JOBS_LOCK:
        active_count = sum(1 for j in _ACTIVE_JOBS.values() if j.get("count_as_active", True))

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
    cwd, has_explicit_cwd = resolve_cwd(raw_cwd, strict=True)
    model = normalize_model(args.get("model"))
    effort = normalize_effort(args.get("effort"))
    thread_id = str(args.get("thread_id") or "").strip() or None
    timeout_sec = parse_timeout_sec(args.get("timeout_sec"))

    existing_engine: Optional[str] = None
    existing_daemon_gen: Optional[int] = None
    if thread_id:
        with _THREADS_LOCK:
            meta = _OWNED_THREADS.get(thread_id)
            tainted_reason = meta.get("tainted_reason") if meta is not None else None
        if meta is None:
            raise ValueError(
                f"拒绝续接未在本 MCP 会话内签发的 thread_id（{thread_id}），以防范跨会话上下文串用；请发起新会话。"
            )
        if tainted_reason:
            raise ValueError(f"拒绝续接已中断未排空的 thread_id（{thread_id}）：{tainted_reason}")
        owner_cwd = meta["cwd"]
        existing_engine = meta["engine"]
        existing_daemon_gen = meta.get("daemon_gen")
        if has_explicit_cwd and os.path.normcase(owner_cwd) != os.path.normcase(cwd):
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
        existing_daemon_gen=existing_daemon_gen,
        timeout_sec=timeout_sec,
        req_key=ctx.get("req_key"),
        cancel_event=ctx.get("cancel_event"),
    )
    returned_tid = res.get("thread_id") or thread_id
    if returned_tid:
        engine_tag = "hot" if res.get("engine") == "hot-app-server" else "exec"
        record_owned_thread(returned_tid, cwd, engine_tag, daemon_gen=res.get("daemon_gen"))

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
    cwd, has_explicit_cwd = resolve_cwd(args.get("cwd"), strict=True)
    model = normalize_model(args.get("model"))
    effort = normalize_effort(args.get("effort"))
    timeout_sec = parse_timeout_sec(args.get("timeout_sec"))

    diff_section = ""
    if include_git_diff:
        if not has_explicit_cwd:
            raise ValueError("开启 include_git_diff=true 时必须同时提供有效的 Git 仓库路径 cwd。")
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
    cwd, has_explicit_cwd = resolve_cwd(args.get("cwd"), strict=True)
    if not has_explicit_cwd:
        raise ValueError("参数 cwd 无效或目录不存在；请提供目标 Git 仓库的绝对路径。")

    model = normalize_model(args.get("model"))
    effort = normalize_effort(args.get("effort"))
    base_branch = str(args.get("base") or "").strip()
    commit_sha = str(args.get("commit") or "").strip()
    focus = str(args.get("focus") or "").strip()
    timeout_sec = parse_timeout_sec(args.get("timeout_sec"))

    if base_branch and commit_sha:
        raise ValueError("参数 base 与 commit 互斥，请仅指定其中一个。")
    if base_branch:
        base_branch = _validate_git_ref_arg(base_branch, "base")
    if commit_sha:
        commit_sha = _validate_git_ref_arg(commit_sha, "commit")

    is_git, diff_summary, diff_truncated = collect_git_diff_summary(
        cwd,
        base_branch=base_branch,
        commit_sha=commit_sha,
    )
    if not is_git:
        raise ValueError(f"目录 {cwd} 不是 Git 仓库；若需非 Git 目录审查请改用 astra_redteam。")

    target_label = (
        f"commit `{commit_sha}`"
        if commit_sha
        else (f"分支差异 `{base_branch}...HEAD`" if base_branch else "工作区未提交改动")
    )
    if not diff_summary:
        return (
            f"[GPT-6.2 Astra Diff 审计 | target={target_label} | 耗时=0.0s]\n\n"
            "VERDICT: LGTM\n\n"
            f"未检测到 {target_label} 包含任何代码变更，无需调用模型审查。"
        )

    # 精准热通道：完整未截断 Diff 或调用方显式指定 focus（原生 codex exec review 不支持自定义 focus 提示词）
    if (not diff_truncated) or focus:
        focus_line = f"重点审查要求：{focus}\n" if focus else ""
        trunc_hint = (
            "注意：下方 Diff 因篇幅限制已截断，如截断部分涉及关键逻辑，可按需只读查看具体变更文件，但禁止遍历整库。\n"
            if diff_truncated
            else "下方已附目标完整 Diff，如无跨文件调用链疑问请直接给出结论，避免全库遍历。\n"
        )
        prompt = (
            f"【Git Diff 极速红队审计（只读沙箱 | 审查目标: {target_label}）】\n"
            f"{focus_line}"
            "第一行必须输出唯一独立裁决行：`VERDICT: LGTM` / `VERDICT: NEEDS_FIX` / `VERDICT: DEADLOCK`，"
            "随后按 P0 阻断 / P1 风险 / P2 建议输出简洁精准的审查意见（附文件路径与行号）。\n"
            f"{trunc_hint}\n"
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
            f"[GPT-6.2 Astra 热连接 Diff 审计 | target={target_label} | model={model} | effort={effort} | "
            f"engine={res.get('engine')} | 耗时={res['duration_sec']}s]\n\n"
        )
        return header + res["answer"]

    # 超大型截断 Diff 且无自定义 focus 时，调用剥离全局外设后的精简 `codex exec review`
    mcp_names = discover_global_mcp_names()
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
    for name in mcp_names:
        cli_args.extend(["-c", format_toml_mcp_disable_key(name)])
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
        f"[GPT-6.2 Astra 原生仓库 Diff 审计 | target={target_label} | model={model} | effort={effort} | "
        f"耗时={res['duration_sec']}s | engine=lean-codex-review]\n\n"
    )
    return header + res["answer"]


def build_tools_schema() -> list[dict[str, Any]]:
    """动态构建工具 Schema，使默认 effort 与环境变量 ASTRA_DEFAULT_EFFORT 实时一致。"""
    cur_effort = get_default_effort()
    return [
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
                "若无需 Astra 自行读盘，省略 cwd 并直接传入代码切片可获得最快响应；支持 thread_id 热续接。"
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
                        "description": "模型名称：默认 gpt-6.2-astra；日常快问/轻量讨论推荐 gpt-6.2-sol。",
                        "default": DEFAULT_MODEL,
                    },
                    "effort": {
                        "type": "string",
                        "enum": ["low", "medium", "high", "xhigh", "max", "ultra"],
                        "description": f"推理思考强度：当前默认 {cur_effort}；支持 low/medium/high/xhigh/max/ultra。",
                        "default": cur_effort,
                    },
                    "thread_id": {
                        "type": "string",
                        "description": "可选：热续接本会话签发的 thread_id（命中 Prompt Cache）。",
                    },
                    "timeout_sec": {
                        "type": "integer",
                        "description": f"超时时间（秒），默认 {DEFAULT_TIMEOUT_SEC}（范围 {MIN_TIMEOUT_SEC}–{MAX_TIMEOUT_SEC}）。",
                        "default": DEFAULT_TIMEOUT_SEC,
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
                        "default": DEFAULT_MODEL,
                    },
                    "effort": {
                        "type": "string",
                        "enum": ["low", "medium", "high", "xhigh", "max", "ultra"],
                        "description": f"当前默认 {cur_effort}；需要极限自动委派可选 ultra，快速预检可降档选 medium/high。",
                        "default": cur_effort,
                    },
                    "timeout_sec": {
                        "type": "integer",
                        "default": DEFAULT_TIMEOUT_SEC,
                    },
                },
                "required": ["proposal"],
            },
        },
        {
            "name": "astra_review_diff",
            "description": (
                "对指定 Git 仓库的未提交改动、分支差异或指定提交进行极速只读 Code Review（完整 Diff 或带 focus 优先走常驻热连接通道）。"
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
                        "description": "可选：对比的基准分支名（如 main）；与 commit 互斥，不填且无 commit 时默认审查未提交改动。",
                    },
                    "commit": {
                        "type": "string",
                        "description": "可选：审查指定 commit SHA 的变更；与 base 互斥。",
                    },
                    "focus": {
                        "type": "string",
                        "description": "可选：额外指定的重点审计方向。",
                    },
                    "model": {
                        "type": "string",
                        "default": DEFAULT_MODEL,
                    },
                    "effort": {
                        "type": "string",
                        "enum": ["low", "medium", "high", "xhigh", "max", "ultra"],
                        "default": cur_effort,
                    },
                    "timeout_sec": {
                        "type": "integer",
                        "default": DEFAULT_TIMEOUT_SEC,
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
    """线程安全的 stdio 传输层，完整兼容 NDJSON 行协议与 Content-Length/Content-Type 帧协议。"""

    def __init__(self) -> None:
        self.use_content_length = False
        self.stdin = sys.stdin.buffer
        self.stdout = sys.stdout.buffer
        self.write_lock = threading.Lock()
        self.broken = False

    def _read_exact(self, length: int) -> Optional[bytes]:
        buf = bytearray()
        while len(buf) < length:
            chunk = self.stdin.read(length - len(buf))
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    def read_message(self) -> Optional[dict[str, Any]]:
        while True:
            raw_line = self.stdin.readline()
            if not raw_line:
                return None
            stripped = raw_line.strip()
            if not stripped:
                continue

            # 判断是否为 HTTP-style 帧头（支持 Content-Length / Content-Type 任意顺序）
            if not stripped.startswith(b"{") and not stripped.startswith(b"[") and b":" in stripped:
                headers: dict[str, str] = {}
                cur_line = raw_line
                while True:
                    if not cur_line or cur_line in (b"\r\n", b"\n"):
                        break
                    if b":" in cur_line:
                        k, v = cur_line.split(b":", 1)
                        headers[k.decode("ascii", errors="ignore").strip().lower()] = v.decode(
                            "ascii", errors="ignore"
                        ).strip()
                    cur_line = self.stdin.readline()
                    if not cur_line:
                        return None

                if "content-length" not in headers:
                    raise ValueError("帧头缺少必填的 Content-Length 字段")

                self.use_content_length = True
                try:
                    length = int(headers["content-length"])
                except ValueError as exc:
                    raise ValueError(f"非法 Content-Length 值: {headers['content-length']!r}") from exc
                if length <= 0 or length > MAX_CONTENT_LENGTH_BYTES:
                    raise ValueError(f"Content-Length 超出允许范围 (1..{MAX_CONTENT_LENGTH_BYTES}): {length}")

                body = self._read_exact(length)
                if body is None:
                    return None
                parsed = json.loads(body.decode("utf-8"))
                if not isinstance(parsed, dict):
                    raise ValueError("JSON-RPC 消息体必须为 JSON Object")
                return parsed

            parsed = json.loads(stripped.decode("utf-8"))
            if not isinstance(parsed, dict):
                raise ValueError("JSON-RPC 消息体必须为 JSON Object")
            return parsed

    def write_message(self, payload: dict[str, Any]) -> None:
        if self.broken:
            return
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            with self.write_lock:
                if self.broken:
                    return
                if self.use_content_length:
                    header = f"Content-Length: {len(data)}\r\n\r\n".encode("ascii")
                    self.stdout.write(header + data)
                else:
                    self.stdout.write(data + b"\n")
                self.stdout.flush()
        except (BrokenPipeError, OSError, ValueError) as exc:
            self.broken = True
            _TRANSPORT_BROKEN_EVENT.set()
            log_err(f"stdout 传输通道已断开，触发优雅清理: {exc}")
            threading.Thread(target=shutdown_all_jobs, daemon=True).start()


def handle_cancel_notification(params: Any) -> None:
    if not isinstance(params, dict):
        return
    req_id = params.get("requestId")
    if req_id is None:
        return
    req_key = make_req_key(req_id)
    proc_to_kill = None
    cancel_cb = None
    with _JOBS_LOCK:
        job = _ACTIVE_JOBS.get(req_key)
        if job:
            job["cancel_event"].set()
            proc_to_kill = job.get("proc")
            cancel_cb = job.get("cancel_cb")

    if job:
        log_err(f"已触发请求取消: requestId={req_id}")

    # 切到后台执行管道写或 taskkill，确保主循环 0ms 阻塞
    if cancel_cb or proc_to_kill:
        def _async_cancel(cb: Any = cancel_cb, proc: Any = proc_to_kill) -> None:
            if cb:
                try:
                    cb()
                except Exception:
                    pass
            if proc:
                kill_process_tree(proc)

        threading.Thread(target=_async_cancel, daemon=True).start()


def _write_final_once(transport: StdioTransport, req_key: str, payload: dict[str, Any]) -> bool:
    """原子确保同一个请求最多只写出一条终态响应，彻底杜绝取消与自然结束重叠导致的双回包。"""
    with _JOBS_LOCK:
        job = _ACTIVE_JOBS.get(req_key)
        if not job or job["responded"]:
            return False
        job["responded"] = True
    transport.write_message(payload)
    return True


def run_tool_call_async(
    transport: StdioTransport,
    req_id: Any,
    req_key: str,
    cancel_event: threading.Event,
    tool_name: str,
    tool_args: dict[str, Any],
) -> None:
    try:
        if cancel_event.is_set():
            raise RequestCancelledError("请求在工作线程启动前已被取消")

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
    raw_params = req.get("params")
    params = raw_params if isinstance(raw_params, dict) else {}

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
            # 立即后台异步预热常驻 app-server，使用户首轮提问直接享受极速热建连
            _HOT_POOL.trigger_background_prewarm()
            return

        if method == "ping":
            transport.write_message({"jsonrpc": "2.0", "id": req_id, "result": {}})
            return

        if method == "tools/list":
            transport.write_message({"jsonrpc": "2.0", "id": req_id, "result": {"tools": build_tools_schema()}})
            return

        if method == "tools/call":
            if raw_params is not None and not isinstance(raw_params, dict):
                transport.write_message(
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": -32602, "message": "Invalid params: expected JSON object"},
                    }
                )
                return

            tool_name = str(params.get("name") or "")
            raw_args = params.get("arguments")
            if raw_args is not None and not isinstance(raw_args, dict):
                transport.write_message(
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": -32602, "message": "Invalid arguments: expected JSON object"},
                    }
                )
                return
            tool_args = raw_args or {}

            # 关键：在主线程启动 worker 之前同步原子登记 job，彻底消除瞬时 notifications/cancelled 丢失竞态
            req_key = make_req_key(req_id)
            cancel_event = threading.Event()
            with _JOBS_LOCK:
                if req_key in _ACTIVE_JOBS:
                    transport.write_message(
                        {
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "error": {"code": -32600, "message": f"Duplicate active request id: {req_id}"},
                        }
                    )
                    return
                _ACTIVE_JOBS[req_key] = {
                    "cancel_event": cancel_event,
                    "proc": None,
                    "cancel_cb": None,
                    "responded": False,
                    "count_as_active": tool_name != "astra_status",
                }

            worker = threading.Thread(
                target=run_tool_call_async,
                args=(transport, req_id, req_key, cancel_event, tool_name, tool_args),
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
        while not _TRANSPORT_BROKEN_EVENT.is_set():
            try:
                msg = transport.read_message()
                if msg is None or _TRANSPORT_BROKEN_EVENT.is_set():
                    break
                dispatch_message(transport, msg)
            except json.JSONDecodeError as exc:
                log_err(f"忽略非法 JSON 输入: {exc}")
            except ValueError as exc:
                log_err(f"忽略非法消息帧: {exc}")
            except KeyboardInterrupt:
                break
            except Exception as exc:
                log_err(f"主循环异常: {exc}")
    finally:
        shutdown_all_jobs()


if __name__ == "__main__":
    main()

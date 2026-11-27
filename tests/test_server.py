#!/usr/bin/env python3
"""
零依赖标准库端到端行为验收测试套件（v1.2.2，跨平台）：

覆盖 4 大端到端套件：
- Suite A：精简隔离 exec 降级回归、瞬时取消零丢失压力测试、int/str ID 隔离、终态严检与参数防御；
- Suite B：完整 Fake `codex app-server` 热连接全链路（后台预热、流式 delta/commentary 过滤、
           多路并发、反向审批强拒 -32000 防死锁、turn/interrupt 排空与防串轮保护、飞行中取消补发打断、
           业务失败不盲降级重跑、中途断连不盲重跑、代次隔离防误杀与跨代自动 thread/resume）；
- Suite C：真实临时 Git 仓库 `astra_review_diff` 全场景（初始空 HEAD 暂存区 + 未跟踪文件、干净仓秒回 LGTM、
           commit/base + focus 精准不误审脏工作区、互斥与 '-' 参数注入防御）；
- Suite D：HTTP-style 帧协议（Content-Type 先于 Content-Length）与 ASTRA_DEFAULT_EFFORT 动态 Schema 同步。
"""

import json
import os
import stat
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
SERVER_SCRIPT = REPO_ROOT / "codex_astra_mcp" / "server.py"

# ---------------- Fake Codex（模式 1：app-server 不可用，验证 exec 降级） ----------------
FAKE_CODEX_EXEC_ONLY_PY = r'''
import json, sys, time, uuid
if "--version" in sys.argv:
    print("fake-codex 1.2.2-exec", flush=True)
    sys.exit(0)
if "app-server" in sys.argv:
    sys.stderr.write("fake app-server unavailable\n")
    sys.exit(1)
prompt = sys.stdin.read()
print(json.dumps({"type": "thread.started", "thread_id": f"fake-tid-{uuid.uuid4().hex[:8]}"}), flush=True)
if "SLEEP_LONG" in prompt:
    time.sleep(15)
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "FINISHED_LATE"}}), flush=True)
    print(json.dumps({"type": "turn.completed"}), flush=True)
    sys.exit(0)
elif "SLEEP_SHORT" in prompt:
    time.sleep(0.35)
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "SHORT_OK"}}), flush=True)
    print(json.dumps({"type": "turn.completed"}), flush=True)
    sys.exit(0)
elif "PARTIAL_TURN_FAILED_EXIT0" in prompt:
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "PARTIAL_TEXT_TF0"}}), flush=True)
    print(json.dumps({"type": "turn.failed", "error": {"message": "turn failed despite exit 0"}}), flush=True)
    sys.exit(0)
elif "PARTIAL_NONZERO_NO_EVENT" in prompt:
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "PARTIAL_TEXT_NZ"}}), flush=True)
    sys.exit(2)
elif "CONFIG_WARNING_ONLY" in prompt:
    print(json.dumps({"type": "error", "message": "Codex is ignoring 1 unrecognized configuration setting."}), flush=True)
    print(json.dumps({"type": "error", "message": "Under-development features enabled: local_thread_store_compression."}), flush=True)
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "WARN_IGNORED_OK"}}), flush=True)
    print(json.dumps({"type": "turn.completed"}), flush=True)
    sys.exit(0)
else:
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "FAKE_OK"}}), flush=True)
    print(json.dumps({"type": "turn.completed"}), flush=True)
    sys.exit(0)
'''

# ---------------- Fake Codex（模式 2：完整模拟常驻热连接 app-server 协议） ----------------
FAKE_CODEX_HOT_PY = r'''
import json, os, sys, threading, time, uuid
from pathlib import Path

STATE_DIR = Path(os.environ["FAKE_HOT_STATE_DIR"])
EXEC_COUNT_FILE = STATE_DIR / "exec_calls.txt"
EVENTS_LOG_FILE = STATE_DIR / "events.jsonl"

def log_event(obj):
    with open(EVENTS_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

if "--version" in sys.argv:
    print("fake-codex 1.2.2-hot", flush=True)
    sys.exit(0)

if "app-server" not in sys.argv:
    # 记录任何回退 exec 调用次数，用于验证业务失败/中途断连绝不盲目降级重跑
    cur = int(EXEC_COUNT_FILE.read_text().strip()) if EXEC_COUNT_FILE.exists() else 0
    EXEC_COUNT_FILE.write_text(str(cur + 1), encoding="utf-8")
    _ = sys.stdin.read() if not sys.stdin.isatty() else ""
    print(json.dumps({"type": "thread.started", "thread_id": f"exec-tid-{uuid.uuid4().hex[:6]}"}), flush=True)
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "EXEC_FALLBACK_RAN"}}), flush=True)
    print(json.dumps({"type": "turn.completed"}), flush=True)
    sys.exit(0)

write_lock = threading.Lock()
state_lock = threading.Lock()
pending_approval = {}
active_turns = {}  # (tid, turn_id) -> {"cancel": Event, "no_drain": bool}
loaded_threads = set()

def send(obj):
    with write_lock:
        sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
        sys.stdout.flush()

def run_turn_worker(tid, turn_id, prompt, ev, no_drain):
    if "APPROVAL_TEST" in prompt:
        req_id = f"srv-req-{uuid.uuid4().hex[:6]}"
        ap_ev = threading.Event()
        with state_lock:
            pending_approval[req_id] = {"ev": ap_ev, "resp": None}
        send({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": tid, "turnId": turn_id, "command": "rm -rf /"},
        })
        ap_ev.wait(timeout=5.0)
        with state_lock:
            ap_resp = pending_approval.pop(req_id, {}).get("resp")
        code = (ap_resp or {}).get("error", {}).get("code")
        send({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {
                "threadId": tid,
                "turnId": turn_id,
                "item": {"type": "agentMessage", "text": f"APPROVAL_DENIED_CODE={code}"},
            },
        })
        send({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": tid, "turn": {"id": turn_id, "status": "completed"}}})
        return

    if "ECHO_PROMPT" in prompt:
        send({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {
                "threadId": tid,
                "turnId": turn_id,
                "item": {"type": "agentMessage", "text": "ECHO_BEGIN\n" + prompt + "\nECHO_END"},
            },
        })
        send({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": tid, "turn": {"id": turn_id, "status": "completed"}}})
        return

    if "DELTA_AND_COMMENTARY" in prompt:
        send({"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {"threadId": tid, "turnId": turn_id, "itemId": "m1", "delta": "DELTA_"}})
        send({"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {"threadId": tid, "turnId": turn_id, "itemId": "m1", "delta": "BODY"}})
        send({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "threadId": tid,
                "turn": {
                    "id": turn_id,
                    "status": "completed",
                    "items": [
                        {"type": "agentMessage", "phase": "commentary", "text": "SHOULD_NOT_WIN"},
                    ],
                },
            },
        })
        return

    if "BUSINESS_FAIL" in prompt:
        send({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "threadId": tid,
                "turn": {"id": turn_id, "status": "failed", "error": {"message": "rate limit exceeded"}},
            },
        })
        return

    if "MID_TURN_CRASH" in prompt:
        time.sleep(0.15)
        os._exit(99)

    if "SLOW_MULTIPLEX" in prompt:
        time.sleep(0.6)
        send({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"threadId": tid, "turnId": turn_id, "item": {"type": "agentMessage", "text": "SLOW_DONE"}},
        })
        send({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": tid, "turn": {"id": turn_id, "status": "completed"}}})
        return

    if "WAIT_CANCEL" in prompt:
        if ev.wait(timeout=10.0):
            if not no_drain:
                send({
                    "jsonrpc": "2.0",
                    "method": "turn/completed",
                    "params": {"threadId": tid, "turn": {"id": turn_id, "status": "interrupted"}},
                })
            return
        send({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"threadId": tid, "turnId": turn_id, "item": {"type": "agentMessage", "text": "NEVER_CANCELLED"}},
        })
        send({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": tid, "turn": {"id": turn_id, "status": "completed"}}})
        return

    # 默认快速回复（包含 Git Diff 审计请求，透传完整 prompt 供断言核验）
    res_text = "HOT_OK"
    if "Git Diff" in prompt or "Branch Diff" in prompt or "Git Commit" in prompt or "Untracked New Files" in prompt:
        res_text = "VERDICT: LGTM\nPROMPT_SNAPSHOT:\n" + prompt
    send({
        "jsonrpc": "2.0",
        "method": "item/completed",
        "params": {"threadId": tid, "turnId": turn_id, "item": {"type": "agentMessage", "text": res_text}},
    })
    send({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": tid, "turn": {"id": turn_id, "status": "completed"}}})

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    msg = json.loads(line)
    # 若是客户端对我们反向请求的响应
    if "id" in msg and "method" not in msg:
        rid = msg["id"]
        with state_lock:
            entry = pending_approval.get(rid)
        if entry:
            entry["resp"] = msg
            entry["ev"].set()
        continue

    method = msg.get("method")
    rid = msg.get("id")
    params = msg.get("params") or {}

    if method == "initialize":
        send({"jsonrpc": "2.0", "id": rid, "result": {"serverInfo": {"name": "fake-app-server"}}})
    elif method == "initialized":
        continue
    elif method == "thread/start":
        tid = f"hot-tid-{uuid.uuid4().hex[:8]}"
        loaded_threads.add(tid)
        log_event({"method": "thread/start", "tid": tid, "pid": os.getpid()})
        send({"jsonrpc": "2.0", "id": rid, "result": {"thread": {"id": tid}}})
    elif method == "thread/resume":
        tid = params.get("threadId")
        loaded_threads.add(tid)
        log_event({"method": "thread/resume", "tid": tid, "pid": os.getpid()})
        send({"jsonrpc": "2.0", "id": rid, "result": {"thread": {"id": tid}}})
    elif method == "turn/start":
        tid = params.get("threadId")
        inputs = params.get("input") or []
        prompt = inputs[0].get("text", "") if inputs else ""
        if tid not in loaded_threads:
            send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32001, "message": f"thread not loaded: {tid}"}})
            continue
        turn_id = f"turn-{uuid.uuid4().hex[:8]}"
        if "DELAY_TURN_START" in prompt:
            def _delayed_start(r=rid, t=tid, tu=turn_id):
                time.sleep(0.35)
                log_event({"method": "delayed_turn_start_replied", "tid": t, "turnId": tu})
                send({"jsonrpc": "2.0", "id": r, "result": {"turn": {"id": tu}}})
            threading.Thread(target=_delayed_start, daemon=True).start()
            continue
        send({"jsonrpc": "2.0", "id": rid, "result": {"turn": {"id": turn_id}}})
        ev = threading.Event()
        no_drain = "NO_DRAIN" in prompt
        with state_lock:
            active_turns[(tid, turn_id)] = {"cancel": ev, "no_drain": no_drain}
        threading.Thread(target=run_turn_worker, args=(tid, turn_id, prompt, ev, no_drain), daemon=True).start()
    elif method == "turn/interrupt":
        tid = params.get("threadId")
        turn_id = params.get("turnId")
        log_event({"method": "turn/interrupt", "tid": tid, "turnId": turn_id})
        with state_lock:
            tinfo = active_turns.get((tid, turn_id))
        if tinfo:
            tinfo["cancel"].set()
        if rid is not None:
            send({"jsonrpc": "2.0", "id": rid, "result": {"interrupted": True}})
'''


def create_fake_bin(tmpdir: Path, script_name: str, code: str) -> Path:
    fake_py = tmpdir / f"{script_name}.py"
    fake_py.write_text(code, encoding="utf-8")
    if os.name == "nt":
        wrapper = tmpdir / f"{script_name}.cmd"
        # 使用 %~dp0 避免含中文/Unicode 的 TEMP 路径被 cmd.exe 误解析
        wrapper.write_text(f'@echo off\n"{sys.executable}" "%~dp0{script_name}.py" %*\n', encoding="ascii")
        return wrapper
    wrapper = tmpdir / f"{script_name}.sh"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{fake_py}" "$@"\n', encoding="utf-8")
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IEXEC)
    return wrapper


class McpClientHarness:
    """支持 NDJSON 与 Content-Length 两种协议的轻量测试客户端。"""

    def __init__(self, env: dict[str, str], use_content_length: bool = False) -> None:
        self.use_content_length = use_content_length
        self.proc = subprocess.Popen(
            [sys.executable, str(SERVER_SCRIPT)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        self.responses: dict[tuple[str, Any], dict[str, Any]] = {}
        self.response_counts: dict[tuple[str, Any], int] = {}
        self.resp_events: dict[tuple[str, Any], threading.Event] = {}
        self.lock = threading.Lock()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _k(self, rid: Any) -> tuple[str, Any]:
        return (type(rid).__name__, rid)

    def _read_exact(self, length: int) -> Optional[bytes]:
        assert self.proc.stdout is not None
        buf = bytearray()
        while len(buf) < length:
            chunk = self.proc.stdout.read(length - len(buf))
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    def _read_loop(self) -> None:
        assert self.proc.stdout is not None
        while True:
            line = self.proc.stdout.readline()
            if not line:
                break
            if self.use_content_length:
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.lower().startswith(b"content-length:"):
                    length = int(stripped.split(b":", 1)[1].strip())
                    while True:
                        h = self.proc.stdout.readline()
                        if not h or h in (b"\r\n", b"\n"):
                            break
                    body = self._read_exact(length)
                    if not body:
                        break
                    obj = json.loads(body.decode("utf-8"))
                else:
                    obj = json.loads(stripped.decode("utf-8"))
            else:
                if not line.strip():
                    continue
                obj = json.loads(line.decode("utf-8"))
            rid = obj.get("id")
            k = self._k(rid)
            with self.lock:
                self.response_counts[k] = self.response_counts.get(k, 0) + 1
                self.responses[k] = obj
                if k in self.resp_events:
                    self.resp_events[k].set()

    def send(self, obj: dict[str, Any], content_type_first: bool = False) -> None:
        assert self.proc.stdin is not None
        rid = obj.get("id")
        if rid is not None:
            k = self._k(rid)
            with self.lock:
                self.resp_events[k] = threading.Event()
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        if self.use_content_length:
            if content_type_first:
                frame = (
                    b"Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n"
                    + f"Content-Length: {len(data)}\r\n\r\n".encode("ascii")
                    + data
                )
            else:
                frame = f"Content-Length: {len(data)}\r\n\r\n".encode("ascii") + data
            self.proc.stdin.write(frame)
        else:
            self.proc.stdin.write(data + b"\n")
        self.proc.stdin.flush()

    def wait_resp(self, rid: Any, timeout: float = 10.0) -> dict[str, Any]:
        k = self._k(rid)
        ev = self.resp_events[k]
        assert ev.wait(timeout=timeout), f"等待响应 id={rid!r} 超时"
        return self.responses[k]

    def count(self, rid: Any) -> int:
        with self.lock:
            return self.response_counts.get(self._k(rid), 0)

    def close(self) -> None:
        if self.proc.stdin:
            try:
                self.proc.stdin.close()
            except Exception:
                pass
        self.proc.wait(timeout=10)


def test_suite_a_exec_fallback_and_core() -> None:
    print("[Suite A] 执行精简隔离 exec 降级、并发取消、ID 隔离与边界参数测试...")
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fake_bin = create_fake_bin(tmpdir, "fake_exec", FAKE_CODEX_EXEC_ONLY_PY)
        env = os.environ.copy()
        env["CODEX_BIN"] = str(fake_bin)
        env.pop("ASTRA_DEFAULT_EFFORT", None)

        cli = McpClientHarness(env)
        try:
            # 1. MCP 初始化握手 & tools/list
            cli.send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}})
            r1 = cli.wait_resp(1)
            assert r1["result"]["serverInfo"]["name"] == "codex-astra"
            assert r1["result"]["serverInfo"]["version"] == "1.2.2"
            cli.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

            cli.send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
            r2 = cli.wait_resp(2)
            tool_names = [t["name"] for t in r2["result"]["tools"]]
            assert tool_names == ["astra_status", "astra_ask", "astra_redteam", "astra_review_diff"]
            ask_schema = next(t for t in r2["result"]["tools"] if t["name"] == "astra_ask")["inputSchema"]["properties"]
            assert ask_schema["effort"]["default"] == "max" and "ultra" in ask_schema["effort"]["enum"]

            # 2. 并发非阻塞 + 取消终止进程树
            cli.send({
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "SLEEP_LONG", "timeout_sec": 30}},
            })
            time.sleep(0.25)

            t_ping = time.monotonic()
            cli.send({"jsonrpc": "2.0", "id": 11, "method": "ping", "params": {}})
            r_ping = cli.wait_resp(11, timeout=2.0)
            assert "result" in r_ping and (time.monotonic() - t_ping) < 1.5

            cli.send({"jsonrpc": "2.0", "id": 12, "method": "tools/call", "params": {"name": "astra_status", "arguments": {}}})
            st_json = json.loads(cli.wait_resp(12, timeout=3.0)["result"]["content"][0]["text"])
            assert st_json["active_jobs"] == 1

            cli.send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 10}})
            r_10 = cli.wait_resp(10, timeout=4.0)
            assert r_10["error"]["code"] == -32800

            # 3. 瞬时背靠背取消压力测试（5 轮，验证主线程预登记 0 丢失 + 严格单回包）
            for idx in range(5):
                rid = 200 + idx
                cli.send({
                    "jsonrpc": "2.0",
                    "id": rid,
                    "method": "tools/call",
                    "params": {"name": "astra_ask", "arguments": {"prompt": "SLEEP_LONG", "timeout_sec": 30}},
                })
                cli.send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": rid}})
            for idx in range(5):
                rid = 200 + idx
                r_c = cli.wait_resp(rid, timeout=5.0)
                assert r_c["error"]["code"] == -32800, f"背靠背取消丢失 rid={rid}: {r_c}"
            time.sleep(0.25)
            for idx in range(5):
                assert cli.count(200 + idx) == 1

            # 4. JSON-RPC ID 类型区分：int(101) 与 str("101") 并发不碰撞
            cli.send({"jsonrpc": "2.0", "id": 101, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "SLEEP_SHORT"}}})
            cli.send({"jsonrpc": "2.0", "id": "101", "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "SLEEP_SHORT"}}})
            r_int = cli.wait_resp(101, timeout=10.0)
            r_str = cli.wait_resp("101", timeout=10.0)
            assert r_int["result"]["isError"] is False and "SHORT_OK" in r_int["result"]["content"][0]["text"]
            assert r_str["result"]["isError"] is False and "SHORT_OK" in r_str["result"]["content"][0]["text"]

            # 5. 单因素终态严检 & 非致命配置警告过滤
            cli.send({"jsonrpc": "2.0", "id": 20, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "PARTIAL_TURN_FAILED_EXIT0"}}})
            r_20 = cli.wait_resp(20)
            assert r_20["result"]["isError"] is True and "turn failed despite exit 0" in r_20["result"]["content"][0]["text"]

            cli.send({"jsonrpc": "2.0", "id": 21, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "PARTIAL_NONZERO_NO_EVENT"}}})
            r_21 = cli.wait_resp(21)
            assert r_21["result"]["isError"] is True and "退出码 2" in r_21["result"]["content"][0]["text"]

            cli.send({"jsonrpc": "2.0", "id": 22, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "CONFIG_WARNING_ONLY"}}})
            r_22 = cli.wait_resp(22)
            assert r_22["result"]["isError"] is False and "WARN_IGNORED_OK" in r_22["result"]["content"][0]["text"]

            # 6. astra_status 不计入 active_jobs，且连续调用命中缓存毫秒返回
            cli.send({"jsonrpc": "2.0", "id": 26, "method": "tools/call", "params": {"name": "astra_status", "arguments": {}}})
            st26 = json.loads(cli.wait_resp(26)["result"]["content"][0]["text"])
            assert st26["active_jobs"] == 0 and st26["default_effort"] == "max"

            # 7. thread_id 归属与跨工作区隔离
            cli.send({"jsonrpc": "2.0", "id": 30, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "hi", "thread_id": "foreign-unowned-tid"}}})
            assert cli.wait_resp(30)["result"]["isError"] is True

            cli.send({"jsonrpc": "2.0", "id": 31, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "hi", "cwd": str(tmpdir)}}})
            r_31 = cli.wait_resp(31)
            txt31 = r_31["result"]["content"][0]["text"]
            assert r_31["result"]["isError"] is False and "thread_id=fake-tid-" in txt31
            issued_tid = txt31.split("thread_id=")[1].split("]")[0].strip()

            cli.send({
                "jsonrpc": "2.0",
                "id": 32,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "follow-up", "thread_id": issued_tid, "cwd": str(Path.home())}},
            })
            assert "拒绝跨工作区" in cli.wait_resp(32)["result"]["content"][0]["text"]

            # 8. 边界参数防御：不存在的 cwd、非法 timeout_sec、无 cwd 的 include_git_diff
            cli.send({"jsonrpc": "2.0", "id": 40, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "hi", "cwd": str(tmpdir / "no_such_dir")}}})
            r_40 = cli.wait_resp(40)
            assert r_40["result"]["isError"] is True and "目录不存在" in r_40["result"]["content"][0]["text"]

            cli.send({"jsonrpc": "2.0", "id": 41, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "hi", "timeout_sec": True}}})
            assert cli.wait_resp(41)["result"]["isError"] is True

            cli.send({"jsonrpc": "2.0", "id": 42, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "hi", "timeout_sec": -5}}})
            assert cli.wait_resp(42)["result"]["isError"] is True

            cli.send({"jsonrpc": "2.0", "id": 421, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "hi", "timeout_sec": 15.9}}})
            assert cli.wait_resp(421)["result"]["isError"] is True

            cli.send({"jsonrpc": "2.0", "id": 43, "method": "tools/call", "params": {"name": "astra_redteam", "arguments": {"proposal": "x", "include_git_diff": True}}})
            assert cli.wait_resp(43)["result"]["isError"] is True

            # 9. 默认 max 与 ultra 档位透传
            cli.send({"jsonrpc": "2.0", "id": 44, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "ultra check", "effort": "ultra"}}})
            r_44 = cli.wait_resp(44)
            assert r_44["result"]["isError"] is False and "effort=ultra" in r_44["result"]["content"][0]["text"]
        finally:
            cli.close()
    print("  -> Suite A PASS")


def test_suite_b_hot_app_server_pool() -> None:
    print("[Suite B] 执行常驻热连接 app-server 全链路协议与并发测试...")
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        state_dir = tmpdir / "state"
        state_dir.mkdir()
        fake_bin = create_fake_bin(tmpdir, "fake_hot", FAKE_CODEX_HOT_PY)
        env = os.environ.copy()
        env["CODEX_BIN"] = str(fake_bin)
        env["FAKE_HOT_STATE_DIR"] = str(state_dir)

        cli = McpClientHarness(env)
        try:
            cli.send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}})
            cli.wait_resp(1)
            cli.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

            # 1. 等待后台静默预热完成
            deadline = time.monotonic() + 6.0
            prewarmed = False
            while time.monotonic() < deadline:
                cli.send({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "astra_status", "arguments": {}}})
                st = json.loads(cli.wait_resp(2)["result"]["content"][0]["text"])
                if st["hot_pool"]["hot_daemon_alive"] and st["hot_pool"]["prewarmed"]:
                    prewarmed = True
                    assert st["hot_pool"]["hot_daemon_gen"] == 1
                    break
                time.sleep(0.15)
            assert prewarmed, "后台预热 app-server 未在预期时间内就绪"

            # 2. 流式 delta 聚合 + 终态 turn.items commentary 过滤（不得覆盖正文）
            cli.send({"jsonrpc": "2.0", "id": 10, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "DELTA_AND_COMMENTARY"}}})
            r10 = cli.wait_resp(10)
            txt10 = r10["result"]["content"][0]["text"]
            assert r10["result"]["isError"] is False
            assert "engine=hot-app-server" in txt10
            assert "DELTA_BODY" in txt10 and "SHOULD_NOT_WIN" not in txt10

            # 3. 反向审批请求强拒 -32000 且不触发双向管道死锁
            cli.send({"jsonrpc": "2.0", "id": 11, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "APPROVAL_TEST"}}})
            r11 = cli.wait_resp(11, timeout=6.0)
            assert r11["result"]["isError"] is False
            assert "APPROVAL_DENIED_CODE=-32000" in r11["result"]["content"][0]["text"]

            # 4. 多路并发：慢请求不阻塞快请求
            t0 = time.monotonic()
            cli.send({"jsonrpc": "2.0", "id": 12, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "SLOW_MULTIPLEX"}}})
            cli.send({"jsonrpc": "2.0", "id": 13, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "FAST_NOW"}}})
            r13 = cli.wait_resp(13, timeout=4.0)
            fast_dt = time.monotonic() - t0
            r12 = cli.wait_resp(12, timeout=4.0)
            assert r13["result"]["isError"] is False and "HOT_OK" in r13["result"]["content"][0]["text"]
            assert r12["result"]["isError"] is False and "SLOW_DONE" in r12["result"]["content"][0]["text"]
            assert fast_dt < 0.5, f"快请求疑似被慢请求阻塞: {fast_dt:.2f}s"

            # 5. 业务终态失败绝不盲目降级重跑 codex exec
            cli.send({"jsonrpc": "2.0", "id": 14, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "BUSINESS_FAIL"}}})
            r14 = cli.wait_resp(14)
            assert r14["result"]["isError"] is True and "rate limit exceeded" in r14["result"]["content"][0]["text"]
            exec_cnt_file = state_dir / "exec_calls.txt"
            assert not exec_cnt_file.exists(), "业务失败被错误降级重跑了 codex exec！"

            # 6. 取消已正常排空（turn/completed 到达）-> 线程仍可安全续接
            cli.send({"jsonrpc": "2.0", "id": 15, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "INIT_CLEAN_THREAD"}}})
            r15 = cli.wait_resp(15)
            clean_tid = r15["result"]["content"][0]["text"].split("thread_id=")[1].split("]")[0].strip()

            cli.send({"jsonrpc": "2.0", "id": 16, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "WAIT_CANCEL", "thread_id": clean_tid}}})
            time.sleep(0.2)
            cli.send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 16}})
            assert cli.wait_resp(16, timeout=4.0)["error"]["code"] == -32800

            cli.send({"jsonrpc": "2.0", "id": 17, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "AFTER_CLEAN_CANCEL", "thread_id": clean_tid}}})
            r17 = cli.wait_resp(17, timeout=4.0)
            assert r17["result"]["isError"] is False and "HOT_OK" in r17["result"]["content"][0]["text"]

            # 7. 取消后未收到排空终态 -> 标记 tainted 禁止复用，杜绝迟到事件串轮
            cli.send({"jsonrpc": "2.0", "id": 18, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "WAIT_CANCEL NO_DRAIN", "thread_id": clean_tid}}})
            time.sleep(0.2)
            cli.send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 18}})
            assert cli.wait_resp(18, timeout=4.0)["error"]["code"] == -32800

            cli.send({"jsonrpc": "2.0", "id": 19, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "REUSE_TAINTED", "thread_id": clean_tid}}})
            r19 = cli.wait_resp(19, timeout=4.0)
            assert r19["result"]["isError"] is True and "已中断未排空" in r19["result"]["content"][0]["text"]

            # 8. 飞行中 turn/start 取消自动补发 turn/interrupt
            cli.send({"jsonrpc": "2.0", "id": 20, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "DELAY_TURN_START"}}})
            time.sleep(0.1)
            cli.send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 20}})
            assert cli.wait_resp(20, timeout=4.0)["error"]["code"] == -32800
            time.sleep(0.5)
            ev_lines = [json.loads(x) for x in (state_dir / "events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
            delayed_replies = [e for e in ev_lines if e.get("method") == "delayed_turn_start_replied"]
            assert delayed_replies, "未观测到延迟 turn/start 响应"
            late_turn_id = delayed_replies[-1]["turnId"]
            assert any(e.get("method") == "turn/interrupt" and e.get("turnId") == late_turn_id for e in ev_lines), (
                "取消飞行中 turn/start 后未自动补发 turn/interrupt！"
            )

            # 9. 先创建持久化线程 -> 触发中途崩溃（不盲重跑 exec）-> 新一代进程自动拉起并 thread/resume 续接成功！
            cli.send({"jsonrpc": "2.0", "id": 21, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "CREATE_FOR_RESUME"}}})
            r21 = cli.wait_resp(21)
            resume_tid = r21["result"]["content"][0]["text"].split("thread_id=")[1].split("]")[0].strip()

            cli.send({"jsonrpc": "2.0", "id": 22, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "MID_TURN_CRASH"}}})
            r22 = cli.wait_resp(22, timeout=5.0)
            assert r22["result"]["isError"] is True and "中途断开" in r22["result"]["content"][0]["text"]
            assert not exec_cnt_file.exists(), "中途断连被错误重跑了 codex exec！"

            # 跨代续接 resume_tid：自动拉起 gen=2 且先发 thread/resume
            cli.send({"jsonrpc": "2.0", "id": 23, "method": "tools/call", "params": {"name": "astra_ask", "arguments": {"prompt": "AFTER_RESTART_RESUME", "thread_id": resume_tid}}})
            r23 = cli.wait_resp(23, timeout=8.0)
            assert r23["result"]["isError"] is False and "HOT_OK" in r23["result"]["content"][0]["text"]

            cli.send({"jsonrpc": "2.0", "id": 24, "method": "tools/call", "params": {"name": "astra_status", "arguments": {}}})
            st24 = json.loads(cli.wait_resp(24)["result"]["content"][0]["text"])
            assert st24["hot_pool"]["hot_daemon_alive"] is True and st24["hot_pool"]["hot_daemon_gen"] == 2

            ev_lines2 = [json.loads(x) for x in (state_dir / "events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
            assert any(e.get("method") == "thread/resume" and e.get("tid") == resume_tid for e in ev_lines2), (
                "跨代重启后未调用 thread/resume 恢复历史会话！"
            )
        finally:
            cli.close()
    print("  -> Suite B PASS")


def test_suite_c_git_review_diff() -> None:
    print("[Suite C] 执行真实 Git 仓库 astra_review_diff 全场景与防错审测试...")
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        state_dir = tmpdir / "state"
        state_dir.mkdir()
        repo = tmpdir / "repo"
        repo.mkdir()
        fake_bin = create_fake_bin(tmpdir, "fake_hot", FAKE_CODEX_HOT_PY)

        def git(*args: str) -> str:
            p = subprocess.run(
                ["git", *args],
                cwd=str(repo),
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=True,
            )
            return p.stdout.strip()

        git("init", "-b", "main")
        git("config", "user.email", "test@example.com")
        git("config", "user.name", "Test User")

        # 场景 1：初始空 HEAD 仓库（已 staged + 未跟踪新文件）
        (repo / "staged.py").write_text("STAGED_INITIAL_TOKEN = 1\n", encoding="utf-8")
        git("add", "staged.py")
        (repo / "untracked_new.py").write_text("UNTRACKED_NEW_TOKEN = 2\n", encoding="utf-8")

        env = os.environ.copy()
        env["CODEX_BIN"] = str(fake_bin)
        env["FAKE_HOT_STATE_DIR"] = str(state_dir)
        cli = McpClientHarness(env)
        try:
            cli.send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}})
            cli.wait_resp(1)

            cli.send({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "astra_review_diff", "arguments": {"cwd": str(repo)}}})
            r2 = cli.wait_resp(2)
            t2 = r2["result"]["content"][0]["text"]
            assert r2["result"]["isError"] is False
            assert "STAGED_INITIAL_TOKEN" in t2, "初始无 HEAD 仓库的已暂存改动被遗漏！"
            assert "UNTRACKED_NEW_TOKEN" in t2, "新建未跟踪小文本文件被遗漏！"

            # 提交初始 commit c1，并创建干净工作区
            git("add", "-A")
            git("commit", "-m", "initial commit")
            c1 = git("rev-parse", "HEAD")

            # 场景 2：干净工作区秒级返回 LGTM（不调用模型）
            cli.send({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "astra_review_diff", "arguments": {"cwd": str(repo)}}})
            r3 = cli.wait_resp(3)
            t3 = r3["result"]["content"][0]["text"]
            assert r3["result"]["isError"] is False and "VERDICT: LGTM" in t3 and "无需调用模型" in t3

            # 在 feat 分支新增 commit c2，并在工作区制造未提交脏改动
            git("checkout", "-b", "feat")
            (repo / "feat_file.py").write_text("FEAT_BRANCH_TOKEN = 42\n", encoding="utf-8")
            git("add", "feat_file.py")
            git("commit", "-m", "add feat")
            c2 = git("rev-parse", "HEAD")

            (repo / "staged.py").write_text("DIRTY_UNCOMMITTED_SHOULD_NOT_APPEAR = 999\n", encoding="utf-8")

            # 场景 3（核心回归）：commit + focus 必须只审 c1，绝不可误审脏工作区，且保留 focus
            cli.send({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "astra_review_diff", "arguments": {"cwd": str(repo), "commit": c1, "focus": "FOCUS_COMMIT_AUDIT"}},
            })
            r4 = cli.wait_resp(4)
            t4 = r4["result"]["content"][0]["text"]
            assert r4["result"]["isError"] is False
            assert "STAGED_INITIAL_TOKEN" in t4 and "FOCUS_COMMIT_AUDIT" in t4
            assert "DIRTY_UNCOMMITTED_SHOULD_NOT_APPEAR" not in t4, "commit + focus 仍误审了未提交工作区！"

            # 场景 4（核心回归）：base + focus 必须只审 main...HEAD，绝不可误审脏工作区，且保留 focus
            cli.send({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "astra_review_diff", "arguments": {"cwd": str(repo), "base": "main", "focus": "FOCUS_BRANCH_AUDIT"}},
            })
            r5 = cli.wait_resp(5)
            t5 = r5["result"]["content"][0]["text"]
            assert r5["result"]["isError"] is False
            assert "FEAT_BRANCH_TOKEN" in t5 and "FOCUS_BRANCH_AUDIT" in t5
            assert "DIRTY_UNCOMMITTED_SHOULD_NOT_APPEAR" not in t5, "base + focus 仍误审了未提交工作区！"

            # 场景 5：互斥与 '-' 参数注入防御
            cli.send({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {"name": "astra_review_diff", "arguments": {"cwd": str(repo), "base": "main", "commit": c2}},
            })
            assert cli.wait_resp(6)["result"]["isError"] is True and "互斥" in cli.wait_resp(6)["result"]["content"][0]["text"]

            cli.send({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {"name": "astra_review_diff", "arguments": {"cwd": str(repo), "base": "--output=evil"}},
            })
            assert cli.wait_resp(7)["result"]["isError"] is True and "'-'" in cli.wait_resp(7)["result"]["content"][0]["text"]

            # 场景 6：拒绝空提交范围（如 HEAD..HEAD）防止误报 LGTM
            cli.send({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {"name": "astra_review_diff", "arguments": {"cwd": str(repo), "commit": "HEAD..HEAD"}},
            })
            r8 = cli.wait_resp(8)
            assert r8["result"]["isError"] is True and "单个有效提交" in r8["result"]["content"][0]["text"]
        finally:
            cli.close()
    print("  -> Suite C PASS")


def test_suite_d_content_length_and_env_effort() -> None:
    print("[Suite D] 执行 Content-Type 先于 Content-Length 帧协议与 ASTRA_DEFAULT_EFFORT 同步测试...")
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fake_bin = create_fake_bin(tmpdir, "fake_exec", FAKE_CODEX_EXEC_ONLY_PY)
        env = os.environ.copy()
        env["CODEX_BIN"] = str(fake_bin)
        env["ASTRA_DEFAULT_EFFORT"] = "high"

        cli = McpClientHarness(env, use_content_length=True)
        try:
            cli.send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}, content_type_first=True)
            r1 = cli.wait_resp(1)
            assert r1["result"]["serverInfo"]["version"] == "1.2.2"
            assert "默认推理强度 high" in r1["result"]["instructions"]

            cli.send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}, content_type_first=True)
            r2 = cli.wait_resp(2)
            for t in r2["result"]["tools"]:
                if "effort" in t["inputSchema"]["properties"]:
                    assert t["inputSchema"]["properties"]["effort"]["default"] == "high"
        finally:
            cli.close()
    print("  -> Suite D PASS")


def main() -> None:
    assert SERVER_SCRIPT.is_file(), f"未找到服务端脚本: {SERVER_SCRIPT}"
    test_suite_a_exec_fallback_and_core()
    test_suite_b_hot_app_server_pool()
    test_suite_c_git_review_diff()
    test_suite_d_content_length_and_env_effort()
    print("ALL 4 SUITES PASSED (codex-astra-mcp v1.2.2)")


if __name__ == "__main__":
    main()

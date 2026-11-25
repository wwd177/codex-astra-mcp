#!/usr/bin/env python3
"""
零依赖标准库端到端验收测试套件（跨平台）：
覆盖并发非阻塞心跳、即时取消杀进程树、瞬时取消零双回包、单因素终态严检、
非致命配置警告过滤、以及跨工作区 / 伪造 thread_id 隔离校验。
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

REPO_ROOT = Path(__file__).resolve().parent.parent
SERVER_SCRIPT = REPO_ROOT / "codex_astra_mcp" / "server.py"

FAKE_CODEX_PY = r'''
import json, sys, time, uuid
if "--version" in sys.argv:
    print("fake-codex 1.2.0", flush=True)
    sys.exit(0)
if "app-server" in sys.argv:
    # 模拟热通道不可用，验证 v1.2.0 自动无缝降级至精简隔离 exec 引擎
    sys.stderr.write("fake app-server unavailable\n")
    sys.exit(1)
prompt = sys.stdin.read()
print(json.dumps({"type": "thread.started", "thread_id": f"fake-tid-{uuid.uuid4().hex[:8]}"}), flush=True)
if "SLEEP_LONG" in prompt:
    time.sleep(15)
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "FINISHED_LATE"}}), flush=True)
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


def create_fake_codex(tmpdir: Path) -> Path:
    fake_py = tmpdir / "fake_codex.py"
    fake_py.write_text(FAKE_CODEX_PY, encoding="utf-8")
    if os.name == "nt":
        wrapper = tmpdir / "fake_codex.cmd"
        wrapper.write_text(f'@echo off\n"{sys.executable}" "{fake_py}" %*\n', encoding="utf-8")
        return wrapper
    wrapper = tmpdir / "fake_codex.sh"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{fake_py}" "$@"\n', encoding="utf-8")
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IEXEC)
    return wrapper


def main() -> None:
    assert SERVER_SCRIPT.is_file(), f"未找到服务端脚本: {SERVER_SCRIPT}"
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fake_bin = create_fake_codex(tmpdir)

        env = os.environ.copy()
        env["CODEX_BIN"] = str(fake_bin)

        proc = subprocess.Popen(
            [sys.executable, str(SERVER_SCRIPT)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        responses: dict = {}
        response_counts: dict = {}
        resp_events: dict = {}
        lock = threading.Lock()

        def reader() -> None:
            while True:
                line = proc.stdout.readline()
                if not line:
                    break
                obj = json.loads(line.decode("utf-8"))
                rid = obj.get("id")
                with lock:
                    response_counts[rid] = response_counts.get(rid, 0) + 1
                    responses[rid] = obj
                    if rid in resp_events:
                        resp_events[rid].set()

        t_reader = threading.Thread(target=reader, daemon=True)
        t_reader.start()

        def send(obj: dict) -> None:
            rid = obj.get("id")
            if rid is not None:
                with lock:
                    resp_events[rid] = threading.Event()
            proc.stdin.write((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))
            proc.stdin.flush()

        def wait_resp(rid: int, timeout: float = 10.0) -> dict:
            ev = resp_events[rid]
            assert ev.wait(timeout=timeout), f"等待响应 id={rid} 超时"
            return responses[rid]

        try:
            # 1. MCP 初始化握手
            send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}})
            r1 = wait_resp(1)
            assert r1["result"]["serverInfo"]["name"] == "codex-astra"
            assert r1["result"]["serverInfo"]["version"] == "1.2.0"
            send({"jsonrpc": "2.0", "method": "notifications/initialized"})

            send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
            r2 = wait_resp(2)
            tool_names = [t["name"] for t in r2["result"]["tools"]]
            assert tool_names == ["astra_status", "astra_ask", "astra_redteam", "astra_review_diff"]

            # 2. 并发非阻塞 + 取消终止进程树
            send({
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "SLEEP_LONG", "timeout_sec": 30}},
            })
            time.sleep(0.25)

            t_ping = time.monotonic()
            send({"jsonrpc": "2.0", "id": 11, "method": "ping", "params": {}})
            r_ping = wait_resp(11, timeout=2.0)
            assert "result" in r_ping and (time.monotonic() - t_ping) < 1.5

            send({"jsonrpc": "2.0", "id": 12, "method": "tools/call", "params": {"name": "astra_status", "arguments": {}}})
            r_st = wait_resp(12, timeout=3.0)
            st_json = json.loads(r_st["result"]["content"][0]["text"])
            assert st_json["active_jobs"] >= 1

            send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 10}})
            r_10 = wait_resp(10, timeout=4.0)
            assert r_10["error"]["code"] == -32800

            # 3. 单因素终态严检 & 非致命配置警告过滤
            send({
                "jsonrpc": "2.0",
                "id": 20,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "PARTIAL_TURN_FAILED_EXIT0"}},
            })
            r_20 = wait_resp(20)
            assert r_20["result"]["isError"] is True and "turn failed despite exit 0" in r_20["result"]["content"][0]["text"]

            send({
                "jsonrpc": "2.0",
                "id": 21,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "PARTIAL_NONZERO_NO_EVENT"}},
            })
            r_21 = wait_resp(21)
            assert r_21["result"]["isError"] is True and "退出码 2" in r_21["result"]["content"][0]["text"]

            send({
                "jsonrpc": "2.0",
                "id": 22,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "CONFIG_WARNING_ONLY"}},
            })
            r_22 = wait_resp(22)
            assert r_22["result"]["isError"] is False and "WARN_IGNORED_OK" in r_22["result"]["content"][0]["text"]

            # 4. 瞬时取消竞态与单回包断言
            send({
                "jsonrpc": "2.0",
                "id": 25,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "SLEEP_LONG", "timeout_sec": 30}},
            })
            send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 25}})
            r_25 = wait_resp(25, timeout=5.0)
            assert r_25["error"]["code"] == -32800
            time.sleep(0.3)
            with lock:
                assert response_counts.get(25) == 1, f"同一请求出现重复回包: {response_counts.get(25)}"

            send({"jsonrpc": "2.0", "id": 26, "method": "tools/call", "params": {"name": "astra_status", "arguments": {}}})
            st26 = json.loads(wait_resp(26)["result"]["content"][0]["text"])
            assert st26["active_jobs"] == 0

            # 5. thread_id 归属与跨工作区隔离
            send({
                "jsonrpc": "2.0",
                "id": 30,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "hi", "thread_id": "foreign-unowned-tid"}},
            })
            r_30 = wait_resp(30)
            assert r_30["result"]["isError"] is True and "拒绝续接" in r_30["result"]["content"][0]["text"]

            send({
                "jsonrpc": "2.0",
                "id": 31,
                "method": "tools/call",
                "params": {"name": "astra_ask", "arguments": {"prompt": "hi", "cwd": str(tmpdir)}},
            })
            r_31 = wait_resp(31)
            txt31 = r_31["result"]["content"][0]["text"]
            assert r_31["result"]["isError"] is False and "thread_id=fake-tid-" in txt31
            issued_tid = txt31.split("thread_id=")[1].split("]")[0].strip()

            send({
                "jsonrpc": "2.0",
                "id": 32,
                "method": "tools/call",
                "params": {
                    "name": "astra_ask",
                    "arguments": {"prompt": "follow-up", "thread_id": issued_tid, "cwd": str(Path.home())},
                },
            })
            r_32 = wait_resp(32)
            assert r_32["result"]["isError"] is True and "拒绝跨工作区" in r_32["result"]["content"][0]["text"]

            print("ALL TESTS PASSED (codex-astra-mcp v1.2.0)")
        finally:
            if proc.stdin:
                proc.stdin.close()
            proc.wait(timeout=10)


if __name__ == "__main__":
    main()

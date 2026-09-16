#!/usr/bin/env python3
"""Simulate interactive JupyterLab notebook connections to Dataproc Kernel Gateway via WebSocket.

Performs Method B:
1. Connects to wss://<gateway>/api/kernels/<id>/channels
2. Sends kernel_info_request (handshake) to transition kernel starting -> idle
3. Submits execute_request with PySpark code to execute real computation
4. Optionally maintains the WebSocket connection open (keepalive) to simulate active users
"""

import argparse
import json
import logging
import ssl
import subprocess
import sys
import time
from typing import Optional, Dict, Any, List

try:
    import websocket
except ImportError:
    print("Error: websocket-client is required. Run: pip install websocket-client", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("simulate_notebook")

DEFAULT_GATEWAY = "https://c4yxwrgwnjdtzg7zn3svsap3im-dot-us-central1.dataproc.googleusercontent.com/gateway/default/jupyter"


def get_token(profile: Optional[str] = None) -> str:
    """Acquire Google Cloud OAuth2 access token."""
    if profile:
        cmd = ["bash", "-c", f"source ~/.bash_profile && source ~/.gcrc && gc {profile} gcloud auth print-access-token"]
    else:
        cmd = ["bash", "-c", "gcloud auth print-access-token"]
    try:
        token = subprocess.check_output(cmd, text=True).strip()
        if token:
            return token
    except Exception as e:
        logger.warning("Failed to get token via gcloud: %s", e)
    
    # Fallback to application default
    cmd_adc = ["bash", "-c", f"source ~/.bash_profile && source ~/.gcrc && gc {profile or 'default'} gcloud auth application-default print-access-token"]
    return subprocess.check_output(cmd_adc, text=True).strip()


def get_active_kernels(gateway_url: str, token: str) -> List[Dict[str, Any]]:
    """Fetch active kernels from Kernel Gateway REST API."""
    import urllib.request
    url = f"{gateway_url}/api/kernels"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def connect_and_handshake(
    gateway_url: str,
    kernel_id: str,
    token: str,
    code: Optional[str] = None,
    keepalive: int = 0,
    user_label: str = "user",
) -> bool:
    """Connect to a kernel over WebSocket, perform handshake, and optionally execute code."""
    ws_base = gateway_url.replace("https://", "wss://").replace("http://", "ws://")
    url = f"{ws_base}/api/kernels/{kernel_id}/channels"

    logger.info("[%s] Connecting to kernel %s...", user_label, kernel_id[:8])
    ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_NONE})

    try:
        ws.connect(url, header=[f"Authorization: Bearer {token}"])
        logger.info("[%s] WebSocket connected successfully!", user_label)

        # 1. Send kernel_info_request handshake
        session_id = f"sim-session-{kernel_id[:8]}-{int(time.time())}"
        handshake_msg = {
            "header": {
                "msg_id": f"handshake-{int(time.time()*1000)}",
                "username": user_label,
                "session": session_id,
                "msg_type": "kernel_info_request",
                "version": "5.3",
            },
            "parent_header": {},
            "metadata": {},
            "content": {},
            "channel": "shell",
        }
        ws.send(json.dumps(handshake_msg))
        logger.info("[%s] Sent kernel_info_request handshake.", user_label)

        ws.settimeout(10.0)
        handshake_complete = False
        start_wait = time.time()
        while time.time() - start_wait < 15.0:
            try:
                raw = ws.recv()
                if not raw:
                    break
                msg = json.loads(raw)
                m_type = msg.get("msg_type") or msg.get("header", {}).get("msg_type")
                if m_type == "kernel_info_reply":
                    logger.info("[%s] Received kernel_info_reply! Handshake complete (kernel is now IDLE).", user_label)
                    handshake_complete = True
                    break
            except websocket.WebSocketTimeoutException:
                break

        if not handshake_complete:
            logger.warning("[%s] Timeout waiting for kernel_info_reply.", user_label)

        # 2. If code execution requested, send execute_request
        if code:
            exec_msg_id = f"exec-{int(time.time()*1000)}"
            exec_req = {
                "header": {
                    "msg_id": exec_msg_id,
                    "username": user_label,
                    "session": session_id,
                    "msg_type": "execute_request",
                    "version": "5.3",
                },
                "parent_header": {},
                "metadata": {},
                "content": {
                    "code": code,
                    "silent": False,
                    "store_history": True,
                    "user_expressions": {},
                    "allow_stdin": False,
                    "stop_on_error": True,
                },
                "channel": "shell",
            }
            ws.send(json.dumps(exec_req))
            logger.info("[%s] Sent execute_request: %r", user_label, code[:50])

            exec_done = False
            start_exec = time.time()
            while time.time() - start_exec < 30.0:
                try:
                    raw = ws.recv()
                    if not raw:
                        break
                    msg = json.loads(raw)
                    m_type = msg.get("msg_type") or msg.get("header", {}).get("msg_type")
                    content = msg.get("content", {})
                    if m_type == "stream":
                        text = content.get("text", "").strip()
                        if text:
                            logger.info("[%s][OUTPUT] %s", user_label, text)
                    elif m_type == "execute_result":
                        data = content.get("data", {})
                        logger.info("[%s][RESULT] %s", user_label, data.get("text/plain", ""))
                    elif m_type == "execute_reply":
                        status = content.get("status")
                        logger.info("[%s] Execution finished with status: %s", user_label, status)
                        exec_done = True
                        break
                except websocket.WebSocketTimeoutException:
                    break

            if not exec_done:
                logger.warning("[%s] Execution timed out waiting for reply.", user_label)

        # 3. If keepalive requested, stay connected
        if keepalive > 0:
            logger.info("[%s] Keeping WebSocket connection open for %d seconds...", user_label, keepalive)
            time.sleep(keepalive)
            logger.info("[%s] Keepalive duration reached, closing connection.", user_label)

        return True

    except Exception as exc:
        logger.error("[%s] WebSocket error: %s", user_label, exc)
        return False
    finally:
        try:
            ws.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Simulate interactive notebook connections to Dataproc Kernel Gateway.")
    parser.add_argument("--gateway-url", default=DEFAULT_GATEWAY, help="Base Component Gateway URL")
    parser.add_argument("--kernel-id", help="Target kernel ID (or use --all)")
    parser.add_argument("--profile", default="admin--kenly-lakehouse-dev-1", help="gcloud profile for credentials")
    parser.add_argument("--token", help="Direct OAuth2 Bearer token")
    parser.add_argument("--code", default="print('Interactive JupyterLab notebook session connected!')", help="Python/PySpark code to execute")
    parser.add_argument("--keepalive", type=int, default=0, help="Seconds to keep connection open")
    parser.add_argument("--all-pyspark", action="store_true", help="Connect to all active PySpark kernels")
    args = parser.parse_args()

    token = args.token or get_token(args.profile)
    if not token:
        logger.error("Could not obtain access token.")
        sys.exit(1)

    if args.all_pyspark:
        kernels = get_active_kernels(args.gateway_url, token)
        pyspark_kernels = [k for k in kernels if "pyspark" in k.get("name", "")]
        logger.info("Found %d active PySpark kernel(s).", len(pyspark_kernels))
        for k in pyspark_kernels:
            k_id = k["id"]
            state = k.get("execution_state")
            logger.info("Processing kernel %s (Current state: %s)...", k_id, state)
            connect_and_handshake(
                args.gateway_url,
                k_id,
                token,
                code=args.code,
                keepalive=args.keepalive,
                user_label=args.profile.split("--")[0],
            )
            time.sleep(1)
    elif args.kernel_id:
        connect_and_handshake(
            args.gateway_url,
            args.kernel_id,
            token,
            code=args.code,
            keepalive=args.keepalive,
            user_label=args.profile.split("--")[0],
        )
    else:
        logger.error("Specify either --kernel-id <id> or --all-pyspark.")
        sys.exit(1)


if __name__ == "__main__":
    main()

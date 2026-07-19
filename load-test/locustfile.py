"""
Load test for baltazar-gateway (Raw STOMP over WebSocket).

Modes
-----
Local (default):
    Requires docker-compose.local.yml to be running (Redis + mock backend).
    Encryption key is fixed; no real auth token needed.

    locust -f locustfile.py --host ws://localhost:8080

Remote (test / prod):
    Uses a real JWT token and a real encryption key fetched from the environment.

    LOAD_ENV=remote AUTH_TOKEN="Bearer <jwt>" ENC_KEY=<base64-24-byte-key> \
        locust -f locustfile.py --host wss://api.game.com

User behaviour
--------------
Each virtual user represents a game session:
  1. Open WebSocket (direct /ws endpoint)
  2. Handshake: send CONNECT → expect CONNECTED
  3. Subscribe to /topic/public/{gameId}
  4. Loop: send one of the simulated game commands (FETCH_DATA, MOVE_CONFIRM,
     GET_HINT, START_TIMER, RESET_TIME, SEND_EMOJI) on a random weight schedule
  5. On disconnect: send STOMP DISCONNECT

Metrics reported
----------------
- "ws_connect"      — full raw STOMP handshake latency
- "ws_subscribe"    — SUBSCRIBE round-trip (no server reply, just timing the send)
- "cmd_<TYPE>"      — each individual game command send latency
- Connection count is visible in Locust's user count panel
"""

import json
import os
import random
import threading
import time
from base64 import b64decode, b64encode

from Crypto.Cipher import DES3
from locust import User, between, events, task
from locust.exception import StopUser
from websocket import WebSocket, WebSocketTimeoutException

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOAD_ENV = os.environ.get("LOAD_ENV", "local")  # "local" | "remote"
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "Bearer load-test-token")  # noqa: E501
# 24-byte 3DES key, Base64-encoded. The local mock server uses this fixed key.
ENC_KEY_B64 = os.environ.get("ENC_KEY", "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIz")

WS_PATH = "/ws"

# Game IDs to use — local mock ignores them; remote needs real IDs
GAME_IDS = list(map(int, os.environ.get("GAME_IDS", "1,2,3,4,5").split(",")))


# ---------------------------------------------------------------------------
# Crypto helper (mirrors TripleDes.kt)
# ---------------------------------------------------------------------------

def _triple_des_encrypt(plain: str, key_b64: str) -> str:
    key = b64decode(key_b64)
    # DES3 ECB — same mode as the Kotlin TripleDes ("DESede" = ECB, PKCS5)
    padded = _pkcs5_pad(plain.encode("utf-8"))
    cipher = DES3.new(key, DES3.MODE_ECB)
    return b64encode(cipher.encrypt(padded)).decode("ascii")


def _pkcs5_pad(data: bytes, block_size: int = 8) -> bytes:
    pad_len = block_size - (len(data) % block_size)
    return data + bytes([pad_len] * pad_len)


# ---------------------------------------------------------------------------
# STOMP wire helpers (RAW STOMP, no SockJS)
# ---------------------------------------------------------------------------

def _stomp_frame(command: str, headers: dict, body: str = "") -> str:
    """Build a raw STOMP frame."""
    lines = [command]
    for k, v in headers.items():
        lines.append(f"{k}:{v}")
    lines.append("")
    lines.append(body)
    return "\n".join(lines) + "\x00"


def _make_outer_envelope(game_id: int, payload: dict, enc_key: str) -> str:
    """Encrypt payload dict and wrap in the {gameId, encryptedData} outer envelope."""
    plain = json.dumps(payload)
    encrypted = _triple_des_encrypt(plain, enc_key)
    return json.dumps({"gameId": game_id, "encryptedData": encrypted})


# ---------------------------------------------------------------------------
# Locust WebSocket user
# ---------------------------------------------------------------------------

class GatewayUser(User):
    """
    One virtual user = one game session (WebSocket connection).

    wait_time controls the pause between game commands.
    """
    wait_time = between(1, 4)

    # ------------------------------------------------------------------ #
    # Lifecycle                                                             #
    # ------------------------------------------------------------------ #

    def on_start(self):
        self.game_id = random.choice(GAME_IDS)
        self.enc_key = ENC_KEY_B64
        self.sub_id = "sub-0"
        self.ws: WebSocket | None = None
        self._recv_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._last_msg: str | None = None
        self._connected = threading.Event()

        self._open_connection()

    def on_stop(self):
        self._disconnect()

    # ------------------------------------------------------------------ #
    # Connection management                                                #
    # ------------------------------------------------------------------ #

    def _open_connection(self):
        """Open WebSocket and perform raw STOMP handshake."""
        host = self.host  # e.g. "ws://localhost:8080" or "wss://api.game.com"
        url = f"{host}{WS_PATH}"  # Direct WebSocket URL (no SockJS session)

        t0 = time.monotonic()
        try:
            ws = WebSocket()
            ws.connect(
                url,
                header={"Authorization": AUTH_TOKEN},
                timeout=10,
            )

            # Send raw STOMP CONNECT frame (no SockJS wrapping)
            connect_frame = _stomp_frame(
                "CONNECT",
                {
                    "accept-version": "1.1",
                    "heart-beat": "0,0",  # No heartbeat for load test
                    "Authorization": AUTH_TOKEN,
                },
            )
            ws.send(connect_frame)

            # Expect raw STOMP CONNECTED frame
            resp = ws.recv()
            if not resp.startswith("CONNECTED"):
                raise RuntimeError(f"Expected CONNECTED, got: {resp!r}")

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            events.request.fire(
                request_type="WebSocket",
                name="ws_connect",
                response_time=elapsed_ms,
                response_length=len(resp),
                exception=None,
                context={},
            )

            self.ws = ws
            self._start_recv_thread()
            self._subscribe()

        except Exception as e:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            events.request.fire(
                request_type="WebSocket",
                name="ws_connect",
                response_time=elapsed_ms,
                response_length=0,
                exception=e,
                context={},
            )
            raise StopUser()

    def _subscribe(self):
        """Send STOMP SUBSCRIBE frame."""
        destination = f"/topic/public/{self.game_id}"
        t0 = time.monotonic()
        try:
            sub_frame = _stomp_frame(
                "SUBSCRIBE",
                {"destination": destination, "id": self.sub_id},
            )
            self.ws.send(sub_frame)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            events.request.fire(
                request_type="WebSocket",
                name="ws_subscribe",
                response_time=elapsed_ms,
                response_length=0,
                exception=None,
                context={},
            )
        except Exception as e:
            events.request.fire(
                request_type="WebSocket",
                name="ws_subscribe",
                response_time=0,
                response_length=0,
                exception=e,
                context={},
            )

    def _disconnect(self):
        """Send STOMP DISCONNECT and close WebSocket."""
        if self.ws:
            try:
                disc = _stomp_frame("DISCONNECT", {"receipt": "disc-1"})
                self.ws.send(disc)
                self.ws.close()
            except Exception:
                pass
            self.ws = None

    # ------------------------------------------------------------------ #
    # Background receive thread — drains server pushes without blocking   #
    # ------------------------------------------------------------------ #

    def _start_recv_thread(self):
        """Start background thread to receive messages."""
        def _recv_loop():
            while self.ws and self.ws.connected:
                try:
                    msg = self.ws.recv()
                    with self._lock:
                        self._last_msg = msg
                except WebSocketTimeoutException:
                    pass
                except Exception:
                    break

        t = threading.Thread(target=_recv_loop, daemon=True)
        t.start()
        self._recv_thread = t

    # ------------------------------------------------------------------ #
    # Helper to send a game command and record latency                    #
    # ------------------------------------------------------------------ #

    def _send_command(self, msg_type: str, extra: dict | None = None):
        """Send a game command as raw STOMP frame."""
        if not self.ws or not self.ws.connected:
            raise StopUser()

        payload = {"type": msg_type, "gameId": self.game_id}
        if extra:
            payload.update(extra)

        body = _make_outer_envelope(self.game_id, payload, self.enc_key)
        stomp = _stomp_frame(
            "SEND",
            {"destination": "/app/game.sendMessage"},
            body,
        )
        t0 = time.monotonic()
        try:
            self.ws.send(stomp)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            events.request.fire(
                request_type="WebSocket",
                name=f"cmd_{msg_type}",
                response_time=elapsed_ms,
                response_length=len(body),
                exception=None,
                context={},
            )
        except Exception as e:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            events.request.fire(
                request_type="WebSocket",
                name=f"cmd_{msg_type}",
                response_time=elapsed_ms,
                response_length=0,
                exception=e,
                context={},
            )
            raise StopUser()

    # ------------------------------------------------------------------ #
    # Game command tasks (weighted)                                        #
    # ------------------------------------------------------------------ #

    @task(5)
    def fetch_data(self):
        self._send_command("FETCH_DATA")

    @task(10)
    def move_confirm(self):
        move_index = random.randint(0, 49)
        indices = random.sample(range(100), k=random.randint(1, 5))
        self._send_command(
            "MOVE_CONFIRM",
            {
                "moveDto": {
                    "indices": indices,
                    "moveIndex": move_index,
                    "ai": False,
                },
                "senderUserId": random.randint(1, 9999),
            },
        )

    @task(3)
    def get_hint(self):
        move_index = random.randint(0, 49)
        self._send_command(
            "GET_HINT",
            {"clientRequest": {"moveIndex": move_index, "cellIndex": 0, "gameId": self.game_id}},
        )

    @task(2)
    def start_timer(self):
        move_index = random.randint(0, 49)
        self._send_command(
            "START_TIMER",
            {"clientRequest": {"moveIndex": move_index, "cellIndex": 0, "gameId": self.game_id}},
        )

    @task(2)
    def reset_time(self):
        move_index = random.randint(0, 49)
        self._send_command(
            "RESET_TIME",
            {"clientRequest": {"moveIndex": move_index, "cellIndex": 0, "gameId": self.game_id}},
        )

    @task(1)
    def send_emoji(self):
        self._send_command("SEND_EMOJI")

    @task(1)
    def send_chat(self):
        self._send_command("SEND_CHAT")

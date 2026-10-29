"""Music Room: one authoritative transport clock shared by all listeners.

Prototype: in-memory rooms and one process. A restart clears rooms. Audio bytes
stay in Supabase Storage; this backend transports control state, never MP3 data.
"""
import asyncio
import hmac
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

app = FastAPI(title="Music Room sync")
ROOT = Path(__file__).resolve().parent
ROOM_CODE = re.compile(r"^[A-Z0-9]{6,8}$")
YT = re.compile(r"^youtube://[A-Za-z0-9_-]{11}$")
MAX_ROOMS = 200


def milliseconds() -> int:
    return int(time.time() * 1000)


@dataclass
class Room:
    token: str
    state: dict[str, Any] = field(default_factory=lambda: {
        "src": "", "title": "", "playing": False,
        "position": 0.0, "at": milliseconds(), "seq": 0,
    })
    sockets: set[WebSocket] = field(default_factory=set)
    owner: WebSocket | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


rooms: dict[str, Room] = {}


def safe_code(code: str) -> str:
    code = code.upper()
    if not ROOM_CODE.fullmatch(code):
        raise HTTPException(400, "Неверный код комнаты")
    return code


def current_position(state: dict[str, Any], now: int) -> float:
    pos = state["position"]
    if state["playing"]:
        pos += max(0, now - state["at"]) / 1000
    return pos


def valid_source(src: str) -> bool:
    # Links only (uploaded tracks are on Supabase Storage). No file:// or javascript:.
    return src == "demo://midnight-room" or bool(YT.fullmatch(src)) or (
        src.startswith("https://") and len(src) <= 1200 and not any(x.isspace() for x in src)
    )


@app.get("/")
async def index():
    return FileResponse(ROOT / "index.html", media_type="text/html")


@app.get("/health")
async def health():
    return {"ok": True, "rooms": len(rooms)}


@app.post("/api/rooms/{code}", status_code=201)
async def create_room(code: str):
    code = safe_code(code)
    if code in rooms:
        raise HTTPException(409, "Комната уже существует: используй свой код ведущего или создай новую")
    if len(rooms) >= MAX_ROOMS:
        raise HTTPException(503, "Слишком много комнат. Повтори позднее")
    secret = secrets.token_urlsafe(32)
    rooms[code] = Room(token=secret)
    return {"room": code, "host_token": secret}


async def broadcast(room: Room, msg: dict[str, Any]):
    lost = []
    for socket in list(room.sockets):
        try:
            await socket.send_json(msg)
        except Exception:
            lost.append(socket)
    for socket in lost:
        room.sockets.discard(socket)
        if room.owner is socket:
            room.owner = None


def apply_command(room: Room, msg: dict[str, Any]) -> bool:
    action = msg.get("action")
    old = room.state
    now = milliseconds()
    if action == "track":
        src = msg.get("src", "")
        if not isinstance(src, str) or not valid_source(src):
            return False
        title = str(msg.get("title", ""))[:80]
        new = {"src": src, "title": title, "playing": False, "position": 0.0,
               "at": now, "seq": old["seq"] + 1}
    elif action in ("play", "pause", "seek"):
        if not old["src"]:
            return False
        supplied = msg.get("position")
        if not isinstance(supplied, (int, float)) or isinstance(supplied, bool) or not (0 <= supplied < 100000):
            return False
        position = float(supplied)
        new = {**old, "position": position, "at": now, "seq": old["seq"] + 1}
        if action == "play":
            new["playing"] = True
        elif action == "pause":
            new["playing"] = False
    else:
        return False
    room.state = new
    return True


@app.websocket("/ws/{code}")
async def room_socket(socket: WebSocket, code: str):
    code = code.upper()
    if not ROOM_CODE.fullmatch(code) or code not in rooms:
        await socket.close(code=4404, reason="Комната не найдена")
        return
    room = rooms[code]
    role = socket.query_params.get("role")
    is_owner = role == "host"
    token = socket.query_params.get("token", "")
    if is_owner and not hmac.compare_digest(token, room.token):
        await socket.close(code=4403, reason="Неверный код ведущего")
        return
    if role not in ("host", "guest"):
        await socket.close(code=4403)
        return

    async with room.lock:
        if is_owner and room.owner is not None:
            await socket.close(code=4409, reason="Ведущий уже в комнате в другой вкладке")
            return
        await socket.accept()
        room.sockets.add(socket)
        if is_owner:
            room.owner = socket
        await socket.send_json({"type": "state", "state": room.state, "serverTime": milliseconds()})
    try:
        while True:
            msg = await socket.receive_json()
            if not isinstance(msg, dict):
                continue
            kind = msg.get("type")
            if kind == "ping":
                t1 = milliseconds()
                await socket.send_json({"type": "pong", "t0": msg.get("t0"), "t1": t1, "t2": milliseconds()})
            elif kind == "get_state":
                async with room.lock:
                    await socket.send_json({"type": "state", "state": room.state, "serverTime": milliseconds()})
            elif kind == "command" and is_owner:
                async with room.lock:
                    if room.owner is socket and apply_command(room, msg):
                        await broadcast(room, {"type": "state", "state": room.state, "serverTime": milliseconds()})
            else:
                await socket.send_json({"type": "error", "message": "Команда недоступна"})
    except (WebSocketDisconnect, RuntimeError, ValueError):
        pass
    finally:
        async with room.lock:
            room.sockets.discard(socket)
            if room.owner is socket:
                room.owner = None

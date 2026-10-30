"""Music Room: one authoritative transport clock shared by all listeners.

Prototype: in-memory rooms and one process. A restart clears rooms. Audio bytes
stay in Supabase Storage; this backend transports control state, never MP3 data.
"""
import asyncio
import os
import httpx
import hmac
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse, RedirectResponse

app = FastAPI(title="Music Room sync")
ROOT = Path(__file__).resolve().parent
ROOM_CODE = re.compile(r"^[A-Z0-9]{6,8}$")
YT = re.compile(r"^youtube://[A-Za-z0-9_-]{11}$")
MAX_ROOMS = 200
VERSION = "1.6"
BOOT_ID = secrets.token_hex(8)


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
    return {"ok": True, "version": VERSION, "rooms": len(rooms), "boot": BOOT_ID}




# Home-page music discovery. Audius permits streaming where the artist has enabled
# API access; downloads are offered only for artist-enabled public downloads.
AUDIUS = "https://api.audius.co/v1"
APPLE = "https://itunes.apple.com/search"
APP_NAME = "music-room-demo"
AUDIUS_KEY = os.getenv("AUDIUS_API_KEY", "").strip()
TRACK_ID = re.compile(r"^[A-Za-z0-9_-]{1,40}$")


def audius_headers():
    headers = {"Accept": "application/json", "User-Agent": "MusicRoom/1.6"}
    if AUDIUS_KEY:
        headers["x-api-key"] = AUDIUS_KEY
    return headers


async def get_provider_json(url: str, params: dict | None = None, *, audius=False):
    try:
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=False) as http:
            response = await http.get(url, params=params, headers=audius_headers() if audius else {"Accept": "application/json"})
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError("Unexpected provider response")
            return data
    except (httpx.HTTPError, ValueError) as err:
        raise HTTPException(status_code=502, detail="Музыкальный каталог временно недоступен. Повтори поиск позже.") from err


def audius_downloadable(track):
    # Paid, gated and non-downloadable tracks must not get a download action.
    return (track.get("is_downloadable") is True
            and track.get("is_download_gated") is not True
            and track.get("is_stream_gated") is not True)


@app.get("/api/music/search")
async def music_search(q: str = Query(min_length=2, max_length=90), source: str = "audius"):
    q = q.strip()
    if len(q) < 2:
        raise HTTPException(400, "Название песни должно содержать хотя бы 2 символа")
    if source == "audius":
        result = await get_provider_json(f"{AUDIUS}/tracks/search",
            {"query": q, "limit": 14, "app_name": APP_NAME}, audius=True)
        items = []
        for tr in result.get("data", []) if isinstance(result.get("data"), list) else []:
            if not isinstance(tr, dict) or tr.get("is_streamable") is False or tr.get("is_stream_gated") is True:
                continue
            track_id = str(tr.get("id", ""))
            if not TRACK_ID.fullmatch(track_id):
                continue
            artist = tr.get("user") or {}
            artwork = tr.get("artwork") or {}
            cover = artwork.get("480x480") or artwork.get("150x150") or "" if isinstance(artwork, dict) else ""
            canonical = tr.get("permalink") or ""
            if not isinstance(canonical, str) or not canonical.startswith("https://audius.co/"):
                canonical = "https://audius.co"
            items.append({"id": track_id, "title": str(tr.get("title") or "Без названия")[:100],
                "artist": str(artist.get("name") or "Исполнитель")[:80] if isinstance(artist, dict) else "Исполнитель",
                "artwork": cover if isinstance(cover, str) and cover.startswith("https://") else "",
                "url": canonical, "stream": f"/api/music/tracks/{track_id}/stream",
                "download": f"/api/music/tracks/{track_id}/download" if audius_downloadable(tr) else "",
                "duration": int(tr.get("duration") or 0), "provider": "audius"})
        return {"results": items, "source": "audius"}
    if source == "catalog":
        result = await get_provider_json(APPLE,
            {"term": q, "entity": "song", "media": "music", "limit": 14})
        items = []
        for tr in result.get("results", []) if isinstance(result.get("results"), list) else []:
            link = str(tr.get("trackViewUrl") or "")
            if not link.startswith("https://"):
                continue
            cover = str(tr.get("artworkUrl100") or "")
            items.append({"id": str(tr.get("trackId") or ""), "title": str(tr.get("trackName") or "Без названия")[:100],
                "artist": str(tr.get("artistName") or "Исполнитель")[:80],
                "artwork": cover if cover.startswith("https://") else "",
                "url": link, "stream": "", "download": "", "duration": int(tr.get("trackTimeMillis") or 0)//1000,
                "provider": "catalog"})
        return {"results": items, "source": "catalog"}
    raise HTTPException(400, "Неизвестный музыкальный источник")


@app.get("/api/music/tracks/{track_id}/stream")
async def music_stream(track_id: str):
    if not TRACK_ID.fullmatch(track_id):
        raise HTTPException(400, "Неверный ID песни")
    return RedirectResponse(f"{AUDIUS}/tracks/{track_id}/stream?app_name={APP_NAME}", status_code=307)


@app.get("/api/music/tracks/{track_id}/download")
async def music_download(track_id: str):
    if not TRACK_ID.fullmatch(track_id):
        raise HTTPException(400, "Неверный ID песни")
    data = await get_provider_json(f"{AUDIUS}/tracks/{track_id}", {"app_name": APP_NAME}, audius=True)
    tr = data.get("data", {})
    if not isinstance(tr, dict) or not audius_downloadable(tr):
        raise HTTPException(403, "Автор не разрешил свободное скачивание этой песни")
    return RedirectResponse(f"{AUDIUS}/tracks/{track_id}/download?app_name={APP_NAME}", status_code=307)


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
        await socket.send_json({"type": "state", "state": room.state, "serverTime": milliseconds(), "snapshot": True, "boot": BOOT_ID})
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
                    await socket.send_json({"type": "state", "state": room.state, "serverTime": milliseconds(), "snapshot": True, "boot": BOOT_ID})
            elif kind == "command" and is_owner:
                async with room.lock:
                    if room.owner is socket and apply_command(room, msg):
                        await broadcast(room, {"type": "state", "state": room.state,
                                               "serverTime": milliseconds(), "boot": BOOT_ID,
                                               "commandId": msg.get("commandId")})
                    else:
                        await socket.send_json({"type": "error", "message": "Неверная команда музыки"})
            else:
                await socket.send_json({"type": "error", "message": "Команда недоступна"})
    except (WebSocketDisconnect, RuntimeError, ValueError):
        pass
    finally:
        async with room.lock:
            room.sockets.discard(socket)
            if room.owner is socket:
                room.owner = None

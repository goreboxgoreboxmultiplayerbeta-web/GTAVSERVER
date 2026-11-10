"""Grand-V protocol-1 WebSocket relay for single-instance Render Web Service.

Compatible message shapes are based on the upstream Grand-V relay.rs.
No Rockstar game files or assets are included.
"""

import asyncio
import hmac
import json
import logging
import math
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from aiohttp import WSMsgType, web

LOG = logging.getLogger("grandv.relay")
ROOM_NAME = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
BUILD_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,96}$")
MAX_SESSIONS = 64
MAX_PEERS_PER_ROOM = 2
MAX_MESSAGE_SIZE = 4096


@dataclass
class Peer:
    id: str
    ws: web.WebSocketResponse
    events: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=32))
    latest: dict = field(default_factory=dict)
    writer: Optional[asyncio.Task] = None
    last_seq: Optional[int] = None
    tokens: float = 30.0
    token_clock: float = field(default_factory=time.monotonic)


@dataclass
class Room:
    build: str
    map_name: str
    peers: dict = field(default_factory=dict)


@dataclass
class RelayState:
    access_code: str
    rooms: dict = field(default_factory=dict)
    sessions: int = 0


STATE_KEY = web.AppKey("grandv_relay_state", RelayState)


def is_uint(value, maximum):
    return type(value) is int and 0 <= value <= maximum


def valid_vector(value, bound):
    return (
        isinstance(value, list)
        and len(value) == 3
        and all(
            type(n) in (int, float)
            and math.isfinite(n)
            and abs(n) <= bound
            for n in value
        )
    )


def clean_pose(message):
    """Accept exactly the fields the game expects, like upstream snapshot()."""
    if not isinstance(message, dict) or message.get("type") != "pose":
        raise ValueError("Unsupported message")
    if not is_uint(message.get("seq"), 2147483647):
        raise ValueError("Invalid sequence")
    if not is_uint(message.get("model"), 4294967295):
        raise ValueError("Invalid model")
    if not valid_vector(message.get("position"), 20000):
        raise ValueError("Invalid position")
    if not valid_vector(message.get("rotation"), 360):
        raise ValueError("Invalid rotation")
    return {key: message[key] for key in ("type", "seq", "model", "position", "rotation")}


def validated_hello(hello, code):
    if not isinstance(hello, dict) or hello.get("type") not in ("join", "check") or not is_uint(hello.get("protocol"), 1) or hello.get("protocol") != 1:
        return "Protocol mismatch"
    if not isinstance(hello.get("key"), str) or not hmac.compare_digest(hello["key"].strip(), code):
        return "Join key rejected"
    if not isinstance(hello.get("room"), str) or ROOM_NAME.fullmatch(hello["room"]) is None:
        return "Invalid room"
    if (
        not isinstance(hello.get("build"), str)
        or BUILD_NAME.fullmatch(hello["build"]) is None
        or hello.get("map") not in ("gta5", "env_test")
    ):
        return "Invalid build/map"
    return None


def push_event(peer, packet):
    try:
        peer.events.put_nowait(packet)
    except asyncio.QueueFull:
        # A slow peer must not block the other player.
        asyncio.create_task(peer.ws.close(code=1013, message=b"Receiver too slow"))


async def peer_writer(peer):
    """Flush reliable state changes promptly; coalesce frequent poses (~20 Hz)."""
    try:
        while not peer.ws.closed:
            try:
                event = await asyncio.wait_for(peer.events.get(), timeout=0.05)
                await asyncio.wait_for(peer.ws.send_json(event), timeout=2)
                while not peer.events.empty():
                    await asyncio.wait_for(peer.ws.send_json(peer.events.get_nowait()), timeout=2)
            except asyncio.TimeoutError:
                pass
            if peer.latest:
                poses = list(peer.latest.values())
                peer.latest.clear()
                for pose in poses:
                    await asyncio.wait_for(peer.ws.send_json(pose), timeout=2)
    except asyncio.CancelledError:
        raise
    except (OSError, RuntimeError, ConnectionError, asyncio.TimeoutError) as exc:
        LOG.info("Writer stopped: %s", type(exc).__name__)
        if not peer.ws.closed:
            await peer.ws.close(code=1013, message=b"Receiver stalled")


async def health(request):
    state = request.app[STATE_KEY]
    return web.json_response({
        "status": "ok", "name": "Grand-V Relay", "protocol": 1,
        "websocket": "/relay", "rooms": len(state.rooms),
        "players": sum(len(room.peers) for room in state.rooms.values()),
        "max_players_per_room": MAX_PEERS_PER_ROOM,
    })


async def diagnostic(request):
    page = Path(__file__).with_name("diagnostic.html").read_text(encoding="utf-8")
    return web.Response(text=page, content_type="text/html", headers={"Cache-Control": "no-store"})


async def relay(request):
    state = request.app[STATE_KEY]
    ws = web.WebSocketResponse(
        heartbeat=20.0, max_msg_size=MAX_MESSAGE_SIZE, compress=False
    )
    await ws.prepare(request)
    if state.sessions >= MAX_SESSIONS:
        await ws.close(code=1013, message=b"Server full")
        return ws

    state.sessions += 1
    peer = None
    room_name = None
    try:
        try:
            initial = await asyncio.wait_for(ws.receive(), timeout=5)
            if initial.type != WSMsgType.TEXT:
                raise ValueError("Invalid or incomplete request")
            hello = json.loads(initial.data)
        except (asyncio.TimeoutError, json.JSONDecodeError, ValueError):
            await ws.close(code=1008, message=b"Invalid or incomplete request")
            return ws

        error = validated_hello(hello, state.access_code)
        if error:
            LOG.info("Relay handshake rejected: %s", error)
            await ws.close(code=1008, message=error.encode())
            return ws

        room_name = hello["room"]
        build = hello["build"]
        map_name = hello["map"]
        room = state.rooms.get(room_name)
        if room and (room.build != build or room.map_name != map_name):
            await ws.close(code=1008, message=b"Room build/map mismatch")
            return ws
        if room and len(room.peers) >= MAX_PEERS_PER_ROOM:
            await ws.close(code=1008, message=b"Room full")
            return ws
        if hello["type"] == "check":
            await ws.send_json({
                "type": "checked", "players": len(room.peers) if room else 0,
                "capacity": MAX_PEERS_PER_ROOM,
            })
            await ws.close(code=1000, message=b"Check complete")
            return ws

        # A single event-loop worker means room mutation here is atomic.
        if room is None:
            room = Room(build=build, map_name=map_name)
            state.rooms[room_name] = room

        peer = Peer(id=secrets.token_hex(8), ws=ws)
        existing_ids = list(room.peers.keys())
        room.peers[peer.id] = peer
        push_event(peer, {
            "type": "welcome", "protocol": 1, "id": peer.id,
            "peers": existing_ids, "hz": 20,
        })
        peer.writer = asyncio.create_task(peer_writer(peer))
        for existing_id in existing_ids:
            push_event(room.peers[existing_id], {"type": "joined", "id": peer.id})
        LOG.info("Player joined room %s (%d/2)", room_name, len(room.peers))

        async for message in ws:
            if message.type == WSMsgType.TEXT:
                now = time.monotonic()
                peer.tokens = min(30.0, peer.tokens + (now - peer.token_clock) * 25.0)
                peer.token_clock = now
                if peer.tokens < 1.0:
                    await ws.close(code=1008, message=b"Message rate exceeded")
                    break
                peer.tokens -= 1.0
                try:
                    pose = clean_pose(json.loads(message.data))
                except (json.JSONDecodeError, ValueError) as exc:
                    await ws.close(code=1008, message=str(exc).encode()[:120])
                    break
                seq = pose["seq"]
                if peer.last_seq is not None and seq <= peer.last_seq:
                    continue
                peer.last_seq = seq
                pose["id"] = peer.id
                for other_id, other in room.peers.items():
                    if other_id != peer.id:
                        other.latest[peer.id] = pose
            elif message.type == WSMsgType.BINARY:
                await ws.close(code=1008, message=b"Unsupported message")
                break
            elif message.type in (WSMsgType.ERROR, WSMsgType.CLOSED, WSMsgType.CLOSING):
                break
    finally:
        if peer is not None and room_name is not None:
            room = state.rooms.get(room_name)
            if room and room.peers.get(peer.id) is peer:
                del room.peers[peer.id]
                for other in room.peers.values():
                    other.latest.pop(peer.id, None)
                    push_event(other, {"type": "left", "id": peer.id})
                if not room.peers:
                    del state.rooms[room_name]
                LOG.info("Player left room %s", room_name)
            if peer.writer is not None:
                peer.writer.cancel()
                try:
                    await peer.writer
                except asyncio.CancelledError:
                    pass
        state.sessions -= 1
        if not ws.closed:
            await ws.close()
    return ws


def make_app(access_code=None):
    if access_code is None:
        access_code = os.environ.get("RELAY_ACCESS_CODE", "").strip()
    if not access_code:
        raise RuntimeError("Set RELAY_ACCESS_CODE in Render Environment before starting")
    app = web.Application()
    app[STATE_KEY] = RelayState(access_code=access_code)
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_get("/relay", relay)
    app.router.add_get("/diagnostic", diagnostic)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    port = int(os.environ.get("PORT", "10000"))
    web.run_app(make_app(), host="0.0.0.0", port=port, access_log=None)
    

"""Standalone Grand-V protocol 1 WebSocket relay for Render.

Independent implementation of the publicly documented Grand-V relay protocol.
Only relays player poses; GTA client must implement display of other players.
"""
import asyncio
import hmac
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from aiohttp import WSMsgType, web

LOG = logging.getLogger("grandv-relay")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(message)s")
VERSION = "2.0.0-official-protocol1"
MAX_CONNECTIONS = 64
POSE_HZ = 20


def ident(s: Any, max_len: int, dots: bool = False) -> bool:
    if not isinstance(s, str) or not 0 < len(s.encode("utf-8")) <= max_len:
        return False
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if dots:
        allowed += "."
    return all(char in allowed for char in s)


def parse_hello(obj: Any, secret: str) -> str | None:
    if not isinstance(obj, dict) or obj.get("type") not in ("check", "join") or type(obj.get("protocol")) is not int or obj["protocol"] != 1:
        return "Protocol mismatch"
    key = obj.get("key")
    if not isinstance(key, str) or not hmac.compare_digest(key.strip().encode("utf-8"), secret.encode("utf-8")):
        return "Join key rejected"
    if not ident(obj.get("room"), 32):
        return "Invalid room"
    if not ident(obj.get("build"), 96, dots=True) or obj.get("map") not in ("gta5", "env_test"):
        return "Invalid build/map"
    return None


def parse_pose(value: Any) -> dict:
    if not isinstance(value, dict) or value.get("type") != "pose":
        raise ValueError("Unsupported message")
    seq = value.get("seq")
    model = value.get("model")
    if type(seq) is not int or not 0 <= seq <= 2147483647:
        raise ValueError("Invalid sequence")
    if type(model) is not int or not 0 <= model <= 4294967295:
        raise ValueError("Invalid model")
    for name, max_abs in (("position", 20000), ("rotation", 360)):
        a = value.get(name)
        if not isinstance(a, list) or len(a) != 3:
            raise ValueError(f"Invalid {name}")
        import math
        if any(type(n) not in (int, float) or not math.isfinite(n) or abs(n) > max_abs for n in a):
            raise ValueError(f"Invalid {name}")
    return {"type": "pose", "seq": seq, "model": model, "position": value["position"], "rotation": value["rotation"]}


@dataclass
class Member:
    ws: web.WebSocketResponse
    id: str
    outgoing: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=32))
    poses_received: int = 0
    poses_forwarded: int = 0
    last_pose_at: float = 0.0


@dataclass
class Room:
    build: str
    map: str
    members: dict[str, Member] = field(default_factory=dict)


class Relay:
    def __init__(self, code: str):
        self.code = code
        self.rooms: dict[str, Room] = {}
        self.active_connections = 0
        self.lock = asyncio.Lock()
        self.stats = {"checks": 0, "joins": 0, "poses_received": 0, "poses_forwarded": 0}

    def public_stats(self) -> dict:
        return {
            "status": "ok", "version": VERSION, "protocol": 1,
            "rooms": len(self.rooms),
            "players": sum(len(r.members) for r in self.rooms.values()),
            "metrics": dict(self.stats),
            "room_details": [
                {"name": name, "players": len(room.members), "map": room.map,
                 "members": [{"id": m.id, "poses_received": m.poses_received,
                              "poses_forwarded": m.poses_forwarded,
                              "seconds_since_last_pose": round(time.monotonic() - m.last_pose_at, 1) if m.last_pose_at else None}
                             for m in room.members.values()]}
                for name, room in self.rooms.items()
            ]
        }

    @staticmethod
    def enqueue(member: Member, message: dict) -> bool:
        try:
            member.outgoing.put_nowait(message)
            return True
        except asyncio.QueueFull:
            LOG.warning("Slow receiver; disconnecting id=%s", member.id)
            asyncio.create_task(member.ws.close(code=1013, message=b"Receiver too slow"))
            return False

    async def sender(self, member: Member):
        while not member.ws.closed:
            msg = await member.outgoing.get()
            await member.ws.send_json(msg)

    async def connect(self, request: web.Request) -> web.StreamResponse:
        async with self.lock:
            if self.active_connections >= MAX_CONNECTIONS:
                return web.Response(text="Server full", status=503)
            self.active_connections += 1
        ws = web.WebSocketResponse(heartbeat=20, receive_timeout=60, max_msg_size=16384, autoping=True)
        room_name = None
        member = None
        sender_task = None
        try:
            await ws.prepare(request)
            try:
                inbound = await asyncio.wait_for(ws.receive(), timeout=5)
            except asyncio.TimeoutError:
                await ws.close(code=1008, message=b"Invalid or incomplete request")
                return ws
            if inbound.type != WSMsgType.TEXT:
                await ws.close(code=1008, message=b"Invalid or incomplete request")
                return ws
            try:
                hello = json.loads(inbound.data)
            except (ValueError, TypeError):
                await ws.close(code=1008, message=b"Invalid or incomplete request")
                return ws
            reason = parse_hello(hello, self.code)
            if reason:
                LOG.info("Handshake rejected: %s", reason)
                await ws.close(code=1008, message=reason.encode())
                return ws

            room_name, kind = hello["room"], hello["type"]
            async with self.lock:
                r = self.rooms.get(room_name)
                mismatch = bool(r and (r.build != hello["build"] or r.map != hello["map"]))
                full = bool(r and len(r.members) >= 2)
                count = len(r.members) if r else 0
                if not mismatch and not full and kind == "join":
                    if r is None:
                        r = Room(build=hello["build"], map=hello["map"])
                        self.rooms[room_name] = r
                    member = Member(ws=ws, id=secrets.token_hex(8))
                    existing = list(r.members)
                    r.members[member.id] = member
                    # First message must be welcome. All sends happen in one sender task.
                    self.enqueue(member, {"type": "welcome", "protocol": 1, "id": member.id, "peers": existing, "hz": POSE_HZ})
                    for other in r.members.values():
                        if other is not member:
                            self.enqueue(other, {"type": "joined", "id": member.id})
                    self.stats["joins"] += 1
                elif kind == "check" and not mismatch and not full:
                    self.stats["checks"] += 1

            if mismatch or full:
                reason = "Room build/map mismatch" if mismatch else "Room full"
                await ws.close(code=1008, message=reason.encode())
                return ws
            if kind == "check":
                await ws.send_json({"type": "checked", "players": count, "capacity": 2})
                await ws.close(code=1000, message=b"Check complete")
                return ws

            LOG.info("Joined room=%s id=%s", room_name, member.id)
            sender_task = asyncio.create_task(self.sender(member))
            last_seq = -1
            tokens = 30.0
            last_tick = time.monotonic()
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    if msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE, WSMsgType.CLOSED):
                        break
                    continue
                now = time.monotonic()
                tokens = min(30.0, tokens + (now-last_tick)*25)
                last_tick = now
                if tokens < 1:
                    await ws.close(code=1008, message=b"Message rate exceeded")
                    break
                tokens -= 1
                try:
                    pose = parse_pose(json.loads(msg.data))
                except (ValueError, TypeError) as e:
                    await ws.close(code=1008, message=str(e).encode()[:110])
                    break
                if pose["seq"] <= last_seq:
                    continue
                last_seq = pose["seq"]
                pose["id"] = member.id
                member.poses_received += 1
                member.last_pose_at = now
                self.stats["poses_received"] += 1
                async with self.lock:
                    room = self.rooms.get(room_name)
                    if room:
                        for m in room.members.values():
                            if m is not member and self.enqueue(m, pose):
                                member.poses_forwarded += 1
                                self.stats["poses_forwarded"] += 1
            return ws
        finally:
            if sender_task:
                sender_task.cancel()
                try:
                    await sender_task
                except asyncio.CancelledError:
                    pass
            async with self.lock:
                self.active_connections -= 1
                if member is not None and room_name in self.rooms:
                    room = self.rooms[room_name]
                    room.members.pop(member.id, None)
                    for other in room.members.values():
                        self.enqueue(other, {"type": "left", "id": member.id})
                    if not room.members:
                        del self.rooms[room_name]
                    LOG.info("Left room=%s id=%s", room_name, member.id)


def create_app(code: str | None = None) -> web.Application:
    if code is None:
        code = os.getenv("RELAY_ACCESS_CODE", "")
    if not code:
        raise RuntimeError("Set RELAY_ACCESS_CODE in Render > Environment")
    relay = Relay(code)
    app = web.Application()
    app["relay"] = relay

    async def home(request):
        return web.Response(text="Grand-V compatible relay online. Use WebSocket /relay. See /health and /stats.\n", content_type="text/plain")
    async def health(request):
        return web.json_response({"status":"ok", "version":VERSION, "protocol":1})
    async def stats(request):
        return web.json_response(relay.public_stats(), headers={"Cache-Control":"no-store"})
    app.router.add_get("/", home)
    app.router.add_get("/health", health)
    app.router.add_get("/stats", stats)
    app.router.add_get("/relay", relay.connect)
    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    web.run_app(create_app(), host="0.0.0.0", port=port, print=lambda msg: LOG.info(msg))

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
from typing import Optional

from aiohttp import WSMsgType, web

LOG = logging.getLogger("grandv.relay")
ROOM_NAME = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
BUILD_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,96}$")
MAX_SESSIONS = 64
MAX_PEERS_PER_ROOM = 2
MAX_MESSAGE_SIZE = 4096
RELAY_VERSION = "2.0-pose-inspection-and-v2-extension"

# This page is embedded: missing diagnostic.html can no longer cause HTTP 500.
DIAGNOSTIC_HTML = '<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Grand-V | Relay test</title><style>body{font:16px system-ui;background:#111827;color:#eee;max-width:620px;margin:35px auto;padding:16px}label,input,button{display:block;width:100%;box-sizing:border-box;margin:12px 0}input,button{padding:12px;border-radius:8px;border:1px solid #888;font-size:16px}input{background:#202938;color:#fff}button{background:#8ac35d;color:#111;border:0}pre{background:#202938;padding:18px;border-radius:8px;white-space:pre-wrap;overflow-wrap:anywhere}code{color:#a3daff}</style></head><body>\n<h2>Проверка Grand-V Relay на Render</h2><p>Проверяем WebSocket <code id="url"></code> и правильность кода. Код не сохраняется и не включается в URL.</p><label>Код RELAY_ACCESS_CODE из Render</label><input type="password" id="key" placeholder="Введи код"><label>Тестовая комната</label><input id="room" value="GrandVTest"><button id="go">Проверить подключение</button><pre id="result">Нажми кнопку проверки.</pre>\n<script>\nconst $=id=>document.getElementById(id), url=(location.protocol===\'https:\'?\'wss://\':\'ws://\')+location.host+\'/relay\';$(\'url\').textContent=url;\n$(\'go\').onclick=()=>{const key=$(\'key\').value,room=$(\'room\').value.trim();if(!key||!/^[a-zA-Z0-9_-]{1,32}$/.test(room)){$(\'result\').textContent=\'Укажи код и корректное название комнаты\';return;}\n$(\'go\').disabled=true; let ws,done=false,success=false;const result=msg=>{$(\'result\').textContent=msg},finish=msg=>{if(done)return;done=true;clearTimeout(timeout);$(\'go\').disabled=false;result(msg)};\nresult(\'Открываю WebSocket…\');const timeout=setTimeout(()=>{finish(\'ТАЙМ-АУТ: нет ответа в течение 12 сек. Проверь сеть/VPN/Render Logs.\');try{ws.close()}catch(e){}},12000);\ntry{ws=new WebSocket(url);ws.onopen=()=>{result(\'WebSocket открыт, проверяю код…\');ws.send(JSON.stringify({type:\'check\',protocol:1,key,room,build:\'grandvdiagnostic\',map:\'gta5\'}))};ws.onmessage=e=>{let data;try{data=JSON.parse(e.data)}catch(x){return finish(\'Не-JSON ответ сервера\')};if(data.type===\'checked\'){success=true;finish(\'УСПЕХ! WebSocket работает, код принят. Игроков: \'+data.players+\'/\'+data.capacity+\'\\nАдрес для GTA: \'+url)}else finish(\'Неожиданный ответ: \'+e.data)};ws.onerror=()=>result(\'Ошибка WebSocket, жду подробности закрытия…\');ws.onclose=e=>{if(!success)finish(\'ОТКАЗ. Код \'+e.code+\'; причина: \'+(e.reason||\'не указана\')+\'\\n1008 обычно означает ошибку кода или параметров. 1006 — вероятный сетевой сбой.\')}}catch(e){finish(\'Ошибка WebSocket: \'+e.message)}};\n</script></body></html>\n'


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
    pose_received: int = 0
    last_pose_at: float = 0.0
    poses_forwarded: int = 0


@dataclass
class Room:
    build: str
    map_name: str
    peers: dict = field(default_factory=dict)
    last_poses: dict = field(default_factory=dict)


@dataclass
class RelayState:
    access_code: str
    rooms: dict = field(default_factory=dict)
    sessions: int = 0
    v2_rooms: dict = field(default_factory=dict)
    received_total: int = 0
    forwarded_total: int = 0


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
        "version": RELAY_VERSION, "diagnostic": "/diagnostic",
        "inspect": "/inspect", "monitor": "/monitor", "websocket": "/relay", "v2": "/relay/v2", "rooms": len(state.rooms),
        "players": sum(len(room.peers) for room in state.rooms.values()),
        "max_players_per_room": MAX_PEERS_PER_ROOM,
        "poses_received": state.received_total, "poses_forwarded": state.forwarded_total,
        "v2_rooms": len(state.v2_rooms),
    })


async def diagnostic(request):
    return web.Response(
        text=DIAGNOSTIC_HTML, content_type="text/html",
        headers={"Cache-Control": "no-store"},
    )


MONITOR_HTML = r'''<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Grand-V — проверка синхронизации</title><style>body{max-width:650px;margin:32px auto;padding:16px;background:#101722;color:#e8f1fa;font:16px system-ui}input,button{width:100%;box-sizing:border-box;padding:12px;margin:8px 0 16px;border-radius:8px;border:1px solid #40516a;background:#253147;color:white;font-size:16px}button{background:#84c15a;color:#101722;font-weight:bold}pre{background:#1f2a38;padding:18px;border-radius:10px;white-space:pre-wrap;overflow-wrap:anywhere}code{color:#a6d9f5}</style></head><body><h2>Grand-V: проверка передачи движений</h2><p>Войди в GTA с двух устройств в одну комнату. Введи сюда тот же <code>RELAY_ACCESS_CODE</code> и название комнаты. Нажми «Проверить» несколько раз, пока оба двигаются. Этот ключ отправляется только твоему серверу, в POST-теле запроса.</p><label>Код доступа</label><input id="key" type="password" autocomplete="off" placeholder="Код из Render"><label>Название комнаты</label><input id="room" value="DanilaServer"><button id="go">Проверить движения</button><pre id="out">Готово к проверке.</pre><p><b>Как читать результат:</b> <code>players = 2</code> означает двух подключённых игроков. <code>poses_sent_by_client</code> должен расти, когда игрок двигается. Если он 0 — клиент вообще не посылает позиции. Если растёт, но другой игрок невидим — нужно исправлять браузерный клиент, а не только сервер.</p><script>document.getElementById('go').onclick=async()=>{const out=document.getElementById('out');out.textContent='Проверка...';try{const r=await fetch('/inspect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key:document.getElementById('key').value,room:document.getElementById('room').value})});const j=await r.json();out.textContent=(r.ok?'ОТВЕТ СЕРВЕРА:\n':'ОШИБКА '+r.status+':\n')+JSON.stringify(j,null,2)}catch(e){out.textContent='Ошибка: '+e.message}}</script></body></html>'''


async def monitor(request):
    return web.Response(text=MONITOR_HTML, content_type="text/html", headers={"Cache-Control": "no-store"})


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
        # Backfill the last known pose to a joining peer. The stock protocol
        # remains unchanged: just ordinary pose packets after welcome.
        for old_id in existing_ids:
            cached = room.last_poses.get(old_id)
            if cached is not None:
                peer.latest[old_id] = cached
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
                peer.pose_received += 1
                peer.last_pose_at = now
                state.received_total += 1
                pose["id"] = peer.id
                room.last_poses[peer.id] = pose
                for other_id, other in room.peers.items():
                    if other_id != peer.id:
                        other.latest[peer.id] = pose
                        peer.poses_forwarded += 1
                        state.forwarded_total += 1
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
                room.last_poses.pop(peer.id, None)
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


async def inspect(request):
    """Authenticated, read-only statistics. No key is logged or returned."""
    state = request.app[STATE_KEY]
    try:
        req = await request.json()
    except (ValueError, TypeError):
        return web.json_response({"error": "Invalid JSON"}, status=400)
    if (not isinstance(req, dict) or not isinstance(req.get("key"), str)
            or not hmac.compare_digest(req["key"].strip(), state.access_code)):
        return web.json_response({"error": "Wrong key"}, status=403)
    if not isinstance(req.get("room"), str) or not ROOM_NAME.fullmatch(req["room"]):
        return web.json_response({"error": "Invalid room"}, status=400)
    room = state.rooms.get(req["room"])
    now = time.monotonic()
    return web.json_response({
        "room": req["room"], "active": room is not None,
        "players": len(room.peers) if room else 0,
        "peers": [
            {"id": p.id[:6] + "…", "poses_sent_by_client": p.pose_received,
             "relayed_to_other": p.poses_forwarded,
             "last_pose_seconds_ago": round(now-p.last_pose_at, 1) if p.last_pose_at else None}
            for p in room.peers.values()
        ] if room else [],
        "hint": "If players=2 but poses_sent_by_client=0, the game did not send movement to this relay. If poses increase but models do not appear, inspect client rendering/remote actor spawning."
    }, headers={"Cache-Control": "no-store"})


@dataclass
class WorldRoom:
    build: str
    map_name: str
    peers: dict = field(default_factory=dict)
    players: dict = field(default_factory=dict)
    vehicles: dict = field(default_factory=dict)
    objects: dict = field(default_factory=dict)


def is_clean_id(s):
    return isinstance(s, str) and bool(re.fullmatch(r"[A-Za-z0-9_-]{1,40}", s))


def finite_number(v, bound):
    return type(v) in (int, float) and math.isfinite(v) and abs(v) <= bound


def validated_v2_packet(v):
    """Protocol 2 is an opt-in extension. Original gta.zip.wf does not use it."""
    if not isinstance(v, dict):
        raise ValueError("Invalid packet")
    typ = v.get("type")
    if typ == "player_state":
        if (not valid_vector(v.get("position"), 20000) or
            not valid_vector(v.get("rotation"), 360) or
            not is_uint(v.get("seq"), 2147483647) or
            not is_uint(v.get("model"), 4294967295) or
            not isinstance(v.get("animation"), str) or
            len(v["animation"]) > 48 or
            not re.fullmatch(r"[A-Za-z0-9_/-]*", v["animation"]) or
            not finite_number(v.get("speed"), 1000) or
            not (v.get("vehicle") is None or is_clean_id(v.get("vehicle")))):
            raise ValueError("Invalid player state")
        return {k: v[k] for k in ("type", "position", "rotation", "seq", "model", "animation", "speed", "vehicle")}
    if typ == "vehicle_state":
        if (not is_clean_id(v.get("entity")) or not is_uint(v.get("seq"), 2147483647) or
            not is_uint(v.get("model"), 4294967295) or
            not valid_vector(v.get("position"), 20000) or
            not valid_vector(v.get("rotation"), 360) or
            not valid_vector(v.get("velocity"), 5000)):
            raise ValueError("Invalid vehicle state")
        return {k: v[k] for k in ("type", "entity", "seq", "model", "position", "rotation", "velocity")}
    if typ == "object_spawn":
        if (not is_clean_id(v.get("entity")) or not is_uint(v.get("model"), 4294967295) or
            not valid_vector(v.get("position"), 20000) or not valid_vector(v.get("rotation"), 360)):
            raise ValueError("Invalid object")
        return {k: v[k] for k in ("type", "entity", "model", "position", "rotation")}
    if typ in ("object_remove", "vehicle_remove"):
        if not is_clean_id(v.get("entity")):
            raise ValueError("Invalid entity")
        return {"type": typ, "entity": v["entity"]}
    raise ValueError("Unsupported v2 packet")


async def relay_v2(request):
    """Extended transport for a modified client; never pretends to modify GTA itself."""
    state = request.app[STATE_KEY]
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=8192, compress=False)
    await ws.prepare(request)
    peer_id = None
    room_name = None
    room = None
    try:
        try:
            first = await asyncio.wait_for(ws.receive(), timeout=5)
            if first.type != WSMsgType.TEXT:
                raise ValueError("Missing join")
            hello = json.loads(first.data)
        except (ValueError, json.JSONDecodeError, asyncio.TimeoutError):
            await ws.close(code=1008, message=b"Invalid join")
            return ws
        if (not isinstance(hello, dict) or hello.get("type") != "join"
                or hello.get("protocol") != 2):
            await ws.close(code=1008, message=b"Protocol 2 required")
            return ws
        checked = dict(hello, protocol=1)
        error = validated_hello(checked, state.access_code)
        if error:
            await ws.close(code=1008, message=error.encode())
            return ws
        room_name = hello["room"]
        room = state.v2_rooms.get(room_name)
        if room and (room.build != hello["build"] or room.map_name != hello["map"]):
            await ws.close(code=1008, message=b"Room build/map mismatch")
            return ws
        if room and len(room.peers) >= 8:
            await ws.close(code=1008, message=b"Room full")
            return ws
        if room is None:
            room = WorldRoom(hello["build"], hello["map"])
            state.v2_rooms[room_name] = room
        peer_id = secrets.token_hex(8)
        # Avoid circular references and duplicating connections inside snapshots.
        await ws.send_json({"type": "world_snapshot", "protocol": 2,
            "id": peer_id, "peers": list(room.peers),
            "players": room.players, "vehicles": room.vehicles, "objects": room.objects})
        room.peers[peer_id] = ws
        for other_id, other in list(room.peers.items()):
            if other_id != peer_id:
                await other.send_json({"type": "player_join", "id": peer_id})
        LOG.info("V2 joined %s (%s/8)", room_name, len(room.peers))
        tokens = 40.0
        last = time.monotonic()
        last_seq = -1
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                if msg.type == WSMsgType.BINARY:
                    await ws.close(code=1008, message=b"Binary unsupported")
                break
            now = time.monotonic()
            tokens = min(40, tokens + (now-last)*30)
            last = now
            if tokens < 1:
                await ws.close(code=1008, message=b"Too many updates")
                break
            tokens -= 1
            try:
                p = validated_v2_packet(json.loads(msg.data))
            except (ValueError, json.JSONDecodeError) as e:
                await ws.close(code=1008, message=str(e).encode()[:100])
                break
            typ = p["type"]
            if typ == "player_state":
                if p["seq"] <= last_seq:
                    continue
                last_seq = p["seq"]
                # Entity IDs are opaque room-local names, not GTA engine handles.
                # A modified client must instantiate and animate them.
                room.players[peer_id] = dict(p, id=peer_id)
            elif typ == "vehicle_state":
                entity = p["entity"]
                current = room.vehicles.get(entity)
                if current is None and len(room.vehicles) >= 32:
                    await ws.close(code=1008, message=b"Vehicle cap")
                    break
                if current is not None and current["owner"] != peer_id:
                    await ws.close(code=1008, message=b"Vehicle owned by other player")
                    break
                if current is not None and p["seq"] <= current["seq"]:
                    continue
                room.vehicles[entity] = dict(p, owner=peer_id)
            elif typ == "object_spawn":
                entity = p["entity"]
                old = room.objects.get(entity)
                if old is None and len(room.objects) >= 128:
                    await ws.close(code=1008, message=b"Object cap")
                    break
                if old is not None and old["owner"] != peer_id:
                    await ws.close(code=1008, message=b"Object owned by other player")
                    break
                room.objects[entity] = dict(p, owner=peer_id)
            else:
                entity = p["entity"]
                collection = room.objects if typ == "object_remove" else room.vehicles
                if entity not in collection or collection[entity]["owner"] != peer_id:
                    await ws.close(code=1008, message=b"Not entity owner")
                    break
                del collection[entity]
            outbound = dict(p, id=peer_id)
            for other_id, other in list(room.peers.items()):
                if other_id != peer_id and not other.closed:
                    try:
                        await asyncio.wait_for(other.send_json(outbound), timeout=2)
                    except (asyncio.TimeoutError, ConnectionError, RuntimeError):
                        LOG.info("V2 outgoing peer stalled")
    finally:
        if peer_id is not None and room_name is not None and room is not None:
            room.peers.pop(peer_id, None)
            room.players.pop(peer_id, None)
            owned_vehicles = [k for k,v in room.vehicles.items() if v["owner"] == peer_id]
            owned_objects = [k for k,v in room.objects.items() if v["owner"] == peer_id]
            for k in owned_vehicles: del room.vehicles[k]
            for k in owned_objects: del room.objects[k]
            for other in list(room.peers.values()):
                if not other.closed:
                    try:
                        await other.send_json({"type": "player_left", "id": peer_id,
                            "vehicles_removed": owned_vehicles, "objects_removed": owned_objects})
                    except (ConnectionError, RuntimeError):
                        pass
            if not room.peers and state.v2_rooms.get(room_name) is room:
                del state.v2_rooms[room_name]
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
    app.router.add_post("/inspect", inspect)
    app.router.add_get("/monitor", monitor)
    app.router.add_get("/relay/v2", relay_v2)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    port = int(os.environ.get("PORT", "10000"))
    web.run_app(make_app(), host="0.0.0.0", port=port, access_log=None)

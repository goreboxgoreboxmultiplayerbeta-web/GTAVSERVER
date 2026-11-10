import asyncio
import unittest

from aiohttp import ClientSession, WSMsgType, web
from server import make_app


def hello(room="testroom", key="test-secret", build="build1", map="gta5", kind="join"):
    return {"type":kind,"protocol":1,"key":key,"room":room,"build":build,"map":map}


class RelayTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        app = make_app("test-secret")
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        self.port = self.site._server.sockets[0].getsockname()[1]
        self.session = ClientSession()
        self.sockets = []

    async def asyncTearDown(self):
        for ws in self.sockets:
            if not ws.closed:
                await ws.close()
        await self.session.close()
        await self.runner.cleanup()

    async def connect(self, packet):
        ws = await self.session.ws_connect(f"http://127.0.0.1:{self.port}/relay")
        self.sockets.append(ws)
        await ws.send_json(packet)
        return ws

    async def packet(self, ws):
        msg = await asyncio.wait_for(ws.receive(), timeout=2)
        self.assertEqual(msg.type, WSMsgType.TEXT, f"unexpected ws message: {msg}")
        return msg.json()

    async def test_health(self):
        async with self.session.get(f"http://127.0.0.1:{self.port}/health") as resp:
            self.assertEqual(resp.status, 200)
            result = await resp.json()
            self.assertEqual(result["protocol"], 1)
            self.assertEqual(result["rooms"], 0)

    async def test_check_room_and_wrong_key(self):
        ws = await self.connect(hello(kind="check"))
        self.assertEqual(await self.packet(ws), {"type":"checked", "players":0,"capacity":2})
        bad = await self.connect(hello(key="wrong"))
        msg = await asyncio.wait_for(bad.receive(), 2)
        self.assertEqual(msg.type, WSMsgType.CLOSE)
        self.assertEqual(bad.close_code, 1008)

    async def test_join_pose_leave(self):
        one = await self.connect(hello())
        welcome1 = await self.packet(one)
        self.assertEqual(welcome1["type"], "welcome")
        self.assertEqual(welcome1["peers"], [])
        two = await self.connect(hello())
        welcome2 = await self.packet(two)
        self.assertEqual(welcome2["peers"], [welcome1["id"]])
        self.assertEqual(await self.packet(one), {"type":"joined","id":welcome2["id"]})
        await one.send_json({"type":"pose","seq":1,"model":42,"position":[1.0,2,3],"rotation":[0,90,0]})
        pose = await self.packet(two)
        self.assertEqual(pose["type"], "pose")
        self.assertEqual(pose["id"], welcome1["id"])
        self.assertEqual(pose["model"], 42)
        await one.close()
        self.assertEqual(await self.packet(two), {"type":"left","id":welcome1["id"]})

    async def test_full_and_mismatch(self):
        a = await self.connect(hello())
        await self.packet(a)
        b = await self.connect(hello())
        await self.packet(b)
        await self.packet(a)
        c = await self.connect(hello())
        msg = await asyncio.wait_for(c.receive(), 2)
        self.assertEqual(msg.type, WSMsgType.CLOSE)
        self.assertEqual(c.close_code, 1008)
        wrong = await self.connect(hello(build="other"))
        msg = await asyncio.wait_for(wrong.receive(), 2)
        self.assertEqual(msg.type, WSMsgType.CLOSE)
        self.assertEqual(wrong.close_code, 1008)


if __name__ == '__main__':
    unittest.main()

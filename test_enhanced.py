import asyncio
import unittest

from aiohttp import ClientSession, WSMsgType, web
from server import make_app, RELAY_VERSION

KEY = 'test-key'


def hello(room='DanilaServer', protocol=1, build='game-1'):
    return dict(type='join', protocol=protocol, room=room, key=KEY, build=build, map='gta5')


class TestRelay(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runner = web.AppRunner(make_app(KEY), access_log=None)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await self.site.start()
        self.url = f'http://127.0.0.1:{self.site._server.sockets[0].getsockname()[1]}'
        self.session = ClientSession()
        self.sockets = []

    async def asyncTearDown(self):
        for ws in self.sockets:
            if not ws.closed:
                await ws.close()
        await self.session.close()
        await self.runner.cleanup()

    async def connect(self, path='/relay', packet=None):
        ws = await self.session.ws_connect(self.url + path)
        self.sockets.append(ws)
        await ws.send_json(packet or hello())
        return ws

    async def message(self, ws):
        item = await asyncio.wait_for(ws.receive(), 2.5)
        self.assertEqual(item.type, WSMsgType.TEXT, f'Non-text reply: {item}')
        return item.json()

    async def test_health_and_monitor(self):
        async with self.session.get(self.url + '/health') as r:
            data = await r.json()
            self.assertEqual(data['version'], RELAY_VERSION)
            self.assertEqual(data['poses_received'], 0)
        for path in ('/diagnostic', '/monitor'):
            async with self.session.get(self.url + path) as r:
                self.assertEqual(r.status, 200)
                self.assertIn('Grand-V', await r.text())

    async def test_compat_pose_and_monitor_and_backfill(self):
        a = await self.connect()
        a0 = await self.message(a)
        self.assertEqual(a0['type'], 'welcome')
        await a.send_json(dict(type='pose', seq=1, model=42, position=[5, 7, 9], rotation=[0, 90, 0]))
        await asyncio.sleep(.08)
        b = await self.connect()
        b0 = await self.message(b)
        self.assertEqual(b0['type'], 'welcome')
        self.assertEqual(b0['peers'], [a0['id']])
        oldpose = await self.message(b)
        self.assertEqual(oldpose['type'], 'pose')
        self.assertEqual(oldpose['position'], [5,7,9])
        self.assertEqual(oldpose['id'], a0['id'])
        self.assertEqual((await self.message(a))['type'], 'joined')
        await b.send_json(dict(type='pose', seq=1, model=13, position=[1, 2, 3], rotation=[0, 0, 0]))
        other = await self.message(a)
        self.assertEqual(other['id'], b0['id'])
        async with self.session.post(self.url + '/inspect', json={'key': KEY,'room':'DanilaServer'}) as r:
            j = await r.json()
            self.assertEqual(j['players'], 2)
            self.assertEqual(sorted(p['poses_sent_by_client'] for p in j['peers']), [1,1])
            self.assertGreaterEqual(sum(p['relayed_to_other'] for p in j['peers']), 1)
        await a.close()
        msg = await self.message(b)
        self.assertEqual(msg['type'], 'left')
        async with self.session.post(self.url + '/inspect', json={'key':'WRONG','room':'DanilaServer'}) as r:
            self.assertEqual(r.status, 403)

    async def test_check_and_reject(self):
        p=hello()
        p['type']='check'
        a=await self.connect(packet=p)
        self.assertEqual((await self.message(a))['type'], 'checked')
        p=hello()
        p['key']='NO'
        b=await self.connect(packet=p)
        await asyncio.wait_for(b.receive(), 2)
        self.assertEqual(b.close_code, 1008)

    async def test_v2_states_and_snapshot(self):
        a = await self.connect('/relay/v2', hello(protocol=2))
        a0 = await self.message(a)
        self.assertEqual(a0['type'], 'world_snapshot')
        self.assertEqual(a0['players'], {})
        player = {'type':'player_state','seq':1,'model':22,'position':[0,1,2],
                  'rotation':[0,90,0],'animation':'run','speed':3.5,'vehicle':None}
        await a.send_json(player)
        car = {'type':'vehicle_state','entity':'carABC','seq':1,'model':101,
               'position':[3,4,5],'rotation':[0,90,0],'velocity':[1,0,0]}
        await a.send_json(car)
        obj = {'type':'object_spawn','entity':'box_1','model':103,
               'position':[6,7,8],'rotation':[0,0,0]}
        await a.send_json(obj)
        await asyncio.sleep(.06)
        b = await self.connect('/relay/v2', hello(protocol=2))
        b0 = await self.message(b)
        self.assertEqual(b0['players'][a0['id']]['animation'], 'run')
        self.assertIn('carABC', b0['vehicles'])
        self.assertIn('box_1', b0['objects'])
        self.assertEqual((await self.message(a))['type'],'player_join')
        await a.send_json(dict(car, seq=2, position=[12,4,5]))
        msg=await self.message(b)
        self.assertEqual(msg['type'], 'vehicle_state')
        self.assertEqual(msg['position'][0], 12)
        await a.send_json({'type':'vehicle_remove','entity':'carABC'})
        self.assertEqual((await self.message(b))['type'],'vehicle_remove')
        await a.close()
        m=await self.message(b)
        self.assertEqual(m['type'], 'player_left')
        self.assertIn('box_1', m['objects_removed'])

    async def test_v2_ownership(self):
        a = await self.connect('/relay/v2', hello(protocol=2))
        await self.message(a)
        b = await self.connect('/relay/v2', hello(protocol=2))
        await self.message(b)
        await self.message(a)
        await a.send_json({'type':'object_spawn','entity':'mine','model':12,
                           'position':[0,0,0],'rotation':[0,0,0]})
        await self.message(b)
        await b.send_json({'type':'object_remove','entity':'mine'})
        await asyncio.wait_for(b.receive(), 2)
        self.assertEqual(b.close_code, 1008)


if __name__ == '__main__':
    unittest.main()

"""Local server protocol regression tests. Run with `python test_sync.py`."""
from fastapi.testclient import TestClient
from main import app, rooms
from time import sleep

def check():
    rooms.clear()
    with TestClient(app) as c:
        h=c.get('/health').json()
        assert h['ok'] and h['version']=='1.5'
        r=c.post('/api/rooms/ABC123').json()
        code=r['room']; token=r['host_token']
        with c.websocket_connect(f'/ws/{code}?role=host&token={token}') as host:
            snap=host.receive_json(); assert snap['state']['seq']==0
            with c.websocket_connect(f'/ws/{code}?role=guest') as guest:
                assert guest.receive_json()['state']['seq']==0
                host.send_json({'type':'command','action':'track','src':'demo://midnight-room','title':'Demo','commandId':1})
                a=host.receive_json(); b=guest.receive_json()
                assert a['state']['seq']==b['state']['seq']==1
                assert b['state']['src']=='demo://midnight-room'
                host.send_json({'type':'command','action':'play','position':3.2,'commandId':2})
                a=host.receive_json(); b=guest.receive_json()
                assert b['state']['playing'] and a['commandId']==2 and b['state']['seq']==2
                sleep(.08)
                guest.send_json({'type':'get_state'})
                snapshot=guest.receive_json()
                assert snapshot['state']['playing'] and snapshot['state']['seq']==2
                guest.send_json({'type':'command','action':'pause','position':3.2})
                assert guest.receive_json()['type']=='error'
                host.send_json({'type':'command','action':'seek','position':26.0,'commandId':3})
                a=host.receive_json(); b=guest.receive_json()
                assert b['state']['position']==26 and b['state']['playing'] and b['state']['seq']==3
                host.send_json({'type':'command','action':'pause','position':26.1,'commandId':4})
                a=host.receive_json(); b=guest.receive_json()
                assert b['state']['playing'] is False and b['state']['seq']==4
                with c.websocket_connect(f'/ws/{code}?role=guest') as late:
                    state=late.receive_json()
                    assert state['state']['seq']==4 and state['state']['position']==26.1
    print('PASS: 2 clients / track / play / seek / pause / late join / guest permission / snapshot')
if __name__=='__main__':check()

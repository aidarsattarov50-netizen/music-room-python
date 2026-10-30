"""Mocked catalog tests. Run with `python test_search.py` (requires FastAPI + httpx)."""
import asyncio
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
import main

async def fake_search(url, params=None, **kw):
    if url == main.YOUTUBE_SEARCH:
        assert params['videoEmbeddable'] == 'true'
        assert params['type'] == 'video'
        assert params['key'] == 'fake-secret-key'
        return {'items': [
            {'id': {'videoId': 'M4ZoCHID9GI'},'snippet':{'title':'The Weeknd - Call Out My Name','channelTitle':'The Weeknd','thumbnails':{'medium':{'url':'https://i.ytimg.com/vi/M4ZoCHID9GI/mqdefault.jpg'}}}},
            {'id': {'videoId': 'BAD'},'snippet':{'title':'Bad'}},
        ]}
    if url == main.APPLE:
        return {'results':[{'trackId':11,'trackViewUrl':'https://music.apple.com/song','trackName':'Song','artistName':'Artist','previewUrl':'https://audio-ssl.itunes.apple.com/preview.m4a'}]}
    return {'data':[]}

def check():
    with TestClient(main.app) as c:
        assert c.get('/health').json()['version']=='1.7'
        with patch.object(main, 'YOUTUBE_API_KEY', ''):
            r=c.get('/api/music/search',params={'q':'The Weeknd','source':'youtube'})
            assert r.status_code==503
        with patch.object(main,'YOUTUBE_API_KEY','fake-secret-key'), patch.object(main, 'get_provider_json', new=AsyncMock(side_effect=fake_search)):
            r=c.get('/api/music/search',params={'q':'Call Out My Name','source':'youtube'})
            assert r.status_code==200,r.text
            track=r.json()['results'][0]
            assert track['youtubeId']=='M4ZoCHID9GI' and track['stream']==''
            assert track['download']=='' and len(r.json()['results'])==1
            assert 'fake-secret-key' not in r.text
            r=c.get('/api/music/search',params={'q':'Call Out','source':'catalog'})
            assert r.status_code==200 and r.json()['results'][0]['preview'].startswith('https://')
            assert c.get('/api/music/tracks/random/download').status_code==404
    print('PASS: YouTube search & missing key & Apple preview & no download endpoint')
if __name__=='__main__': check()

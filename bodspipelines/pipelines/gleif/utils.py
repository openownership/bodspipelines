import requests

def gleif_download_links(url):
    out = {}
    with requests.get(url) as r:
        r.raise_for_status()
        data = r.json()
        out['lei'] = data['data']['lei2']['full_file']['xml']['url']
        out['rr'] = data['data']['rr']['full_file']['xml']['url']
        out['repex'] = data['data']['repex']['full_file']['xml']['url']
    return out

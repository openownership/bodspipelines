from bodspipelines.infrastructure.utils import download_delayed

def get_source(r, name):
    data = r.json()
    if name == 'lei':
        url = data['data']['lei2']['full_file']['xml']['url']
    elif name == 'rr':
        url = data['data']['rr']['full_file']['xml']['url']
    elif name == 'repex':
        url = data['data']['repex']['full_file']['xml']['url']
    print(f"Using: {url}")
    return url

def gleif_download_link(url):
    return download_delayed(url, get_source)

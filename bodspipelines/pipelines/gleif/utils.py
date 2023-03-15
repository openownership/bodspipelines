from bodspipelines.infrastructure.utils import download_delayed

def get_source(r, name):
    data = r.json()
    if name == 'lei':
        return data['data']['lei2']['full_file']['xml']['url']
    elif name == 'rr':
        return data['data']['rr']['full_file']['xml']['url']
    elif name == 'repex'
        return data['data']['repex']['full_file']['xml']['url']
    return None

def gleif_download_link(url):
    return download_delayed(url, get_source)

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from functools import partial


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def download_delayed(url, func):
    def download(url, func, param):
        with requests_retry_session().get(url) as r:
            r.raise_for_status()
            out = func(r, param)
        return out
    return partial(download, url, func)

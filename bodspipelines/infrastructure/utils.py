import datetime
import dateutil
import pytz
import string
import random
import hashlib
import uuid
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from functools import partial

def random_string(length):
    """Generate random string of specified length"""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def format_date(d):
    """Format date in ISO 8601"""
    return dateutil.parser.isoparse(d).strftime("%Y-%m-%d")


def current_date_iso():
    """Generate current date in ISO 8601"""
    return datetime.datetime.now(pytz.timezone('Europe/London')).strftime("%Y-%m-%d")


def generate_statement_id(name, role, version=None):
    """Generate statement ID deterministically"""
    if version:
        seed = '-'.join([name, role, version])
    else:
        seed = '-'.join([name, role])
    m = hashlib.md5()
    m.update(seed.encode('utf-8'))
    return str(uuid.UUID(m.hexdigest()))


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    """Requests session with automatic retries""" 
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


def download(url):
    """Download url"""
    with requests_retry_session().get(url) as r:
        r.raise_for_status()
        return r


def download_delayed(url, func):
    """Return partial download function"""
    def download(url, func, param):
        with requests_retry_session().get(url) as r:
            r.raise_for_status()
            out = func(r, param)
        return out
    return partial(download, url, func)

# Identify type of BODS data
def identify_bods(item):
    if item['statementType'] == 'entityStatement':
        return 'entity'
    elif item['statementType'] == 'personStatement':
        return 'person'
    elif item['statementType'] == 'ownershipOrControlStatement':
        return 'ownership'

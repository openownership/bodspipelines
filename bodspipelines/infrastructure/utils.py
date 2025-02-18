import datetime
import dateutil.parser
import pytz
import string
import random
import hashlib
import uuid
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from functools import partial
from copy import deepcopy

def broken_data(fields, data):
    for field in fields
        if not field in data:
            return True
        elif not data[field]:
            return True
    return False

def first_n(d, n):
    out = {}
    count = 0
    for k in d:
        out[k] = d[k]
        count += 1
        if count >= 5: break
    return out

def random_string(length):
    """Generate random string of specified length"""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def format_date(d):
    """Format date in ISO 8601"""
    #print("Date:", d)
    return dateutil.parser.isoparse(d).strftime("%Y-%m-%d")

def build_date(date):
    if "T" in date:
        date = date.split("T")[0]
    if date and "/" in date:
        comp = date.split("/")
        comp.reverse()
        return "-".join(comp)
    elif date and "-" in date:
        return date
    else:
        return None

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

def identify_bods(item):
    """Identify type of BODS data"""
    return item['recordType']
    #if item['recordType'] == 'entityStatement':
    #    return 'entity'
    #elif item['recordType'] == 'personStatement':
    #    return 'person'
    #elif item['recordType'] == 'ownershipOrControlStatement':
    #    return 'ownership'

async def load_last_run(storage, name=None):
    """Load data about last pipeline run"""
    runs = []
    async for run in storage.stream_items("runs"):
        runs.append(run)
    if name:
        runs = [run for run in runs if run['stage_name'] == name]
    return sorted(runs, key=lambda x: float(x["end_timestamp"]))[-1]

async def save_run(storage, data):
    """Save data about last pipeline run"""
    await storage.add_item(data, "runs")

def map_unspecified(statement):
    #print(statement)
    if "recordType" in statement and statement["recordType"] == "relationship":
        if isinstance(statement["recordDetails"]["subject"], dict):
            statement = deepcopy(statement)
            statement["recordDetails"]["subject_unspecified"] = statement["recordDetails"]["subject"]
            del statement["recordDetails"]["subject"]
        if isinstance(statement["recordDetails"]["interestedParty"], dict):
            statement = deepcopy(statement)
            statement["recordDetails"]["interestedParty_unspecified"] = statement["recordDetails"]["interestedParty"]
            del statement["recordDetails"]["interestedParty"]
    return statement

def unmap_unspecified(statement):
    if "recordType" in statement and statement["recordType"] == "relationship":
        if "subject_unspecified" in statement["recordDetails"]:
            statement["recordDetails"]["subject"] = statement["recordDetails"]["subject_unspecified"]
            del statement["recordDetails"]["subject_unspecified"]
        if "interestedParty_unspecified" in statement["recordDetails"]:
            statement["recordDetails"]["interestedParty"] = statement["recordDetails"]["interestedParty_unspecified"]
            del statement["recordDetails"]["interestedParty_unspecified"]

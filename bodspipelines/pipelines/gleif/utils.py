from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from bodspipelines.infrastructure.utils import download_delayed, download

def source_metadata(r):
    """Get metadata from request"""
    data = r.json()
    return data['data']


def get_source(r, name):
    """Extract source url from metadata"""
    data = source_metadata(r)
    url = data['full_file']['xml']['url']
    print(f"Using: {url}")
    return url

def step_date(date, time, period_name, count):
    """Step date forward based on delta file type"""
    if period_name == 'IntraDay':
        d = datetime.strptime(f"{date} {time}", "%Y%m%d %H%M")
        delta = relativedelta(hours=-8*count)
        return (d + delta).strftime("%Y%m%d"), (d + delta).strftime("%H%M")
    else:
        d = datetime.strptime(date, "%Y%m%d")
        if period_name == "LastMonth":
            delta = relativedelta(months=-1)
        elif period_name == "LastWeek":
            delta = relativedelta(days=-7)
        elif period_name == "LastDay":
            delta = relativedelta(days=-1)
        return (d + delta).strftime("%Y%m%d"), time


def delta_days(date1, date2):
    """Days between two dates"""
    delta = datetime.strptime(date2, "%Y%m%d") - datetime.strptime(date1, "%Y%m%d")
    return delta.days


def step_period(data, base, period_name, count):
    """Step through count periods"""
    for _ in range(count):
        url = data['delta_files'][period_name]['xml']['url']
        date, time, *_ = url.split('/')[-1].split('-')
        date, time = step_date(date, time, period_name, 1)
        request = download(f"{base}/{date}-{time}")
        data = source_metadata(request)
        yield url, data


def get_sources_int(data, base, last_update):
    """Get urls for sources (internal)"""
    current_date = data['publish_date']
    print(current_date, last_update)
    delta = relativedelta(datetime.strptime(current_date, "%Y-%m-%d %H:%M:%S"),
                          datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S"))
    print(current_date, delta)
    periods = {'months': 'LastMonth', 'days': 'LastDay', 'hours': 'IntraDay'}
    done = 0
    if getattr(delta, 'months') > 0:
        for url, data in step_period(data, base, 'LastMonth', 1):
            yield url
        done = -1
    for period in periods:
        if done < 0:
            if period == 'days':
                if getattr(delta, period) > 0:
                    for url, data in step_period(data, base, 'IntraDay', getattr(delta, period)*3):
                        yield url
            elif period == 'hours':
                if getattr(delta, period) > 0:
                    for url, data in step_period(data, base, 'IntraDay', getattr(delta, period)//8):
                        yield url
            else:
                if getattr(delta, period) - done > 0:
                    date1, time, *_ = url.split('/')[-1].split('-')
                    date2, time = step_date(date1, time, 'LastMonth', getattr(delta, period) - done)
                    month_days = delta_days(date1, date2)
                    for url, data in step_period(data, base, 'IntraDay', month_days*3):
                        yield url
        else:
            if period == 'days':
                if getattr(delta, period) > 6:
                    for url, data in step_period(data, base, 'LastWeek', getattr(delta, period)//7):
                        yield url
                    extra_days = getattr(delta, period)%7
                else:
                    extra_days = getattr(delta, period)
                if extra_days > 0:
                    for url, data in step_period(data, base, 'LastDay', extra_days):
                        yield url
            elif period == 'hours':
                if getattr(delta, period) > 0:
                    for url, data in step_period(data, base, 'IntraDay', getattr(delta, period)//8):
                        yield url
                else:
                    if getattr(delta, period) > 0:
                        for url, data in step_period(data, base, periods[period], getattr(delta, period)):
                            yield url

def get_sources(url, last_update):
    """Get urls for sources"""
    base = url.rsplit('/', 1)[0]
    request = download(url)
    data = source_metadata(request)
    yield from get_sources_int(data, base, last_update)

def get_source_by_date(url, data_date, delta_type=None):
    """Get source data url for specified date"""
    target_date = datetime.strptime(data_date, "%Y-%m-%d")
    if delta_type == "month":
        target_date = target_date + timedelta(days=31)
    elif delta_type == "week":
        target_date = target_date + timedelta(days=7)
    elif delta_type == "day":
        target_date = target_date + timedelta(days=1)
    page = 1
    while True:
        page_url = f"https://goldencopy.gleif.org/api/v2/golden-copies/publishes?page={page}&per_page=10"
        response = download(page_url)
        data = source_metadata(response)
        end_date = datetime.strptime(data[0]["publish_date"].split()[0], "%Y-%m-%d")
        start_date = datetime.strptime(data[9]["publish_date"].split()[0], "%Y-%m-%d")
        if target_date < start_date:
            delta = start_date - target_date
            print(delta.days)
            page = page + max(1, int(delta.days/3))
        elif target_date > end_date:
            delta = target_date - end_date
            print(delta.days)
            page = page - max(1, int(delta.days/3))
        else:
            for item in data:
                item_date = datetime.strptime(item["publish_date"].split()[0], "%Y-%m-%d")
                if item_date == target_date:
                    #print(json.dumps(item, indent=4))
                    if "/lei" in url:
                        data_type = "lei2"
                    elif "/rr" in url:
                        data_type = "rr"
                    elif "/repex" in url:
                        data_type = "repex"
                    if delta_type:
                        if delta_type == "month":
                            return item[data_type]["delta_files"]["LastMonth"]["xml"]["url"]
                        elif delta_type == "week":
                            return item[data_type]["delta_files"]["LastWeek"]["xml"]["url"]
                        elif delta_type == "day":
                            return item[data_type]["delta_files"]["LastDay"]["xml"]["url"]
                    else:
                        return item[data_type]["full_file"]["xml"]["url"]

def gleif_download_link(url):
    """Returns callable to download data"""
    return download_delayed(url, get_source)

class GLEIFData:
    def __init__(self, url=None, data_date=None):
        """Initialise with url"""
        self.url = url
        self.data_date = data_date

    def sources(self, last_update=False, delta_type=None):
        """Yield data sources"""
        if last_update:
            print("Updating ...")
            if delta_type:
                yield get_source_by_date(url, data_date, delta_type=delta_type)
            else:
                yield from get_sources(self.url, last_update)
        else:
            if self.data_date:
                yield get_source_by_date(self.url, self.data_date)
            else:
                yield gleif_download_link(self.url)

# Identify type of GLEIF data
def identify_gleif(item):
    if 'Entity' in item:
        return 'lei'
    elif 'Relationship' in item:
        return 'rr'
    elif 'ExceptionCategory' in item:
        return 'repex'

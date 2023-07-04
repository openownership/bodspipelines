import requests
from requests.adapters import HTTPAdapter, Retry

def authenticate(auth_url, client_id, client_secret):
    """Authenticate and return token"""
    r = requests.post(auth_url, json={"username": client_id, "password": client_secret})
    return r.json()['token']

def create_session(url):
    """Create requests session"""
    session = requests.Session()
    retries = Retry(total=5,
                backoff_factor=0.1,
                status_forcelist=[ 500, 502, 503, 504 ])
    if url.startswith('https'):
        session.mount('https://', HTTPAdapter(max_retries=retries))
    else:
        session.mount('http://', HTTPAdapter(max_retries=retries))
    return session

class APIData:
    """API data definition class"""

    def __init__(self, display, api_urls, headers=None, params=None, paging_names=None,
                 pagesize=None, data_element=None, auth_url=None, username=None, password=None):
        """Initial setup"""
        self.display = display			# Name to display in logging
        self.api_urls = api_urls		# List of API URLs to query
        self.auth_url = auth_url                # Authentication URL (or None)
        self.username = username                # Username (or None)
        self.password = password                # Password (or None)
        self.data_element = data_element        # JSON element where data is located
        self.streaming = True
        if headers:
            self.headers = headers
        else:
            self.headers = {}
        if pagesize:
            self.pagesize = pagesize
        else:
            self.pagesize = 100
        if params:
            self.params = params
        else:
            self.params = {}
        if paging_names:
            self.paging_names = paging_names
        else:
            self.paging_names = {"pagesize": "pagesize", "page": "page"}
        self.session = create_session(self.api_urls[0])

    def build_params(self, params):
        """Build list of parameters into URI string"""
        param_list = []
        for p in params:
            if isinstance(params[p], list):
                param_list.append(f"{p}{params[p][0]}{params[p][1]}")
            else:
                param_list.append(f"{p}={params[p]}")
        return "&".join(param_list)

    def query(self, api_url, params, token=None):
        """Query API, return response and status code"""
        if token:
            auth_headers={'Authorization': f'Bearer {token}'}
        else:
            auth_headers={}
        headers = self.headers | auth_headers
        print("Headers:", headers, "Params:", params)
        param_str = self.build_params(params)
        print(param_str)
        response = self.session.get(f"{api_url}?{param_str}", headers=headers, timeout=15)
        print(response.url)
        if response.status_code == 200:
            return response.json(), response.status_code
        else:
            print("Error code:", response.status_code, response.text)
            return None, response.status_code

    def data_stream(self):
        """Stream JSON items from API"""
        if self.username:
            token = authenticate(self.auth_url, self.username, self.password)
        else:
            token = None
        for api_url in self.api_urls:
            last_page = False
            page = 1
            while not last_page:
                print("Downloading page: ", page, self.streaming)
                done = False
                while not done:
                    data, status_code = self.query(api_url, self.params | {self.paging_names["pagesize"]: self.pagesize,
                                                          self.paging_names["page"]: page}, token)
                    if status_code == 401:
                        token = authenticate(self.auth_url, self.username, self.password)
                    else:
                        done = True
                if data:
                    if self.data_element:
                        for item in data[self.data_element]:
                            yield item
                    else:
                        if isinstance(data, dict):
                            yield data
                        else:
                            for item in data:
                                yield item
                    page += 1
                else:
                    last_page = True
                if not self.streaming: break

    def process(self):
        """Iterate over items from API"""
        for item in self.data_stream():
            yield item

import time
from pathlib import Path
import requests
from progress.bar import Bar
import json
import zipfile

class BulkData:
    """Bulk data definition class"""

    def __init__(self, display=None, data=None, size=None, directory=None):
        """Initial setup"""
        self.display = display
        self.data = data
        self.size = size
        self.directory = directory

    def data_dir(self, path) -> Path:
        """Return subdirectory path for data"""
        return path / self.directory

    def manifest_file(self, path) -> Path:
        """Return manifest file path"""
        return self.data_dir(path) / "manifest.json"

    def create_manifest(self, path, name):
        """Create manifest file"""
        manifest_file = self.manifest_file(path)
        manifest = []
        with open(manifest_file, "w") as outfile:
            for data in self.data.sources():
                if callable(data):
                    url = data(name)
                else:
                    url = data
                manifest.append({"url": url, "timestamp": time.time()})
            json.dump(manifest, outfile)

    def read_manifest(self, path):
        """Read manifest file if exists"""
        manifest_file = self.manifest_file(path)
        if manifest_file.exists():
            with open(manifest_file, 'r') as openfile:
                try:
                    return json.load(openfile)
                except json.decoder.JSONDecodeError:
                    return False
        else:
            return None

    def source_data(self, name, last_update=None, delta_type=False):
        """Yield urls for source"""
        for data in self.data.sources(last_update=last_update, delta_type=delta_type):
            if callable(data):
                url = data(name)
            else:
                url = data
            yield url

    def check_manifest(self, path, name, updates=False):
        """Check manifest file exists and up-to-date"""
        #manifest_file = self.manifest_file(path)
        manifest = self.read_manifest(path)
        if updates and manifest:
            d, t, *_ = manifest['url'].split('/')[-1].split('-')
            last_update = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        else:
            last_update = False
        if last_update:
            if updates in ("month", "week", "day"):
                # Special case for testing (usually True/False)
                delta_type = updates
            else:
                delta_type = None
            yield from self.source_data(name, last_update=last_update, delta_type=delta_type)
        else:
            if manifest: #manifest_file.exists():
                #with open(manifest_file, 'r') as openfile:
                #    try:
                #        manifest = json.load(openfile)
                #    except json.decoder.JSONDecodeError:
                #        return False
                for data in self.data.sources():
                    if callable(data):
                        url = data(name)
                    else:
                        url = data
                    match = [m for m in manifest if m["url"] == url]
                    if not match or abs(m["timestamp"] - time.time()) > 24*60*60:
                        yield url
            else:
                yield from self.source_data(name, last_update=last_update)

    def download_large(self, directory, name, url):
        """Download file to specified directory"""
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            if 'content-disposition' in r.headers:
                local_filename = r.headers['content-disposition'].split("filename=")[-1].strip('"')
            else:
                local_filename = url.rsplit('/')[-1]
            if 'content-length' in r.headers:
                size = r.headers['content-length']
            else:
                size = self.size
            if directory: local_filename = directory / local_filename
            with open(local_filename, 'wb') as f:
                for chunk in Bar(f"Downloading {self.display}", max=size).iter(r.iter_content(chunk_size=8192)):
                    f.write(chunk)
        return local_filename

    def unzip_data(self, filename, directory):
        """Unzip specified file to directory"""
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            for fn in zip_ref.namelist():
                zip_ref.extract(fn, path=directory)
                yield fn

    def delete_old_data_all(self, directory):
        """Delete all data files"""
        for file in directory.glob('*'):
            print(f"Deleting {file.name} ...")
            file.unlink()

    def delete_old_data(self, directory, url):
        """Delete filename for specified url"""
        fn = url.rsplit('/', 1)[-1]
        for file in directory.glob('*'):
            print(file.name, fn)
            if file.name == fn:
                print(f"Deleting {file.name} ...")
                file.unlink()

    def delete_unused_data(self, directory, files):
        """Delete files not in list"""
        for file in directory.glob('*'):
            if not file.name in files:
                print(f"Deleting {file.name} ...")
                file.unlink()

    def delete_zip_data(self, directory, url):
        """Delete filename for specified url"""
        fn = url.rsplit('/', 1)[-1]
        for file in directory.glob('*'):
            if file.name == fn:
                print(f"Deleting {file.name} ...")
                file.unlink()

    def download_data(self, directory, name):
        """Download data files"""
        for data in self.data.sources():
            if callable(data):
                url = data(name)
            else:
                url = data
            zip = self.download_large(directory, name, url)
            for fn in self.unzip_data(zip, directory):
                yield fn

    def download_extract_data(self, path, name):
        """Download and extract data"""
        #directory = self.data_dir(path)
        #directory.mkdir(exist_ok=True)
        self.delete_old_data(directory)
        #zip = self.download_large(directory, name)
        #self.unzip_data(zip, directory)
        for fn in self.download_data(directory, name):
            yield fn

    def download_extract_data(self, directory, name, url):
        """Download and unzip data files"""
        self.delete_old_data(directory, url)
        zip = self.download_large(directory, name, url)
        for fn in self.unzip_data(zip, directory):
            self.delete_zip_data(directory, url)
            yield fn

    def prepare(self, path, name, updates=False) -> Path:
        """Prepare data for use"""
        print("In prepare:")
        directory = self.data_dir(path)
        directory.mkdir(exist_ok=True)
        files = []
        if list(directory.glob("*.xml")) and not list(directory.glob("*golden-copy.xml")):
            for f in directory.glob("*.xml"):
                fn = f.name
                files.append(fn)
                yield directory / fn
        else:
            for url in self.check_manifest(path, name, updates=updates):
                for fn in self.download_extract_data(directory, name, url):
                    files.append(fn)
                    yield directory / fn
        print("Files:", files)
        self.create_manifest(path, name)

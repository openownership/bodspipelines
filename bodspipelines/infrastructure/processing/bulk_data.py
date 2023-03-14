import time
from pathlib import Path
import requests
from progress.bar import Bar
import json
import zipfile

class BulkData:
    """Bulk data definition class"""

    def __init__(self, display=None, url=None, size=None, directory=None):
        """Initial setup"""
        self.display = display
        self.url = url
        self.size = size
        self.directory = directory

    def data_dir(self, path) -> Path:
        """Return subdirectory path for data"""
        return path / self.directory

    def manifest_file(self, path) -> Path:
        """Return manifest file path"""
        return self.data_dir(path) / "manifest.json"

    def create_manifest(self, path):
        """Create manifest file"""
        manifest_file = self.manifest_file(path)
        with open(manifest_file, "w") as outfile:
            json.dump({"url": self.url, "timestamp": time.time()}, outfile)

    def check_manifest(self, path):
        """Check manifest file exists and up-to-date"""
        manifest_file = self.manifest_file(path)
        if manifest_file.exists():
            with open(manifest_file, 'r') as openfile:
                try:
                    manifest = json.load(openfile)
                except json.decoder.JSONDecodeError:
                    return False
            if manifest["url"] == self.url and abs(manifest["timestamp"] - time.time()) < 24*60*60:
                return True
            else:
                return False
        else:
            return False

    def download_large(self, directory, name):
        """Download file to specified directory"""
        if isinstance(self.url, dict):
            url = self.url[name]
        else:
            url = self.url
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
            zip_ref.extractall(directory)

    def delete_old_data(self, directory):
        for file in directory.glob('*'):
            file.unlink()

    def download_extract_data(self, path, name):
        """Download and extract data"""
        directory = self.data_dir(path)
        directory.mkdir(exist_ok=True)
        self.delete_old_data(directory)
        zip = self.download_large(directory, name)
        self.unzip_data(zip, directory)

    def prepare(self, path, name) -> Path:
        """Prepare data for use"""
        if not self.check_manifest(path):
            self.download_extract_data(path, name)
            self.create_manifest(path)
        else:
            print(f"{self.display} data up-to-date ...")
        for file in self.data_dir(path).glob('*.xml'):
            return file


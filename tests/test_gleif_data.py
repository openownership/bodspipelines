from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import random
import zipfile
import io
import pytest

from bodspipelines.infrastructure.processing.bulk_data import BulkData

from bodspipelines.pipelines.gleif.utils import gleif_download_link, GLEIFData

from unittest.mock import patch, Mock, MagicMock

class TestGLEIFData:
    """Test download of single file"""
    source = 'lei'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def stage_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / "data" / self.source
        output_dir.mkdir(parents=True)
        return output_dir

    def test_gleif_data(self, stage_dir):
        """Test downloading GLEIF data"""
        with (patch('bodspipelines.infrastructure.utils.download_delayed') as mock_download,
              patch('bodspipelines.infrastructure.processing.bulk_data.requests.get') as mock_requests):
            def download_delayed(url):
                def download(param):
                    fn = param.rsplit('/', 1)[-1]
                    print(fn)
                    return fn
                return partial(download, url)
            mock_download.side_effect = download_delayed
            def zip_file(fn):
                file = io.BytesIO()
                zf = zipfile.ZipFile(file, mode='w')
                zf.writestr(fn, '<xm></xml>')
                zf.close()
                return bytes(file.getbuffer())
            def requests_get(url, stream=True):
                fn = url.rsplit('/', 1)[-1]
                xfn = fn.split('.zip')[0]
                mock_return = Mock()
                mock_return.headers = {'content-disposition': f"filename={fn}",
                                       'content-length': 10000}
                mock_return.iter_content.return_value = iter([zip_file(xfn)])
                mock_context = Mock()
                mock_context.__enter__ = Mock(return_value=mock_return)
                mock_context.__exit__ = Mock(return_value=False)
                return mock_context
            mock_requests.side_effect = requests_get

            origin=BulkData(display="LEI-CDF v3.1",
                data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest"),
                size=41491,
                directory="lei-cdf")
            count = 0
            for fn in origin.prepare(stage_dir, self.source):
                assert fn.endswith("-gleif-goldencopy-lei2-golden-copy.xml")
                print([x for x in (stage_dir/'lei-cdf').iterdir()])
                assert (stage_dir/'lei-cdf'/fn).exists()
                count += 1
            assert count == 1

class TestGLEIFDataUpdateMonths:
    """Test download of updates after over a month"""
    source = 'lei'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def stage_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / "data2" / self.source
        output_dir.mkdir(parents=True)
        return output_dir

    def test_gleif_data(self, stage_dir):
        """Test downloading GLEIF data"""
        with (patch('bodspipelines.infrastructure.utils.download') as mock_download,
              patch('bodspipelines.infrastructure.processing.bulk_data.requests.get') as mock_requests):
            def download(url):
                url_base = "https://goldencopy.gleif.org/storage/golden-copy-files/2023/06/13/795336/"
                d = datetime.now().strftime("%Y%m%d")
                t = f'{[x for x in (0, 800, 1600) if x < int(datetime.now().strftime("%H%M"))][-1]:04d}'
                return {'data': {'publish_date': f"{d} {t}",
                                 'delta_files': {
                                 'LastMonth': {'xml': {'url': f"{url_base}{d}-{t}-gleif-goldencopy-lei2-last-month.xml.zip"}},
                                 'LastWeek': {'xml': {'url': f"{url_base}{d}-{t}-gleif-goldencopy-lei2-last-week.xml.zip"}},
                                 'LastDay': {'xml': {'url': f"{url_base}{d}-{t}-gleif-goldencopy-lei2-last-day.xml.zip"}},
                                 'IntraDay': {'xml': {'url': f"{url_base}{d}-{t}-gleif-goldencopy-lei2-intra-day.xml.zip"}},
                                                }}}
            mock_download.side_effect = download
            def zip_file(fn):
                file = io.BytesIO()
                zf = zipfile.ZipFile(file, mode='w')
                zf.writestr(fn, '<xm></xml>')
                zf.close()
                return bytes(file.getbuffer())
            def requests_get(url, stream=True):
                fn = url.rsplit('/', 1)[-1]
                xfn = fn.split('.zip')[0]
                mock_return = Mock()
                mock_return.headers = {'content-disposition': f"filename={fn}",
                                       'content-length': 10000}
                mock_return.iter_content.return_value = iter([zip_file(xfn)])
                mock_context = Mock()
                mock_context.__enter__ = Mock(return_value=mock_return)
                mock_context.__exit__ = Mock(return_value=False)
                return mock_context
            mock_requests.side_effect = requests_get
            last_update_date = (datetime.now() + relativedelta(months=-1) - timedelta(days=5)).strftime("%Y-%m-%d")
            last_update_time = f"{[x for x in (0, 8, 16) if int(datetime.now().strftime('%H00')) > x][-1]:02d}:00:00"

            origin=BulkData(display="LEI-CDF v3.1",
                data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest"),
                size=41491,
                directory="lei-cdf")
            print(f'{last_update_date} {last_update_time}')
            count = 0
            for fn in origin.prepare(stage_dir, self.source, last_update=f'{last_update_date} {last_update_time}'):
                print(fn)
                if count == 0:
                    assert fn.endswith("-gleif-goldencopy-lei2-last-month.xml")
                    first = fn
                else:
                    assert fn.endswith("-gleif-goldencopy-lei2-intra-day.xml")
                print([x for x in (stage_dir/'lei-cdf').iterdir()])
                assert (stage_dir/'lei-cdf'/fn).exists()
                count += 1
            first_date = datetime.strptime(first.split("-gleif")[0], "%Y%m%d-%H%M")
            last_date = datetime.strptime(f'{last_update_date} {last_update_time}', "%Y-%m-%d %H:%M:%S")
            delta = relativedelta(first_date, last_date)
            assert count == 1 + 3*delta.days + delta.hours/8
            assert len([x for x in (stage_dir/'lei-cdf').iterdir() if x.suffix == ".xml"]) == 1 + 3*delta.days + delta.hours/8


class TestGLEIFDataUpdateWeeks:
    """Test download of updates after less than a month"""
    source = 'lei'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def stage_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / "data3" / self.source
        output_dir.mkdir(parents=True)
        return output_dir

    def test_gleif_data(self, stage_dir):
        """Test downloading GLEIF data"""
        with (patch('bodspipelines.infrastructure.utils.download') as mock_download,
              patch('bodspipelines.infrastructure.processing.bulk_data.requests.get') as mock_requests):
            def download(url):
                url_base = "https://goldencopy.gleif.org/storage/golden-copy-files/2023/06/13/795336/"
                d = datetime.now().strftime("%Y%m%d")
                t = f'{[x for x in (0, 800, 1600) if x < int(datetime.now().strftime("%H%M"))][-1]:04d}'
                return {'data': {'publish_date': f"{d} {t}",
                                 'delta_files': {
                                 'LastMonth': {'xml': {'url': f"{url_base}{d}-{t}-gleif-goldencopy-lei2-last-month.xml.zip"}},
                                 'LastWeek': {'xml': {'url': f"{url_base}{d}-{t}-gleif-goldencopy-lei2-last-week.xml.zip"}},
                                 'LastDay': {'xml': {'url': f"{url_base}{d}-{t}-gleif-goldencopy-lei2-last-day.xml.zip"}},
                                 'IntraDay': {'xml': {'url': f"{url_base}{d}-{t}-gleif-goldencopy-lei2-intra-day.xml.zip"}},
                                                }}}
            mock_download.side_effect = download
            def zip_file(fn):
                file = io.BytesIO()
                zf = zipfile.ZipFile(file, mode='w')
                zf.writestr(fn, '<xm></xml>')
                zf.close()
                return bytes(file.getbuffer())
            def requests_get(url, stream=True):
                fn = url.rsplit('/', 1)[-1]
                xfn = fn.split('.zip')[0]
                mock_return = Mock()
                mock_return.headers = {'content-disposition': f"filename={fn}",
                                       'content-length': 10000}
                mock_return.iter_content.return_value = iter([zip_file(xfn)])
                mock_context = Mock()
                mock_context.__enter__ = Mock(return_value=mock_return)
                mock_context.__exit__ = Mock(return_value=False)
                return mock_context
            mock_requests.side_effect = requests_get
            last_update_date = (datetime.now() - timedelta(days=19)).strftime("%Y-%m-%d")
            last_update_time = f"{[x for x in (0, 8, 16) if int(datetime.now().strftime('%H00')) > x][-1]:02d}:00:00"

            origin=BulkData(display="LEI-CDF v3.1",
                data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest"),
                size=41491,
                directory="lei-cdf")
            print(f'{last_update_date} {last_update_time}')
            count = 0
            for fn in origin.prepare(stage_dir, self.source, last_update=f'{last_update_date} {last_update_time}'):
                print(fn)
                if count < 2:
                    assert fn.endswith("-gleif-goldencopy-lei2-last-week.xml")
                    if count == 0: first = fn
                #elif count < 7:
                #    assert fn.endswith("-gleif-goldencopy-lei2-last-day.xml")
                else:
                    assert fn.endswith("-gleif-goldencopy-lei2-intra-day.xml") or fn.endswith("-gleif-goldencopy-lei2-last-day.xml")
                print([x for x in (stage_dir/'lei-cdf').iterdir()])
                assert (stage_dir/'lei-cdf'/fn).exists()
                count += 1
            first_date = datetime.strptime(first.split("-gleif")[0], "%Y%m%d-%H%M")
            last_date = datetime.strptime(f'{last_update_date} {last_update_time}', "%Y-%m-%d %H:%M:%S")
            delta = relativedelta(first_date, last_date)
            print(delta)
            assert count == delta.days//7 + delta.days%7 + delta.hours/8
            assert len([x for x in (stage_dir/'lei-cdf').iterdir() if x.suffix == ".xml"]) == delta.days//7 + delta.days%7 + delta.hours/8

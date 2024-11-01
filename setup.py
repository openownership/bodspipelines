from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install

install_requires = [
    "async-kinesis",
    "boto3",
    "elasticsearch",
    "elastic_transport",
    "lxml",
    "parsel",
    "progress",
    "pytest",
    "python-dateutil",
    "requests",
    "tqdm",
    "pytz",
    "pycountry",
    "aiohttp",
    "aiofiles",
    "redis",
    "psutil",
    "loguru"
]

setup(
    name="bodspipelines",
    version="0.1",
    author="Open Ownership",
    author_email="code@opendataservices.coop",
    py_modules=['bodspipelines'],
    packages=['bodspipelines'],
    url="https://github.com/openownership/bodspipelines",
    license="MIT",
    description="Library for building pipelines to produce BODS data",
    install_requires=install_requires,
)

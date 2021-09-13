from setuptools import find_packages, setup
from os.path import abspath, dirname, join

ROOT_DIR = abspath(dirname(__file__))

with open(join(ROOT_DIR, "README.md"), encoding="utf-8") as f:
    readme = f.read()

setup(
    name="tap-amazon-sp",
    version="0.1.0",
    description="Singer ETL tap for extracting data from the Amazon Selling Partner API",
    long_description=readme,
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_amazon_sp"],
    install_requires=[
        'appnope==0.1.2',
        'astroid==2.5.2',
        'attrs==20.3.0',
        'backcall==0.2.0',
        'backoff==1.8.0',
        'certifi==2020.12.5',
        'chardet==4.0.0',
        'ciso8601==2.1.3',
        'decorator==4.4.2',
        'idna==2.10',
        'iniconfig==1.1.1',
        'ipython==7.21.0',
        'ipython-genutils==0.2.0',
        'isort==5.8.0',
        'jedi==0.18.0',
        'jsonschema==2.6.0',
        'lazy-object-proxy==1.6.0',
        'mccabe==0.6.1',
        'packaging==20.9',
        'parso==0.8.1',
        'pexpect==4.8.0',
        'pickleshare==0.7.5',
        'pluggy==0.13.1',
        'prompt-toolkit==3.0.17',
        'ptyprocess==0.7.0',
        'py==1.10.0',
        'Pygments==2.8.1',
        'pylint==2.7.4',
        'pyparsing==2.4.7',
        'pytest==6.2.3',
        'python-dateutil==2.8.1',
        'pytz==2018.4',
        'requests==2.25.1',
        'simplejson==3.11.1',
        'singer-python==5.10.0',
        'six==1.15.0',
        'toml==0.10.2',
        'traitlets==5.0.5',
        'urllib3==1.26.3',
        'wcwidth==0.2.5',
        'wrapt==1.12.1'
    ],
    entry_points="""
    [console_scripts]
    tap-amazon-sp=tap_amazon_sp:main
    """,
    packages=find_packages(exclude=["tests"]),
    package_data = {
        "schemas": ["tap_amazon_sp/schemas/*.json"]
    },
    include_package_data=True,
)

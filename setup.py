import os
from setuptools import setup, find_packages

CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))

def version():
    version = open(os.path.join(CURRENT_DIRECTORY, "VERSION")).read()
    return version + "-%s" % os.environ.get('BUILD', 'dev0')

setup(
    name="riskapi_client",
    version=version(),
    packages=find_packages(),
    zip_safe=False,
    dependency_links=[],
    package_data={},
    extras_require = {
        "msgpack": ["msgpack-python>=0.4"]
    }
)

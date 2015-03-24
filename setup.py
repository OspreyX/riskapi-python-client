import os
from setuptools import setup, find_packages

CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))

def version():
    version = open(os.path.join(CURRENT_DIRECTORY, "VERSION")).read().strip()
    return version + ".%s" % os.environ.get('BUILD', 'dev0')

setup(
    name="riskapi_client",
    version=version(),
    packages=find_packages(),
    zip_safe=False,
    dependency_links=[],
    package_data={},
    scripts=["riskapi"],
    extras_require = {
        "msgpack": ["msgpack-python>=0.4"],
    },
    test_suite = "nose.collector",
    tests_require = [
        "nose>=1.3.4",
        "voluptuous==0.8.7"
    ]
)

from __future__ import absolute_import

import codecs
from os import path
from setuptools import find_packages, setup

# get __version__ from _version.py
def read(rel_path):
    here = path.abspath(path.dirname(__file__))
    with codecs.open(path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]


ver_file = path.join(".", "version.py")
__version__ = get_version(ver_file)

this_directory = path.abspath(path.dirname(__file__))

# read the contents of README.rst
def readme():
    with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
        return f.read()


# read the contents of requirements.txt
with open(path.join(this_directory, "requirements.txt"), encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="openGemini-castor",
    version=__version__,
    author="Cloud Database Innovation Lab, Huawei Cloud Computing Technologies Co., Ltd.",
    author_email="community.ts@opengemini.org",
    url="http://opengemini.org/",
    description="A package for time series computing",
    long_description=readme(),
    long_description_content_type="text/markdown",
    keywords=[
        "time series",
        "anomaly detection",
        "prediction",
        "deep neural network",
        "time series insight",
    ],
    license="Apache-2.0",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)

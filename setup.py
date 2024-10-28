"""A setuptools based setup module.

See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from glob import glob
import pathlib
import codecs
import os.path

PACKAGE_NAME = "py_async"
here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(str(os.path.join(here, rel_path)), 'r') as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


version = get_version(f"{PACKAGE_NAME}/__init__.py")

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name="py_async",  # Required
    version=version,  # Required
    description="A Python project",  # Optional
    long_description=long_description,  # Optional
    long_description_content_type="text/markdown",  # Optional (see note above)
    url="https://github.com/uncomplete/py_async",  # Optional
    author="uncomplete",  # Optional
    author_email="uncomplete3@gmail.com",  # Optional
    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        # Pick your license as you wish
        "License :: OSI Approved :: MIT License",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate you support Python 3. These classifiers are *not*
        # checked by 'pip install'. See instead 'python_requires' below.
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    packages=find_packages(exclude=["contrib", "docs", "tests", "tests.*"]),  # Required
    python_requires=">=3.8, <4",
    install_requires=[
        "peppercorn",
        "boto3",
        "pandas",
        "requests",
        "fastparquet",
        "StrEnum",
        "ijson",
        "pyarrow",
        "s3fs",
        "fsspec",
        "aiohttp",
    ],  # Optional
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "pytest-mock",
            "pytest-xdist",
            "parameterized",
            "flake8",
            "black",
            "memory-profiler",
        ],
    },
    entry_points={
        'console_scripts': [
            'loopback = py_async.cmd:loopback',
        ],
    },
)

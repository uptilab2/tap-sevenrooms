#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-sevenrooms",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_sevenrooms"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-sevenrooms=tap_sevenrooms:main
    """,
    packages=["tap_sevenrooms"],
    package_data={
        "schemas": ["tap_sevenrooms/schemas/*.json"]
    },
    include_package_data=True,
)

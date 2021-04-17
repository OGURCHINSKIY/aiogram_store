# -*- coding: utf-8 -*-
import re
from setuptools import setup

version = re.search(
    r'^__version__\s*=\s*"(.*)"',
    open('aiogram_store/__init__.py').read(),
    re.M
).group(1)

with open("README.md", "rb") as f:
    long_description = f.read().decode("utf-8")

setup(
    name="aiogram_store",
    packages=["aiogram_store"],
    entry_points={
        "console_scripts": ['aiogram_store = aiogram_store.main:main']
    },
    version=version,
    description="Python command line example package",
    long_description=long_description,
    author="ogurchik",
    url="https://github.com/OGURCHINSKIY/aiogram_store/issues",
    project_urls={
        "Bug Tracker": "https://github.com/OGURCHINSKIY/aiogram_store/issues",
    }
)

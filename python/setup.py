from setuptools import setup, find_packages, Extension
import os
import subprocess
from glob import glob

# Project information
name = "pyfractal"
version = "3.0.0"
description = "A brief description of what your package does."
author = "Vinícius Dias"
author_email = "vvsdias@gmail.com"
license = "MIT"
url = "https://github.com/username/my-project"
classifiers = [
    "Programming Language :: Python",
    "License :: OSI Approved :: MIT License",
]
install_requires = ["networkx", "dill", "pyspark"]
scripts = []

setup(name=name, version=version, description=description, author=author, author_email=author_email, license=license,
      url=url, classifiers=classifiers, install_requires=install_requires, scripts=scripts, include_package_data=True,
      package_dir={'': 'src'})
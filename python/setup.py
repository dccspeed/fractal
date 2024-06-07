from setuptools import setup

# Project information
name = "pyfractal"
version = "3.0.0"
description = "A brief description of what your package does."
author = "Vinícius Dias"
author_email = "vvsdias@gmail.com"
license = "Apache 2.0"
url = "https://github.com/dccspeed/fractal"
classifiers = [
    "Programming Language :: Python",
    "License :: Apache 2.0",
]
install_requires = ["networkx", "dill", "", "pyspark"]
scripts = []

setup(name=name, version=version, description=description, author=author, author_email=author_email, license=license,
      url=url, classifiers=classifiers, install_requires=install_requires, scripts=scripts, include_package_data=True,
      package_dir={'': 'src'})
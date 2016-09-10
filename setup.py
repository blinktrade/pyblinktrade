#!/usr/bin/env python

from setuptools import setup

version = "0.6"

setup(
  name="pyblinktrade",
  version=version,
  packages = [
    "pyblinktrade",
  ],
  author="Rodrigo Souza",
  install_requires=[],
  author_email='r@blinktrade.com',
  url='https://github.com/blinktrade/pyblinktrade',
  license='http://www.gnu.org/copyleft/gpl.html',
  description='Blinktrade python api library'
)

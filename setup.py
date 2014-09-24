#!/usr/bin/env python

from setuptools import setup

version = "0.1"

REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()]

setup(
  name="pyblinktrade",
  version=version,
  packages = [
    "pyblinktrade",
  ],
  author="Rodrigo Souza",
  entry_points = { 'console_scripts':
    [ 
      'blockchain_info_withdrawal = pyblinktrade.scripts.blockchain_info_withdrawal:main'  
    ]
  },
  install_requires=REQUIREMENTS,
  author_email='r@blinktrade.com',
  url='https://github.com/blinktrade/pyblinktrade',
  license='http://www.gnu.org/copyleft/gpl.html',
  description='Utilities for Blinktrade plataform'
)
 


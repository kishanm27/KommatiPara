#import
from setuptools import setup, find_packages

setup(
    name='parra_pack',
    version='1.0.0',
    author='Kishan Minguel',
    description='package contains my assignment',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'pyspark',
    ],
)
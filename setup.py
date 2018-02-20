#!/usr/bin/env python
"""Setup script for mapchete plugin."""

from setuptools import setup

setup(
    name='mapchete_s3',
    version='0.1',
    description='Mapchete S3 plugin',
    author='Joachim Ungar',
    author_email='joachim.ungar@eox.at',
    url='https://github.com/ungarj/mapchete-s3',
    license='MIT',
    packages=['mapchete_s3'],
    install_requires=[
        'mapchete>=0.20',
        'rasterio[s3]>=1.0a12'
    ],
    entry_points={'mapchete.formats.drivers': ['swift=mapchete_s3']},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
)

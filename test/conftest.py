"""Fixtures for test suite."""

import boto3
import os
import pytest
import yaml


@pytest.fixture
def example_mapchete():
    """Fixture for example.mapchete."""
    path = os.path.join(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "testdata"),
        "example.mapchete")
    config = yaml.load(open(path).read())
    config.update(config_dir=os.path.dirname(path))
    return config


@pytest.fixture
def example_tile(example_mapchete):
    """Example tile for fixture."""
    zoom, row, col = (5, 15, 32)
    yield (zoom, row, col)
    _delete_tile(zoom, row, col, example_mapchete)


def _delete_tile(zoom, row, col, example_mapchete):
    client = boto3.client('s3')
    client.delete_object(
        Bucket=example_mapchete["output"]["bucket"],
        Key="_test/mapchete_s3/%s/%s/%s.tif" % (zoom, row, col))

#!/usr/bin/env Python
"""Test module."""

import numpy as np

import mapchete
from mapchete.formats import available_output_formats


def test_format_available():
    """Format can be listed."""
    assert "s3" in available_output_formats()


def test_output_data(example_mapchete, example_tile):
    """Basic functions."""
    with mapchete.open(example_mapchete) as mp:
        process_tile = mp.config.process_pyramid.tile(*example_tile)
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # read empty
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert data.mask.all()


def test_write_output_data(example_mapchete, example_tile):
    """Write and read output."""
    with mapchete.open(example_mapchete) as mp:
        process_tile = mp.config.process_pyramid.tile(*example_tile)
        # write
        mp.batch_process(tile=process_tile.id)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()


def test_overwrite(example_mapchete, example_tile):
    """Write and read output."""
    conf = dict(**example_mapchete)
    with mapchete.open(conf, mode="overwrite") as mp:
        process_tile = mp.config.process_pyramid.tile(*example_tile)
        # write
        mp.batch_process(tile=process_tile.id)
        # write again, this time with an array of ones
        mp.write(process_tile, np.ones((3, ) + process_tile.shape) * 100)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert np.where(data == 100, True, False).all()


def test_multiprocessing(example_mapchete, example_tile):
    """Write and read output."""
    with mapchete.open(example_mapchete) as mp:
        next(mp.batch_processor(multi=2))

#!/usr/bin/env Python
"""Test module."""

import numpy as np
# import numpy.ma as ma

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


# def test_write_and_overwrite(example_mapchete, example_tile):
#     """Write and read output."""
#     conf = dict(**example_mapchete)
#     example_tile_id = tuple(example_tile)
#     with mapchete.open(conf) as mp:
#         process_tile = mp.config.process_pyramid.tile(*example_tile_id)
#         # write
#         mp.batch_process(tile=process_tile.id)
#         data = mp.config.output.read(process_tile)
#         assert isinstance(data, np.ndarray)
#         assert not data[0].mask.all()
#     with mapchete.open(conf, mode="overwrite") as mp:
#         process_tile = mp.config.process_pyramid.tile(*example_tile_id)
#         # write again, this time with an array of ones
#         shape = (3, ) + process_tile.shape
#         mp.write(
#             process_tile,
#             ma.masked_array(np.ones(shape), np.zeros(shape)))
#     with mapchete.open(conf) as mp:
#         process_tile = mp.config.process_pyramid.tile(*example_tile_id)
#         # read again, this time with custom data
#         data = mp.config.output.read(process_tile)
#         assert isinstance(data, np.ndarray)
#         assert np.where(data.data == 1, True, False).all()


def test_multiprocessing(example_mapchete, example_tile):
    """Write and read output."""
    with mapchete.open(example_mapchete) as mp:
        next(mp.batch_processor(multi=2))

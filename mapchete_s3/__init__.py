"""Read from and write to S3 buckets."""

import numpy as np
import numpy.ma as ma
import os
import rasterio
import six
import warnings
import boto3

from mapchete.config import validate_values
from mapchete.formats import base
from mapchete.formats.default import gtiff
from mapchete.io.raster import RasterWindowMemoryFile, prepare_array
from mapchete.log import driver_logger
from mapchete.tile import BufferedTile


logger = driver_logger("mapchete_s3")

METADATA = {
    "driver_name": "s3",
    "data_type": "raster",
    "mode": "rw"
}

GDAL_OPTS = {
    "GDAL_DISABLE_READDIR_ON_OPEN": True,
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.ovr"
}


class OutputData(base.OutputData):
    """Driver output class."""

    METADATA = {
        "driver_name": "s3",
        "data_type": "raster",
        "mode": "rw"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.output_params = output_params
        if output_params["profile"]["driver"] != "GTiff":
            raise NotImplementedError(
                "other drivers thatn GTiff not yet supported")
        self.file_extension = ".tif"
        self.nodata = output_params["profile"].get("nodata", 0)
        self.bucket = output_params["bucket"]

    def read(self, output_tile):
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        process output : ``BufferedTile`` with appended data
        """
        if self.tiles_exist(output_tile):
            with rasterio.open(self.get_path(output_tile), "r") as src:
                logger.debug(
                    "read existing output from %s", self.get_path(output_tile))
                return src.read(masked=True)
        else:
            logger.debug((output_tile.id, "no existing output"))
            return self.empty(output_tile)

    def write(self, process_tile, data):
        """
        Write data from process tiles into GeoTIFF file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        if (
            isinstance(data, tuple) and
            len(data) == 2 and
            isinstance(data[1], dict)
        ):
            data, tags = data
        else:
            tags = {}
        data = prepare_array(
            data, masked=True, nodata=self.nodata,
            dtype=self.profile(process_tile)["dtype"])
        if data.mask.all():
            logger.debug((process_tile.id, "empty data"))
            return
        bucket = boto3.resource('s3').Bucket(self.bucket)
        for tile in self.pyramid.intersecting(process_tile):
            logger.debug((tile.id, "prepare to upload", self.get_path(tile)))
            out_tile = BufferedTile(tile, self.pixelbuffer)
            with RasterWindowMemoryFile(
                in_tile=process_tile, in_data=data,
                out_profile=self.profile(out_tile), out_tile=out_tile,
                tags=tags
            ) as memfile:
                logger.debug((
                    tile.id, "upload tile", self.get_bucket_key(tile)))
                bucket.put_object(Key=self.get_bucket_key(tile), Body=memfile)

    def tiles_exist(self, process_tile=None, output_tile=None):
        """
        Check whether output tiles of a process tile exist.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        exists : bool
        """
        if process_tile and output_tile:
            raise ValueError(
                "just one of 'process_tile' and 'output_tile' allowed")
        bucket = boto3.resource('s3').Bucket(self.bucket)

        def _any_key_exists(keys):
            for key in keys:
                for obj in bucket.objects.filter(Prefix=key):
                    if obj.key == key:
                        return True
                return False

        if process_tile and output_tile:
            raise ValueError(
                "just one of 'process_tile' and 'output_tile' allowed")
        if process_tile:
            return _any_key_exists([
                self.get_bucket_key(tile)
                for tile in self.pyramid.intersecting(process_tile)
            ])
        if output_tile:
            return _any_key_exists([self.get_bucket_key(output_tile)])

    def is_valid_with_config(self, config):
        """
        Check if output format is valid with other process parameters.

        Parameters
        ----------
        config : dictionary
            output configuration parameters

        Returns
        -------
        is_valid : bool
        """
        return all([
            validate_values(
                config, [
                    ("profile", dict),
                    ("basekey", six.string_types),
                    ("bucket", six.string_types)]),
            validate_values(
                config["profile"], [
                    ("driver", six.string_types), ("bands", int),
                    ("dtype", six.string_types)])
        ])

    def get_path(self, tile):
        """
        Determine target file path.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        path : string
        """
        return os.path.join(*["s3://", self.bucket, self.get_bucket_key(tile)])

    def get_bucket_key(self, tile):
        """
        Determine target file key in bucket.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        path : string
        """
        return os.path.join(*[
            self.output_params["basekey"], str(tile.zoom), str(tile.row),
            str(tile.col)]) + self.file_extension

    def empty(self, process_tile):
        """
        Return empty data.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        empty data : array
            empty array with data type provided in output profile
        """
        profile = self.profile(process_tile)
        return ma.masked_array(
            data=np.full(
                (profile["count"], ) + process_tile.shape, profile["nodata"],
                dtype=profile["dtype"]),
            mask=True)

    def profile(self, tile=None):
        """
        Create a metadata dictionary for rasterio.

        Parameters
        ----------
        tile : ``BufferedTile``

        Returns
        -------
        metadata : dictionary
            output profile dictionary used for rasterio.
        """
        out_profile = self.output_params["profile"]
        dst_metadata = dict(
            gtiff.GTIFF_PROFILE,
            count=out_profile["bands"],
            dtype=out_profile["dtype"],
            driver="GTiff")
        dst_metadata.pop("transform", None)
        if tile is not None:
            dst_metadata.update(
                crs=tile.crs, width=tile.width, height=tile.height,
                affine=tile.affine)
        else:
            for k in ["crs", "width", "height", "affine"]:
                dst_metadata.pop(k, None)
        if "nodata" in out_profile:
            dst_metadata.update(nodata=out_profile["nodata"])
        try:
            if "compression" in out_profile:
                warnings.warn(
                    "use 'compress' instead of 'compression'",
                    DeprecationWarning
                )
                dst_metadata.update(
                    compress=out_profile["compression"])
            else:
                dst_metadata.update(
                    compress=out_profile["compress"])
            dst_metadata.update(predictor=out_profile["predictor"])
        except KeyError:
            pass
        return dst_metadata

    def for_web(self, data):
        """
        Convert data to web output (raster only).

        Parameters
        ----------
        data : array

        Returns
        -------
        web data : array
        """
        raise NotImplementedError
        # data = prepare_array(
        #     data, masked=True, nodata=self.nodata,
        #     dtype=self.profile()["dtype"])
        # return memory_file(data, self.profile()), "image/tiff"

    def open(self, tile, process, **kwargs):
        """
        Open process output as input for other process.

        Parameters
        ----------
        tile : ``Tile``
        process : ``MapcheteProcess``
        kwargs : keyword arguments
        """
        return InputTile(tile, process, kwargs.get("resampling", None))


class InputTile(base.InputTile):
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    process : ``MapcheteProcess``
    resampling : string
        rasterio resampling method

    Attributes
    ----------
    tile : ``Tile``
    process : ``MapcheteProcess``
    resampling : string
        rasterio resampling method
    pixelbuffer : integer
    """

    def __init__(self, tile, process, resampling):
        """Initialize."""
        self.tile = tile
        self.process = process
        self.pixelbuffer = None
        self.resampling = resampling

    def read(self, indexes=None):
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        indexes : integer or list
            band number or list of band numbers

        Returns
        -------
        data : array
        """
        band_indexes = self._get_band_indexes(indexes)
        arr = self.process.get_raw_output(self.tile)
        if len(band_indexes) == 1:
            return arr[band_indexes[0] - 1]
        else:
            return ma.concatenate([
                ma.expand_dims(arr[i - 1], 0) for i in band_indexes
            ])

    def is_empty(self, indexes=None):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        # empty if tile does not intersect with file bounding box
        return not self.tile.bbox.intersects(
            self.process.config.process_area()
        )

    def _get_band_indexes(self, indexes=None):
        """Return valid band indexes."""
        if indexes:
            if isinstance(indexes, list):
                return indexes
            else:
                return [indexes]
        else:
            return range(
                1, self.process.config.output.profile(self.tile)["count"] + 1)

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, t, v, tb):
        """Clear cache on close."""
        pass

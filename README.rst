==================
Mapchete S3 plugin
==================

Let mapchete read from and write to S3 buckets.

Usage
=====

Define as output type in the mapchete configuration:

.. code-block:: yaml

    output:
        format: s3
        profile:
            driver: GTiff
            bands: 3
            dtype: uint16
        basekey: _test/mapchete_s3
        bucket: test-gtiff

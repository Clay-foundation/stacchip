import io
import os

import boto3
import numpy as np

from stacchip.processors.prechip import (
    LINZ_BANDS,
    LS_BANDS,
    NAIP_BANDS,
    S1_BANDS,
    S2_BANDS,
)

MAX_CUBES = 1000


def get_stats_keys(key, nodata):
    s3_session = boto3.resource("s3")
    obj = s3_session.Object("clay-v1-data-cubes", key)
    body = obj.get()["Body"].read()
    with io.BytesIO(body) as f:
        f.seek(0)
        data = np.load(f)["pixels"]

    data = data.astype("float64").swapaxes(0, 1)

    data = np.ma.array(data, mask=data == nodata)

    pixel_count = np.ma.count(data)
    pixel_sum = np.ma.sum(data, axis=(1, 2, 3))
    pixel_sqr = np.ma.sum(np.ma.power(data, 2), axis=(1, 2, 3))

    return pixel_count, pixel_sum, pixel_sqr


def process():
    if "STACCHIP_PLATFORM" not in os.environ:
        raise ValueError("STACCHIP_PLATFORM env var not set")

    platform = os.environ.get("STACCHIP_PLATFORM")
    if platform == "naip":
        bands = NAIP_BANDS
        nodata = 0
    elif platform == "linz":
        bands = LINZ_BANDS
        nodata = 0
    elif platform == "sentinel-2-l2a":
        bands = S2_BANDS
        nodata = 0
    elif platform in ["landsat-c2l2-sr", "landsat-c2l1"]:
        bands = LS_BANDS
        nodata = 0
    elif platform == "sentinel-1-rtc":
        bands = S1_BANDS
        nodata = -32768
    else:
        raise ValueError(f"Platform {platform} not found")

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket="clay-v1-data-cubes", Prefix=f"mode_v1_chipper_v2/{platform}"
    )

    band_count = len(bands)
    pixel_count = np.zeros(band_count)
    pixel_sum = np.zeros(band_count)
    pixel_sqr = np.zeros(band_count)

    counter = 0
    for page in page_iterator:
        keys = [dat["Key"] for dat in page["Contents"]]
        for key in keys:
            counter += 1
            print(counter, key)

            px0, px1, px2 = get_stats_keys(key, nodata)

            pixel_count = np.add(pixel_count, px0)
            pixel_sum = np.add(pixel_sum, px1)
            pixel_sqr = np.add(pixel_sqr, px2)

            # https://stackoverflow.com/questions/1174984/how-to-efficiently-calculate-a-running-standard-deviation
            mean = pixel_sum / pixel_count
            stdev = np.sqrt((pixel_sqr / pixel_count) - (mean * mean))

            print(f"Progressive stats Mean {dict(zip(bands, mean))}")
            print(f"Progressive stats Stdev {dict(zip(bands, stdev))}")

            if counter == MAX_CUBES:
                print(f"Finished after {MAX_CUBES} cubes")
                return

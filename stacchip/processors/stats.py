import io
import os
from multiprocessing import Pool

import boto3
import numpy as np

from stacchip.processors.prechip import (
    LINZ_BANDS,
    LS_BANDS,
    NAIP_BANDS,
    S1_BANDS,
    S2_BANDS,
)


def get_stats_keys(key):
    print(f"Processing {key}")
    if "sentinel-1-rtc" in key:
        nodata = -32768
    else:
        nodata = 0

    s3_session = boto3.resource("s3")
    obj = s3_session.Object("clay-v1-data-cubes", key)
    body = obj.get()["Body"].read()
    with io.BytesIO(body) as f:
        f.seek(0)
        data = np.load(f)["pixels"]

    data = data.astype("float64").swapaxes(0, 1)

    data = np.ma.array(data, mask=data == nodata)

    pixel_count = np.ma.count(data, axis=(1, 2, 3))
    pixel_sum = np.ma.sum(data, axis=(1, 2, 3))
    pixel_sqr = np.ma.sum(np.ma.power(data, 2), axis=(1, 2, 3))

    return pixel_count, pixel_sum, pixel_sqr


def process():
    if "STACCHIP_PLATFORM" not in os.environ:
        raise ValueError("STACCHIP_PLATFORM env var not set")
    pool_size = int(os.environ.get("STACCHIP_POOL_SIZE", 4))
    max_cubes = int(os.environ.get("STACCHIP_MAX_CUBES", 4))

    platform = os.environ.get("STACCHIP_PLATFORM")
    if platform == "naip":
        bands = NAIP_BANDS
    elif platform == "linz":
        bands = LINZ_BANDS
    elif platform == "sentinel-2-l2a":
        bands = S2_BANDS
    elif platform in ["landsat-c2l2-sr", "landsat-c2l1"]:
        bands = LS_BANDS
    elif platform == "sentinel-1-rtc":
        bands = S1_BANDS
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
    all_keys = []
    for page in page_iterator:
        keys = [dat["Key"] for dat in page["Contents"]]
        for key in keys:
            counter += 1
            all_keys.append(key)
            if counter == max_cubes:
                break
        if counter == max_cubes:
            break

    with Pool(pool_size) as pl:
        result = pl.map(get_stats_keys, all_keys)

    for dat in result:
        pixel_count = np.add(pixel_count, dat[0])
        pixel_sum = np.add(pixel_sum, dat[1])
        pixel_sqr = np.add(pixel_sqr, dat[2])

    # https://stackoverflow.com/questions/1174984/how-to-efficiently-calculate-a-running-standard-deviation
    mean = pixel_sum / pixel_count
    stdev = np.sqrt((pixel_sqr / pixel_count) - (mean * mean))

    print("-- Mean by band")
    for band, val in zip(bands, mean):
        print(f"{band}: {val}")

    print("-- Std by band")
    for band, val in zip(bands, stdev):
        print(f"{band}: {val}")

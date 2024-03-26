from pathlib import Path
from urllib.parse import urlparse
import json

import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import geopandas as gp
import pystac_client

from stacchip.indexer import Sentinel2Indexer

STAC_API = "https://earth-search.aws.element84.com/v1"
S2_ASSETS = [
    "blue",
    "green",
    "nir",
    "nir08",
    "red",
    "rededge1",
    "rededge2",
    "rededge3",
    "scl",
    "swir16",
    "swir22",
]
ABSOLUTE_CLOUD_COVER_FILTER = 0.75
V1_BUCKET = "clay-v1-data"

data = gp.read_file("https://clay-mgrs-samples.s3.amazonaws.com/mgrs_sample_v02.fgb")
catalog = pystac_client.Client.open(STAC_API)
s3 = boto3.resource("s3")

wd = Path("~/Desktop/clay-v1-sentinel-2-items").expanduser()
assert wd.exists()


quartals = [
    "{year}-01-01/{year}-03-31",
    "{year}-04-01/{year}-06-30",
    "{year}-07-01/{year}-09-30",
    "{year}-10-01/{year}-12-31",
]

all_scenes = []

for id, row in data.iterrows():
    print("ROW", row["name"])
    for year in ["2019", "2021", "2023"]:
        print(f"YEAR {year}")
        for quartal in quartals:
            items = catalog.search(
                collections=["sentinel-2-l2a"],
                datetime=quartal.format(year=year),
                max_items=1,
                intersects=row.geometry,
                sortby="properties.eo:cloud_cover",
                query={
                    "grid:code": {
                        "eq": f"MGRS-{row['name']}",
                    }
                },
            )
            item = items.item_collection()[0]

            if item.properties["eo:cloud_cover"] < ABSOLUTE_CLOUD_COVER_FILTER:
                all_scenes.append(item)

            for key in list(item.assets.keys()):
                if key not in S2_ASSETS:
                    del item.assets[key]
                else:
                    url = urlparse(item.assets[key].href)
                    copy_source = {"Bucket": "sentinel-cogs", "Key": url.path.lstrip("/")}
                    print(f"Copying {copy_source}")
                    new_key = (
                        f"sentinel-2-l2a/{item.id}/{Path(item.assets[key].href).name}"
                    )
                    # s3.meta.client.copy(copy_source, V1_BUCKET, new_key)
                    item.assets[key].href = f"s3://{V1_BUCKET}/{new_key}"

            # Convert Dictionary to JSON String
            data_string = json.dumps(item.to_dict())

            # Upload JSON String to an S3 Object
            s3_bucket = s3.Bucket(name=V1_BUCKET)
            s3_bucket.put_object(
                Key=f"sentinel-2-l2a/{item.id}/stac_item.json",
                Body=data_string,
            )

            indexer = Sentinel2Indexer(item)
            index = indexer.create_index()

            writer = pa.BufferOutputStream()
            pq.write_table(index, writer)
            body = bytes(writer.getvalue())

            s3_bucket.put_object(Body=body, Key=f"sentinel-2-l2a/{item.id}/chip_index.parquet")
            break

        print([dat.properties["eo:cloud_cover"] for dat in all_scenes[-4:]])
        break
    break

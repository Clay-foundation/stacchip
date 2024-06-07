import json
from pathlib import Path

import boto3
from pystac import Item

from stacchip.indexer import ChipIndexer


def load_indexer_s3(bucket: str, platform: str, item_id: str) -> ChipIndexer:
    """
    Load stacchip index table from a remote location
    """
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(name=bucket)
    content_object = s3_bucket.Object(f"{platform}/{item_id}/stac_item.json")
    file_content = content_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)
    item = Item.from_dict(json_content)

    return ChipIndexer(item)


def load_indexer_local(mountpath: Path, platform: str, item_id: str) -> ChipIndexer:
    """
    Load stacchip index table from local file
    """
    item = Item.from_file(mountpath / Path(f"{platform}/{item_id}/stac_item.json"))
    return ChipIndexer(item)

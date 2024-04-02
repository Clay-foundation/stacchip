import os

from pyarrow import dataset as ds


def merge() -> None:
    if "STACCHIP_BUCKET" not in os.environ:
        raise ValueError("STACCHIP_BUCKET env var not set")
    bucket = os.environ["STACCHIP_BUCKET"]

    if "STACCHIP_MERGE_TARGET_DIR" not in os.environ:
        raise ValueError("STACCHIP_MERGE_TARGET_DIR env var not set")
    dst = os.environ["STACCHIP_MERGE_TARGET_DIR"]

    part = ds.partitioning(field_names=["platform", "item"])
    data = ds.dataset(
        f"s3://{bucket}/index",
        format="parquet",
        partitioning=part,
    )
    ds.write_dataset(
        data,
        dst,
        format="parquet",
    )

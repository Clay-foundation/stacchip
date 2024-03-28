# stacchip
Dynamically create image chips from STAC items

## Processors

stacchip comes with processors that can be used to collect and index
imagery from multiple data sources.

### Sentinel-2

The `stacchip-sentinel-2` processor CLi command processes Sentinel-2
data. It will process MGRS tiles from a list of tiles from a layer
that can be opened by geopandas.

Each MGRS tile will be processed by the row index in the source file.

The script uses environment variables to determine all inputs:

1. The index of the MGRS tile to be processes from the source file
2. The source file for the MGRS tile sample
3. A target bucket for writing the assets, stac items, and stacchip index.

```bash
export AWS_BATCH_JOB_ARRAY_INDEX=0
export STACCHIP_MGRS_SOURCE=https://clay-mgrs-samples.s3.amazonaws.com/mgrs_sample_v02.fgb
export STACCHIP_BUCKET=clay-v1-data
```

## Batch processing

The following base image can be used for batch processing.

```dockerfile
FROM ubuntu:jammy

RUN apt update -y
RUN apt upgrade -y

RUN apt install -y python3-pip

RUN pip install https://github.com/Clay-foundation/stacchip/archive/refs/heads/main.zip
```

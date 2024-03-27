import warnings
from pathlib import Path
from typing import Tuple

import numpy as np
import pyarrow as pa
import geoarrow.pyarrow as ga
import rasterio
from pystac import Item
from rasterio.crs import CRS
from rasterio.enums import Resampling

warnings.filterwarnings(
    "ignore",
    message=(
        "The argument 'infer_datetime_format' is deprecated and will"
        " be removed in a future version. A strict version of it is now "
        "the default, see https://pandas.pydata.org/pdeps/0004-consistent"
        "-to-datetime-parsing.html. You can safely remove this argument."
    ),
)


class ChipIndexer:

    def __init__(
        self,
        item: Item,
        chip_size: int = 256,
    ) -> None:
        self.item = item
        self.chip_size = chip_size

        assert self.item.ext.has("proj")
        self.assert_units_metre()

        self.x_pixel_size = (self.item.bbox[2] - self.item.bbox[0]) / self.shape[1]
        self.y_pixel_size = (self.item.bbox[3] - self.item.bbox[1]) / self.shape[0]

    def assert_units_metre(self) -> None:
        crs = CRS(init=f"EPSG:{self.item.properties['proj:epsg']}")
        assert crs.linear_units == "metre"

    _shape = None

    @property
    def shape(self) -> list:
        """
        Get shape of hightest resolution band.
        """
        if self._shape is None:
            if "proj:shape" in self.item.properties:
                self._shape = self.item.properties["proj:shape"]
            else:
                for asset in self.item.assets.values():
                    if "proj:shape" not in asset.extra_fields:
                        continue

                    if (
                        self._shape is None
                        or self._shape[0] < asset.extra_fields["proj:shape"][0]
                    ):
                        self._shape = asset.extra_fields["proj:shape"]

            if self._shape is None:
                raise ValueError("Could not determine shape and resolution")

        return self._shape

    def get_stats(self, chip_index_x: int, chip_index_y: int) -> Tuple[float, float]:
        raise NotImplementedError()

    def get_bbox(
        self, chip_index_x: int, chip_index_y: int
    ) -> Tuple[float, float, float, float]:
        return [
            self.item.bbox[0] + chip_index_x * self.x_pixel_size,
            self.item.bbox[1] + chip_index_y * self.y_pixel_size,
            self.item.bbox[0] + (chip_index_x + self.chip_size) * self.x_pixel_size,
            self.item.bbox[1] + (chip_index_y + self.chip_size) * self.y_pixel_size,
        ]

    def create_index(self) -> None:
        index = []

        for y in range(0, self.shape[0] - (self.chip_size - 1), self.chip_size):
            if y + self.chip_size > self.shape[0]:
                continue
            for x in range(0, self.shape[1] - (self.chip_size - 1), self.chip_size):
                if x + self.chip_size > self.shape[1]:
                    continue
                cloud_cover_percentage, nodata_percentage = self.get_stats(x, y)
                xmin, ymin, xmax, ymax = self.get_bbox(x, y)
                # print(ga.as_geoarrow(f"POLYGON (({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"))

                row = [
                    f"{self.item.id}-{x}-{y}",
                    Path(self.item.get_self_href()).name,
                    self.item.datetime.date(),
                    x,
                    y,
                    cloud_cover_percentage,
                    nodata_percentage,
                    xmin,
                    ymin,
                    xmax,
                    ymax,
                    f"POLYGON (({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))",
                ]
                index.append(row)

        return pa.table(
            [
                pa.array([dat[0] for dat in index], type=pa.string()),  # chip id
                pa.array([dat[1] for dat in index], type=pa.string()),  # stac item link
                pa.array([dat[2] for dat in index], type=pa.date32()),  # image date
                pa.array([dat[3] for dat in index], type=pa.int16()),  # chip index x
                pa.array([dat[4] for dat in index], type=pa.int16()),  # chip index y
                pa.array([dat[5] for dat in index], type=pa.float32()),  # cloud cover
                pa.array([dat[6] for dat in index], type=pa.float32()),  # nodata %
                pa.array([dat[7] for dat in index], type=pa.float32()),  # bbox x min
                pa.array([dat[8] for dat in index], type=pa.float32()),  # bbox y min
                pa.array([dat[9] for dat in index], type=pa.float32()),  # bbox x max
                pa.array([dat[10] for dat in index], type=pa.float32()),  # bbox y max
                ga.as_geoarrow([dat[11] for dat in index]),  # geoarrow bbox
            ],
            names=[
                "chipid",
                "stac_item",
                "date",
                "chip_index_x",
                "chip_index_y",
                "cloud_cover_percentage",
                "nodata_percentage",
                "bbox_x_min",
                "bbox_y_min",
                "bbox_x_max",
                "bbox_y_max",
                "geometry",
            ],
        )


class NoStatsChipIndexer(ChipIndexer):
    def get_stats(self, chip_index_x: int, chip_index_y: int) -> Tuple[float, float]:
        return 0.0, 0.0


class LandsatIndexer(ChipIndexer):
    _qa = None

    @property
    def qa(self):
        if self._qa is None:
            print("Loading qa band")
            self.item.assets["qa_pixel"].href = self.item.assets[
                "qa_pixel"
            ].extra_fields["alternate"]["s3"]["href"]
            with rasterio.open(self.item.assets["qa_pixel"].href) as src:
                self._qa = src.read(1)
        return self._qa

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        qa = self.qa[y : (y + self.chip_size), x : (x + self.chip_size)]

        # Bit 1 is dilated cloud, 3 is cloud, 4 is cloud shadow.
        nodata_byte = np.array(1 << 0, dtype=qa.dtype)
        dilated_cloud_byte = np.array(1 << 1, dtype=qa.dtype)
        cloud_byte = np.array(1 << 3, dtype=qa.dtype)
        shadow_byte = np.array(1 << 4, dtype=qa.dtype)

        nodata_mask = np.bitwise_and(qa, nodata_byte)
        dilated_cloud = np.bitwise_and(qa, dilated_cloud_byte)
        cloud = np.bitwise_and(qa, cloud_byte)
        shadow = np.bitwise_and(qa, shadow_byte)

        layer_clouds = (dilated_cloud | cloud | shadow).astype(dtype="bool")

        cloud_percentage = np.sum(layer_clouds) / qa.size
        nodata_percentage = np.sum(nodata_mask) / qa.size

        return cloud_percentage, nodata_percentage


class Sentinel2Indexer(ChipIndexer):
    scl_filter = [1, 3, 8, 9, 10]
    nodata_value = 0

    _scl = None

    @property
    def scl(self):
        if self._scl is None:
            print("Loading scl band")
            with rasterio.open(self.item.assets["scl"].href) as src:
                self._scl = src.read(
                    out_shape=(1, *self.shape), resampling=Resampling.bilinear
                )[0]

        return self._scl

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        scl = self.scl[y : (y + self.chip_size), x : (x + self.chip_size)]

        cloud_percentage = int(np.isin(scl, self.scl_filter).sum()) / scl.size

        nodata_percentage = np.sum(scl == self.nodata_value) / scl.size

        return cloud_percentage, nodata_percentage

import warnings
from functools import cached_property
from math import floor
from typing import Tuple

import geoarrow.pyarrow as ga
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyproj
import rasterio
from pystac import Item
from rasterio.crs import CRS
from rasterio.enums import Resampling
from shapely.geometry import box
from shapely.ops import transform

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
        self, item: Item, chip_size: int = 256, chip_max_nodata: float = 0.5
    ) -> None:
        self.item = item
        self.chip_size = chip_size
        self.chip_max_nodata = chip_max_nodata

        assert self.item.ext.has("proj")

        self.assert_units_metre()
        self.setup_projector()

    def assert_units_metre(self) -> None:
        crs = CRS(init=f"EPSG:{self.item.properties['proj:epsg']}")
        assert crs.linear_units == "metre"

    def setup_projector(self):
        wgs84 = pyproj.CRS("EPSG:4326")
        target_crs = pyproj.CRS(f'EPSG:{self.item.properties["proj:epsg"]}')
        self._projector = pyproj.Transformer.from_crs(
            target_crs, wgs84, always_xy=True
        ).transform

    def reproject(self, geom) -> str:
        return transform(self._projector, geom)

    def _get_trsf_or_shape(self, key: str) -> Tuple:
        """
        Get shape of hightest resolution band.
        """
        data = None
        if key in self.item.properties:
            data = self.item.properties[key]
        else:
            for asset in self.item.assets.values():
                if key not in asset.extra_fields:
                    continue
                if data is None or data[0] < asset.extra_fields[key][0]:
                    data = asset.extra_fields[key]

        return data

    @cached_property
    def shape(self) -> Tuple[int, int]:
        return self._get_trsf_or_shape("proj:shape")

    @cached_property
    def transform(
        self,
    ) -> Tuple[float, float, int, float, float, int]:
        return self._get_trsf_or_shape("proj:transform")

    @property
    def x_size(self) -> int:
        return floor(self.shape[1] / self.chip_size)

    @property
    def y_size(self) -> int:
        return floor(self.shape[0] / self.chip_size)

    @property
    def size(self) -> int:
        return self.x_size * self.y_size

    @property
    def bbox(self) -> Tuple[float, float, float, float]:
        return (
            self.transform[2],
            self.transform[5] + self.transform[4] * self.shape[0],
            self.transform[2] + self.transform[0] * self.shape[1],
            self.transform[5],
        )

    def get_stats(self, chip_index_x: int, chip_index_y: int) -> Tuple[float, float]:
        raise NotImplementedError()

    def get_chip_bbox(self, x: int, y: int) -> str:
        chip_box = box(
            self.bbox[0] + x * self.transform[0] * self.chip_size,
            self.bbox[3] + y * self.transform[4] * self.chip_size,
            self.bbox[0] + (x + 1) * self.transform[0] * self.chip_size,
            self.bbox[3] + (y + 1) * self.transform[4] * self.chip_size,
        )

        return self.reproject(chip_box).wkt

    def create_index(self) -> None:
        index = {
            "chipid": np.empty(self.size, dtype="<U256"),
            "date": np.empty(self.size, dtype="datetime64[D]"),
            "chip_index_x": np.empty(self.size, dtype="uint16"),
            "chip_index_y": np.empty(self.size, dtype="uint16"),
            "cloud_cover_percentage": np.empty(self.size, dtype="float32"),
            "nodata_percentage": np.empty(self.size, dtype="float32"),
            "geometry": np.empty(self.size, dtype="object"),
        }
        counter = 0
        for y in range(0, self.y_size):
            for x in range(0, self.x_size):

                cloud_cover_percentage, nodata_percentage = self.get_stats(x, y)

                index["chipid"][counter] = f"{self.item.id}-{x}-{y}"
                index["date"][counter] = self.item.datetime.date()
                index["chip_index_x"][counter] = x
                index["chip_index_y"][counter] = y
                index["cloud_cover_percentage"][counter] = cloud_cover_percentage
                index["nodata_percentage"][counter] = nodata_percentage
                index["geometry"][counter] = self.get_chip_bbox(x, y)

                counter += 1

        index["geometry"] = ga.as_geoarrow(index["geometry"])

        table = pa.table(index)
        chips_count = table.shape[0]
        table = table.filter(pc.field("nodata_percentage") <= self.chip_max_nodata)
        print(
            f"Dropped {chips_count - table.shape[0]}/{chips_count} chips due to nodata above {self.chip_max_nodata}"
        )
        return table


class NoStatsChipIndexer(ChipIndexer):
    def get_stats(self, chip_index_x: int, chip_index_y: int) -> Tuple[float, float]:
        return 0.0, 0.0


class LandsatIndexer(ChipIndexer):

    @cached_property
    def qa(self):
        print("Loading qa band")
        self.item.assets["qa_pixel"].href = self.item.assets["qa_pixel"].extra_fields[
            "alternate"
        ]["s3"]["href"]
        with rasterio.open(self.item.assets["qa_pixel"].href) as src:
            return src.read(1)

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        qa = self.qa[
            y * self.chip_size : (y + 1) * self.chip_size,
            x * self.chip_size : (x + 1) * self.chip_size,
        ]

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

    @cached_property
    def scl(self):
        print("Loading scl band")
        with rasterio.open(self.item.assets["scl"].href) as src:
            return src.read(out_shape=(1, *self.shape), resampling=Resampling.nearest)[
                0
            ]

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        scl = self.scl[
            y * self.chip_size : (y + 1) * self.chip_size,
            x * self.chip_size : (x + 1) * self.chip_size,
        ]

        cloud_percentage = int(np.isin(scl, self.scl_filter).sum()) / scl.size

        nodata_percentage = np.sum(scl == self.nodata_value) / scl.size

        return cloud_percentage, nodata_percentage


class Sentinel1Indexer(ChipIndexer):
    nodata_value = 0

    @cached_property
    def vv(self):
        print("Loading vv band")
        with rasterio.open(self.item.assets["vv"].href) as src:
            return src.read(out_shape=(1, *self.shape), resampling=Resampling.nearest)[
                0
            ]

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        vv = self.vv[
            y * self.chip_size : (y + 1) * self.chip_size,
            x * self.chip_size : (x + 1) * self.chip_size,
        ]


        nodata_percentage = np.sum(vv == self.nodata_value) / vv.size

        return nodata_percentage
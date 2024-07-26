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
from numpy.typing import ArrayLike
from pystac import Item
from rasterio.crs import CRS
from rasterio.enums import Resampling
from shapely import GeometryType, Polygon
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
    """
    Indexer base class
    """

    def __init__(
        self,
        item: Item,
        chip_size: int = 256,
        chip_max_nodata: float = 0.5,
        shape=None,
    ) -> None:
        """
        Init ChipIndexer
        """
        self.item = item
        self.chip_size = chip_size
        self.chip_max_nodata = chip_max_nodata
        self._shape = shape

        assert self.item.ext.has("proj")

        self.assert_units_metre()
        self.setup_projector()

    def assert_units_metre(self) -> None:
        """
        Ensure input data has meters as units
        """
        assert self.crs.linear_units.lower() in ["metre", "meter"]

    @property
    def crs(self) -> CRS:
        """
        Get coordinate reference system for the assets in this index
        """
        if self.item.properties.get("proj:epsg", None):
            return CRS.from_epsg(self.item.properties["proj:epsg"])
        elif "proj:wkt2" in self.item.properties:
            return CRS.from_string(self.item.properties["proj:wkt2"])
        else:
            raise ValueError("Could not identify CRS of source files")

    def setup_projector(self):
        """
        Prepare projection function to project geometries into WGS84
        """
        wgs84 = pyproj.CRS("EPSG:4326")
        self._projector = pyproj.Transformer.from_crs(
            self.crs, wgs84, always_xy=True
        ).transform

    def reproject(self, geom) -> GeometryType:
        """
        Reproject a geometry into WGS84
        """
        return transform(self._projector, geom)

    def _get_trsf_or_shape(self, key: str) -> list:
        """
        The shape of the hightest resolution band
        """
        data = []
        if key in self.item.properties:
            data = self.item.properties[key]
        else:
            for asset in self.item.assets.values():
                if key not in asset.extra_fields:
                    continue
                if not data or data[0] < asset.extra_fields[key][0]:
                    data = asset.extra_fields[key]
        if not data:
            raise ValueError("Could not determine {key} for this STAC item")

        return data

    @cached_property
    def shape(self) -> list:
        """
        Shape of the STAC item data

        Obtains the shape of the highest resolution band from
        all the available bands.
        """
        if self._shape is not None:
            return self._shape
        else:
            return self._get_trsf_or_shape("proj:shape")

    @cached_property
    def transform(self) -> list:
        """
        The transform property from the STAC item
        """
        return self._get_trsf_or_shape("proj:transform")

    @property
    def x_size(self) -> int:
        """
        Number of tiles vailable in x direction
        """
        return floor(self.shape[1] / self.chip_size)

    @property
    def y_size(self) -> int:
        """
        Number of tiles vailable in y direction
        """
        return floor(self.shape[0] / self.chip_size)

    @property
    def size(self) -> int:
        """
        Number of tiles in this STAC item
        """
        return self.x_size * self.y_size

    @property
    def bbox(self) -> Tuple[float, float, float, float]:
        """
        Bounding box that covers all tiles

        This is different from the bounding box of the STAC item
        if the tiles don't fit into the number of pixels perfectly.
        """
        return (
            self.transform[2],
            self.transform[5] + self.transform[4] * self.shape[0],
            self.transform[2] + self.transform[0] * self.shape[1],
            self.transform[5],
        )

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        """
        A function to write for each indexer that returns nodata and
        cloud statistics for a chip
        """
        raise NotImplementedError()

    def get_chip_bbox(self, x: int, y: int) -> Polygon:
        """
        Bounding box for a chip
        """
        chip_box = box(
            self.bbox[0] + x * self.transform[0] * self.chip_size,
            self.bbox[3] + y * self.transform[4] * self.chip_size,
            self.bbox[0] + (x + 1) * self.transform[0] * self.chip_size,
            self.bbox[3] + (y + 1) * self.transform[4] * self.chip_size,
        )

        return self.reproject(chip_box)

    def create_index(self) -> pa.Table:
        """
        The index for this STAC item
        """
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
                index["geometry"][counter] = self.get_chip_bbox(x, y).wkt

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
    """
    Indexer that assumes that none of the chips have any clouds or nodata
    """

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        """
        Cloud and nodata percentage for a chip
        """
        return 0.0, 0.0


class NoDataMaskChipIndexer(ChipIndexer):
    """
    Chip indexer that takes the nodata mask as input and assumes that
    there are no clouds in the image
    """

    def __init__(
        self,
        item: Item,
        nodata_mask: ArrayLike,
        chip_size: int = 256,
        chip_max_nodata: float = 0.5,
    ) -> None:
        """
        Init NoDataMaskChipIndexer
        """
        super().__init__(item, chip_size, chip_max_nodata)
        self.nodata_mask = nodata_mask

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        """
        Cloud and nodata percentage for a chip

        Assumes there are no cloudy pixels and computes nodata from mask
        """
        nodata_percentage = np.sum(
            self.nodata_mask[
                y * self.chip_size : (y + 1) * self.chip_size,
                x * self.chip_size : (x + 1) * self.chip_size,
            ]
        ) / (self.chip_size**2)

        return 0.0, nodata_percentage


class LandsatIndexer(ChipIndexer):
    """
    Chip indexer for Landsat 8 and 9 STAC items
    """

    @cached_property
    def qa(self):
        """
        The quality band data for the STAC item
        """
        print("Loading qa band")
        self.item.assets["qa_pixel"].href = self.item.assets["qa_pixel"].extra_fields[
            "alternate"
        ]["s3"]["href"]
        with rasterio.open(self.item.assets["qa_pixel"].href) as src:
            return src.read(1)

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        """
        Cloud and nodata percentage for a chip

        Uses the qa band to compute these values.
        """
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
    """
    Indexer for Sentinel-2 STAC items
    """

    scl_filter = [1, 3, 8, 9, 10]
    nodata_value = 0

    @cached_property
    def scl(self):
        """
        The Scene Classification (SCL) band data for the STAC item
        """
        print("Loading scl band")
        with rasterio.open(self.item.assets["scl"].href) as src:
            return src.read(out_shape=(1, *self.shape), resampling=Resampling.nearest)[
                0
            ]

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        """
        Cloud and nodata percentage for a chip

        Uses the SCL band to compute these values.
        """
        scl = self.scl[
            y * self.chip_size : (y + 1) * self.chip_size,
            x * self.chip_size : (x + 1) * self.chip_size,
        ]

        cloud_percentage = int(np.isin(scl, self.scl_filter).sum()) / scl.size

        nodata_percentage = np.sum(scl == self.nodata_value) / scl.size

        return cloud_percentage, nodata_percentage


class ModisIndexer(ChipIndexer):
    """
    Indexer for MODIS STAC items
    """

    @cached_property
    def quality(self):
        """
        The Quality band data for the STAC item
        """
        print("Loading quality band")
        with rasterio.open(self.item.assets["sur_refl_qc_500m"].href) as src:
            return src.read(out_shape=(1, *self.shape), resampling=Resampling.nearest)[
                0
            ]

    def get_stats(self, x: int, y: int) -> Tuple[float, float]:
        """
        Cloud and nodata percentage for a chip
        """
        qa = self.quality[
            y * self.chip_size : (y + 1) * self.chip_size,
            x * self.chip_size : (x + 1) * self.chip_size,
        ]
        byte1 = np.array(1 << 0, dtype=qa.dtype)
        byte2 = np.array(1 << 1, dtype=qa.dtype)
        b1mask = np.bitwise_and(qa, byte1)
        b2mask = np.bitwise_and(qa, byte2)

        # Clouds are flagged as 10 in the first two bytes, nodata is flagged
        # as 11 in the first two bytes. Extracte from table 10 in
        # https://lpdaac.usgs.gov/documents/925/MOD09_User_Guide_V61.pdf
        cloud_mask = np.logical_and(b1mask, np.logical_not(b2mask))
        nodata_mask = np.logical_and(b1mask, b2mask)

        nodata_percentage = np.sum(nodata_mask) / nodata_mask.size
        cloud_percentage = np.sum(cloud_mask) / cloud_mask.size

        return cloud_percentage, nodata_percentage

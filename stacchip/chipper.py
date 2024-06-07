import math
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import rasterio
from numpy.typing import ArrayLike
from rasterio.enums import Resampling
from rasterio.windows import Window

from stacchip.indexer import ChipIndexer


class Chipper:
    """
    Chipper class
    """

    def __init__(
        self,
        indexer: ChipIndexer,
        mountpath: Optional[str] = None,
        asset_blacklist: Optional[List[str]] = None,
    ) -> None:
        """
        Init Chipper class
        """
        self.mountpath = None if mountpath is None else Path(mountpath)
        self.indexer = indexer

        if asset_blacklist is None:
            self.asset_blacklist = ["scl", "qa_pixel"]
        else:
            self.asset_blacklist = asset_blacklist

    def __len__(self):
        """
        Number of chips available
        """
        return self.indexer.size

    def __getitem__(self, index):
        """
        Get the chip by single index
        """
        y_index = index // self.indexer.x_size
        x_index = index % self.indexer.x_size
        return self.chip(x_index, y_index)

    def __iter__(self):
        """
        Iterate over chips
        """
        counter = 0
        while counter < self.indexer.size:
            yield self[counter]
            counter += 1

    def get_pixels_for_asset(self, key: str, x: int, y: int) -> ArrayLike:
        """
        Extract chip pixel values for one asset
        """
        asset = self.indexer.item.assets[key]

        srcpath = asset.href
        if self.mountpath:
            url = urlparse(srcpath, allow_fragments=False)
            srcpath = self.mountpath / Path(url.path.lstrip("/"))

        with rasterio.open(srcpath) as src:
            # Currently assume that different assets may be at different
            # resolutions, but are aligned and the gsd differs by an integer
            # multiplier.
            if self.indexer.shape[0] % src.height:
                raise ValueError(
                    f"Asset height {src.height} is not a multiple of highest resolution height {self.indexer.shape[0]}"  # noqa: E501
                )

            if self.indexer.shape[1] % src.width:
                raise ValueError(
                    f"Asset width {src.width} is not a multiple of highest resolution height {self.indexer.shape[1]}"  # noqa: E501
                )

            factor = self.indexer.shape[0] / src.height

            chip_window = Window(
                math.floor(x * self.indexer.chip_size / factor),
                math.floor(y * self.indexer.chip_size / factor),
                math.ceil(self.indexer.chip_size / factor),
                math.ceil(self.indexer.chip_size / factor),
            )

            return src.read(
                window=chip_window,
                out_shape=(src.count, self.indexer.chip_size, self.indexer.chip_size),
                resampling=Resampling.nearest,
            )

    def chip(self, x: int, y: int) -> dict:
        """
        Chip pixel array for the x and y index numbers
        """
        keys = []
        for key in self.indexer.item.assets.keys():
            if key not in self.asset_blacklist:
                keys.append(key)

        return {key: self.get_pixels_for_asset(key, x, y) for key in keys}

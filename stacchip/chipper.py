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
    Chipper class for managing and processing raster data chips.
    """

    def __init__(
        self,
        indexer: ChipIndexer,
        mountpath: Optional[str] = None,
        assets: Optional[List[str]] = None,
        asset_blacklist: Optional[List[str]] = None,
    ) -> None:
        """
        Initializes the Chipper class.

        Args:
            indexer (Type[ChipIndexer]): Input data which has to be of type ChipIndexer.
            mountpath (Optional[str]): Path to the mount directory for raster indexer.
                Defaults to None.
            assets (Optional[List[str]]): List of asset names to include for processing.
                If not provided, all assets are processed. Defaults to None.
            asset_blacklist (Optional[List[str]]): List of asset names to exclude from
                processing. Defaults to None.

        """
        self.mountpath = None if mountpath is None else Path(mountpath)
        self.assets = assets
        self.asset_blacklist = asset_blacklist
        self.indexer = indexer

    def __len__(self) -> int:
        """
        Returns the number of chips available.

        Returns:
            int: Number of chips available based on the indexer size.
        """
        return self.indexer.size

    def __getitem__(self, index: int) -> tuple:
        """
        Gets the chip by a single index.

        Args:
            index (int): Index of the chip to retrieve.

        Returns:
            tuple: A tuple containing x index, y index, and the chip data.
        """
        y_index = index // self.indexer.x_size
        x_index = index % self.indexer.x_size
        return x_index, y_index, self.chip(x_index, y_index)

    def __iter__(self):
        """
        Iterates over chips.

        Yields:
            tuple: The next chip data in the sequence.
        """
        counter = 0
        while counter < self.indexer.size:
            yield self[counter]
            counter += 1

    def get_pixels_for_asset(self, key: str, x: int, y: int) -> ArrayLike:
        """
        Extracts chip pixel values for one asset.

        Args:
            key (str): The asset key to extract pixels from.
            x (int): The x index of the chip.
            y (int): The y index of the chip.

        Returns:
            ArrayLike: Array of pixel values for the specified asset.

        Raises:
            ValueError: If asset dimensions are not multiples of the highest resolution dimensions.
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
                    f"Asset width {src.width} is not a multiple of highest resolution width {self.indexer.shape[1]}"  # noqa: E501
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
        Retrieves chip pixel array for the specified x and y index numbers.

        Args:
            x (int): The x index of the chip.
            y (int): The y index of the chip.

        Returns:
            dict: A dictionary where keys are asset names and values are arrays of pixel values.
        """
        if self.assets is not None:
            keys = self.assets
        else:
            keys = list(self.indexer.item.assets.keys())

        if self.asset_blacklist is not None:
            keys = [key for key in keys if key not in self.asset_blacklist]

        return {key: self.get_pixels_for_asset(key, x, y) for key in keys}

CHIP_SIZE = 256
CHIP_DTYPE = "float32"

LANDSAT_ASSETS = ["coastal", "blue", "green", "red", "nir08", "swir16", "swir22"]
SENTINEL_ASSETS = [
    "aot",
    "blue",
    "coastal",
    "green",
    "nir",
    "nir08",
    "nir09",
    "red",
    "rededge1",
    "rededge2",
    "rededge3",
    "swir22",
]
NAIP_ASSETS = ["asset"]
NZ_ASSETS = ["asset"]

ASSET_LOOKUP = {
    "landsat": LANDSAT_ASSETS,
    "sentinel": SENTINEL_ASSETS,
    "naip": NAIP_ASSETS,
    "nz": NZ_ASSETS,
}

LANDSAT_ASSET_NORMS = {
    "coastal": [1, 1],
    "blue": [1, 1],
    "green": [1, 1],
    "red": [1, 1],
    "nir08": [1, 1],
    "swir16": [1, 1],
    "swir22": [1, 1],
}

NAIP_ASSET_NORMS = {
    "asset": [1, 1],
}

NZ_ASSET_NORMS = {
    "asset": [1, 1],
}

SENTINEL_ASSET_NORMS = {
    "aot": [1, 1],
    "blue": [1, 1],
    "coastal": [1, 1],
    "green": [1, 1],
    "nir": [1, 1],
    "nir08": [1, 1],
    "nir09": [1, 1],
    "red": [1, 1],
    "rededge1": [1, 1],
    "rededge2": [1, 1],
    "rededge3": [1, 1],
    "swir22": [1, 1],
}

NORM_LOOKUP = {
    "landsat": LANDSAT_ASSET_NORMS,
    "sentinel": SENTINEL_ASSET_NORMS,
    "naip": NAIP_ASSET_NORMS,
    "nz": NZ_ASSET_NORMS,
}

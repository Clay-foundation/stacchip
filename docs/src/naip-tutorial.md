The following code example shows how to obtain RGB+NIR chips from
NAIP imagery and plot them.

```python
import random

import pystac_client
from stacchip.indexer import NoStatsChipIndexer
from stacchip.chipper import Chipper
import os
import matplotlib.pyplot as plt

# Optimize GDAL settings for cloud optimized reading
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
os.environ["AWS_REQUEST_PAYER"] = "requester"

# Query STAC catalog for NAIP data
catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")


items = catalog.search(
    collections=["naip"],
    max_items=100,
)

items = items.item_collection()

items_list = list(items)
random.shuffle(items_list)

chips = []
for item in items_list[:10]:
    print(f"Working on {item}")

    # Index the chips in the item
    indexer = NoStatsChipIndexer(item)

    # Instanciate the chipper
    chipper = Chipper(indexer, assets=["image"])

    # Get first chip for the "image" asset key
    for chip_id in random.sample(range(0, len(chipper)), 5):
        x_index, y_index, chip = chipper[chip_id]
        chips.append(chip["image"])


fig, axs = plt.subplots(5, 10, gridspec_kw={'wspace': 0.01, 'hspace': 0.01}, squeeze=True)

for idx, ax in enumerate(axs.flatten()):
    chip = chips[idx]
    # Visualize the data
    ax.imshow(chip[:3].swapaxes(0, 1).swapaxes(1, 2))

plt.tight_layout()
plt.show()
```

Resutling in the following plot

![naip-rgb](https://github.com/Clay-foundation/stacchip/assets/901647/86844530-9297-4971-b9e5-dd5c25b28b0e)

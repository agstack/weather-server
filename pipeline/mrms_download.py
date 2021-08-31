import pyarrow.feather as feather
import re
import os
from urllib.request import urlopen


def download(inventory, dest_dir, max_download=4):
    """Given an inventory file for a particular day, and a destination
    directory, download all files in the inventory that are not
    already in the destination directory with the same size.

    While at it, don't download more than `max_download` files in this
    call.
    """

    inv_df = feather.read_feather(inventory)
    downloads = 0

    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    for i in range(0, inv_df.shape[0]):
        url = inv_df["url"][i]
        file = os.path.basename(url)

        date = inv_df["date"][i]
        output_dir = os.path.join(dest_dir, str(date.year), "{:02d}".format(date.month), "{:02d}".format(date.day))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_file = os.path.join(output_dir, file)

        expected = inv_df["size"][i]
        size_ok = correct_size(expected, output_file)
        if not os.path.exists(output_file) or not size_ok:
            with open(output_file, "wb") as out:
                with urlopen(url) as input:
                    print(f"downloading to {output_file}")
                    out.write(input.read())
                    downloads += 1
                    if downloads >= max_download:
                        return


def correct_size(expected, file):
    if not os.path.exists(file):
        return False
    if not expected:
        return False

    units = re.sub(r"\d+", "", expected)
    size = int(re.sub(r"[KMGT]+\w*$", "", expected))
    tolerance = 1
    for c in units:
        for i in range(4):
            if c.upper() in "KMGT"[i:]:
                size = size * 1024
                tolerance = tolerance / 1.025

    actual = os.path.getsize(file)
    if actual == 0:
        return size == 0

    ratio = size / actual
    if ratio > 1:
        ratio = 1.0 / ratio
    return ratio >= tolerance

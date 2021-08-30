import pyarrow.feather
import tempfile
import mrms_inventory
import mrms_download
import os
import datetime

target = tempfile.TemporaryDirectory()
inv_dir = tempfile.TemporaryDirectory()


def test_download():
    inv = mrms_inventory.inventory(0, datetime.date(2021, 8, 21))
    inventory_file = os.path.join(inv_dir.name, "inv.feather")
    target_dir = target.name
        
    pyarrow.feather.write_feather(inv, inventory_file)
    mrms_download.download(inventory_file, target_dir)
    contents = os.listdir(target_dir)
    assert len(contents) == 4

    # mess with two files
    with open(os.path.join(target_dir, contents[0]), "w") as out:
        out.write("messed up contents")
    os.remove(os.path.join(target_dir, contents[1]))

    # download 5 more
    mrms_download.download(inventory_file, target_dir, max_download=5)

    # verify we restored the files we messed with and got 3 new ones
    more = os.listdir(target_dir)
    assert len(more) == 7
    assert len(set(more).intersection(set(contents))) == 4
    assert len(set(more).difference(set(contents))) == 3

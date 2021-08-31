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
    first_pass = os.listdir(os.path.join(target_dir, "2021", "08", "21"))
    assert len(first_pass) == 4

    # mess with two files
    with open(os.path.join(target_dir, "2021", "08", "21", first_pass[0]), "w") as out:
        out.write("messed up first_pass")
    os.remove(os.path.join(target_dir, "2021", "08", "21", first_pass[1]))

    # download 5 more
    mrms_download.download(inventory_file, target_dir, max_download=5)

    # verify we restored the files we messed with and got 3 new ones
    more = os.listdir(os.path.join(target_dir, "2021", "08", "21"))
    assert len(more) == 7
    assert len(set(more).intersection(set(first_pass))) == 4
    assert len(set(more).difference(set(first_pass))) == 3

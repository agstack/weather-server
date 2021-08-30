import unittest
import pyarrow.feather
import tempfile
import mrms_inventory
import mrms_download
import os
import datetime

class TestDownload(unittest.TestCase):
    target = tempfile.TemporaryDirectory()
    inv_dir = tempfile.TemporaryDirectory()

    def test_download(self):
        inv = mrms_inventory.inventory(0, datetime.date(2021,8,21))
        inventory_file = os.path.join(self.inv_dir.name, "inv.feather")
        target_dir = self.target.name
        
        pyarrow.feather.write_feather(inv, inventory_file)
        mrms_download.download(inventory_file, target_dir)
        contents = os.listdir(target_dir)
        self.assertEqual(4, len(contents))

        # mess with two files
        with open(os.path.join(target_dir, contents[0]), "w") as out:
            out.write("messed up contents")
        os.remove(os.path.join(target_dir, contents[1]))

        # download 5 more
        mrms_download.download(inventory_file, target_dir, max_download=5)

        # verify we restored the files we messed with and got 3 new ones
        more = os.listdir(target_dir)
        self.assertEquals(7, len(more))
        self.assertEquals(4, len(set(more).intersection(set(contents))))
        self.assertEquals(3, len(set(more).difference(set(contents))))

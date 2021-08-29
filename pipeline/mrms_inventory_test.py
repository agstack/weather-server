import unittest
from mrms_inventory import *
import datetime
import pandas
import re
import os.path
import pytz

class TestInventory(unittest.TestCase):
    def test_one_day(self):
        inv = inventory(0, date(2021,8,21))
        self.assertEqual(inv.shape[0], 24)
        self.assertEqual(inv.shape[1], 3)
        self.verify_day(datetime.datetime(2021,8,21), inv, True)

    def test_start_offset(self):
        for start in [-3, timedelta(days=-3), date(2021,8,18)]:
            inv = inventory(start, date(2021,8,21))
            self.assertEqual(inv.shape[0], 4 * 24)
            self.assertEqual(inv.shape[1], 3)
            for i in range(0, 4):
                self.verify_day(datetime.datetime(2021, 8, 21+i-3), inv.iloc[i*24:(i+1)*24], True)

    def test_no_pattern(self):
        inv = inventory(0, date(2021,8,21), mtime_pattern="", size_pattern="")  
        self.assertEqual(inv.shape[0], 24)
        self.assertEqual(inv.shape[1], 3)
        self.verify_day(None, inv, False)

        inv = inventory(0, date(2021,8,21), size_pattern="")  
        self.assertEqual(inv.shape[0], 24)
        self.assertEqual(inv.shape[1], 3)
        self.verify_day(datetime.datetime(2021,8,21), inv, False)

    def test_invalid(self):
        try:
            inv = inventory(0, date(2021,8,21), mtime_pattern="")
            self.fail("Expected exception")
            
        except ValueError:
            # expected this
            return
        
    def verify_day(self, expected, inv, has_mtime=True):
        for f in inv["url"]:
            self.assertTrue(re.match(r"Multi.*gz", os.path.basename(f)))
        for s in inv["size"]:
            if has_mtime:
                self.assertTrue(re.match(r"\d+[KMGT]*", s))
            else:
                self.assertTrue(s is None)
        if expected:
            for d in inv["mtime"]:
                # Iowa State is in Ames, Iowa which is the same timezone as Chicago
                tz = pytz.timezone("America/Chicago")
                # but the files are produced relative to the day in UTC time
                actual = tz.localize(datetime.datetime.strptime(d, "%Y-%m-%d %H:%M")).astimezone(pytz.utc)
                delta = actual - pytz.utc.localize(expected)
                self.assertTrue(delta <= datetime.timedelta(hours=25))
            

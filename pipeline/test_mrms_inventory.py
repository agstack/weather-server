import mrms_inventory
import datetime
import re
import os.path
import pytz


def test_one_day():
    inv = mrms_inventory.inventory(0, datetime.date(2021, 8, 21))
    assert inv.shape[0] == 24
    assert inv.shape[1] == 4
    d = inv["date"][0]
    print(f"{d} {type(d)} {d == datetime.date(2021, 8, 21)}")
    verify_day(datetime.datetime(2021, 8, 21), inv, True)


def test_start_offset():
    for start in [-3, datetime.timedelta(days=-3), datetime.date(2021, 8, 18)]:
        inv = mrms_inventory.inventory(start, datetime.date(2021, 8, 21))
        assert inv.shape[0] == 4 * 24
        assert inv.shape[1] == 4
        for i in range(0, 4):
            verify_day(datetime.datetime(2021, 8, 21+i-3), inv.iloc[i*24:(i+1)*24], True)


def test_no_pattern():
    inv = mrms_inventory.inventory(0, datetime.date(2021, 8, 21), mtime_pattern="", size_pattern="")
    assert inv.shape[0] == 24
    assert inv.shape[1] == 4
    verify_day(None, inv, False)

    inv = mrms_inventory.inventory(0, datetime.date(2021, 8, 21), size_pattern="")
    assert inv.shape[0] == 24
    assert inv.shape[1] == 4
    verify_day(datetime.datetime(2021, 8, 21), inv, False)


def test_invalid():
    try:
        mrms_inventory.inventory(0, datetime.date(2021, 8, 21), mtime_pattern="")
        assert not "Expected exception"
    except ValueError:
        pass


def verify_day(expected, inv, has_mtime=True):
    for f in inv["url"]:
        assert re.match(r"Multi.*gz", os.path.basename(f))
    for s in inv["size"]:
        if has_mtime:
            assert re.match(r"\d+[KMGT]*", s)
        else:
            assert s is None

    if expected:
        for d in inv["date"]:
            assert d == expected.date()

        for d in inv["mtime"]:
            # Iowa State is in Ames, Iowa which is the same timezone as Chicago
            tz = pytz.timezone("America/Chicago")
            # but the files are produced relative to the day in UTC time
            actual = tz.localize(datetime.datetime.strptime(d, "%Y-%m-%d %H:%M")).astimezone(pytz.utc)
            delta = actual - pytz.utc.localize(expected)
            assert delta <= datetime.timedelta(hours=25)

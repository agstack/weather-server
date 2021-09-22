import argparse
from urllib.request import urlopen
import re
from datetime import timedelta
from datetime import date
from datetime import datetime
import pandas
import pyarrow.feather


def inventory(start=-200, end=date.today(),
              url="https://mtarchive.geol.iastate.edu/{year:4d}/{month:02d}/{day:02d}/mrms/ncep/MultiSensor_QPE_01H_Pass2",
              file_prefix="Multi",
              anchor_pattern='<a href={quote_char}({file_prefix}.*?){quote_char}.*?</a>.*?{date_pattern}.*?{size_pattern}',
              quote_char='"',
              mtime_pattern=r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})",
              size_pattern=r"(\d+[KM])"):
    """Reads file inventories from a website for dates ranging from
    `start` to `end` (inclusive). The result is a dataframe containing
    the date for file, the URL for a file, the last-modified time and
    the size of the file.

    Start and end can be expressed as a timestamp, an integer
    representing a relative day, a datetime.timedelta or a
    datetime.date. The default starting time is 200 days ago and the
    default end is today. A relative starting time is relative to the
    end.  A relative end time is relative to now.

    For each day in that range, `url` is used to format the year, month
    and day into a url that can be used to read an inventory page. The
    url can contain references to `year`, `month` and `day` by those names
    or in that order, as you like.  The default URL points to the Iowa State
    archive for MRMS data.

    The downloaded page is assumed to be in HTML form and will be
    scanned for anchors that refer to files that start with
    `file_prefix`. All such file names are returned in a list as
    absolute URLs. By default, the files are assumed to start with
    "Multi" as is the case with the MRMS files.

    You can customize the html pattern used to extract links by changing
    `anchor_pattern`. It is assumed that `anchor_pattern` will capture
    the relative URL of each file referenced from the page. By default,
    this looks for the href value in a double-quoted HTML anchor. You
    can change this by setting `quote_char`.

    You can also change the patterns for the last-modified time
    (`mtime_pattern`) and the size (`size_pattern`). Setting these to an
    empty string will prevent the inventory from containing those
    values. If you set these, make sure you surround the pattern with
    "()" so that the matched string will be captured and returned.

    """

    dt = timedelta(days=1)

    end = force_date(end, date.today())
    start = force_date(start, end)

    if (not mtime_pattern or mtime_pattern == "") and (size_pattern != ""):
        raise ValueError("Must disable mtime parsing if you disable size parsing")

    pattern = re.compile(anchor_pattern.format(file_prefix=file_prefix,
                                               quote_char=quote_char,
                                               date_pattern=mtime_pattern,
                                               size_pattern=size_pattern))

    results = pandas.DataFrame({}, [], ["date", "url", "mtime", "size"])

    t = start
    while (t <= end):
        actual_url = url.format(year=t.year, month=t.month, day=t.day)
        with urlopen(actual_url) as input:
            soup = input.read().decode('utf-8')

        for m in re.finditer(pattern, soup):
            file = actual_url + "/" + m.group(1)
            extras = [m.group(i) for i in range(2, len(m.groups()) + 1)]
            while len(extras) < 2:
                extras.append(None)
            results = results.append(dict(date=t, url=file, mtime=extras[0], size=extras[1]), ignore_index=True)

        t = t + dt

    return results


def force_date(t, base=date.today()):
    """
    force_date(t)

    Converts a variety of values into a sensible date. These include:
    - integers are interpreted as relative dates. Thus, -1 is yesterday
    - floats are interpreted as timestamps in seconds since the epoch as with time.time()
    - a datetime.timedelta is interpreted relative to today
    - a datetime.date is returned verbatim
    """

    if isinstance(t, date):
        return t
    elif isinstance(t, timedelta):
        return base + t
    elif isinstance(t, int):
        return base + timedelta(days=t)
    elif isinstance(t, float):
        return date.fromtimestamp(t)
    else:
        raise ValueError(f"Expected date, timedelta, small integer or timestamp, got {type(t)}")


def parse_date(s):
    mx = re.compile(r"\-?\d+")
    if mx.match(s):
        return int(s)
    else:
        return datetime.strptime(s, "%Y-%m-%d")


if __name__ == "__main__":
    # execute only if run as a script
    parser = argparse.ArgumentParser(description='Download MRMS file inventories')
    parser.add_argument("--start", nargs='?', default="0", help="Starting date in yyyy-mm-dd form or an integer indicating number of days before the ending date")
    parser.add_argument("--end", nargs='?', default="0", help="Ending date in yyyy-mm-dd form or as number of days offset from today. Default is today")
    parser.add_argument("out", help="Output file name")

    args = parser.parse_args()
    inv = inventory(parse_date(args.start), parse_date(args.end))
    print(inv)
    print(args.out)
    with open(args.out, "wb") as output:
        pyarrow.feather.write_feather(inv, output)

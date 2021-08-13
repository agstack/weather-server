# This program reads a day of data in CSV form and computes S2 hash
# for each data point.

# It is ... ahem ... unacceptably slow
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import csv

# read one day of data by accumulating tables, one per CSV file
t = []
for i in range(24):
    tx = pa.csv.read_csv("grib/20210806%02d.grib2.csv" % i)
    t.append(tx)
# now glue those all together
all = pa.concat_tables(t)


# Add S2 tags to each data point
def s2cell(row):
     z.append(s2sphere.CellId.from_lat_lng(s2sphere.LatLng(row['latitude'], row['longitude']))

# to do that, we need to switch to pandas. The conversion takes a bit of time, but running
# python against each row is excruciating
hashed = all.to_pandas().apply(s2cell, axis=1)

# if you got this far, we can talk about how to sort the table. This takes next to no time
i = pc.sort_indices(all, sort_keys=[('latitude', 'ascending'), ('longitude', 'ascending'), ('datetime', 'ascending')])
sorted = pc.take(all, i)

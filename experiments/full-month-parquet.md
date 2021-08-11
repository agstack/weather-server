# Querying a full month using Parqut

This experiment involved querying a full month of Parquet data using different arrangements of data.

# The data
I used 740 CSV files with non-zero precipitation from MRMS. These represented gridded locations that had non-trivial precipation on an hourly basis.

The data was converted day by day into parquet partitioned into 1x1 degree boxes and by day. The inclusion of day as a partition parameter allows these files to be queried as a large group with no further processing. The data preparation queries looked like this
```sql
create table dfs.home.`mrms/grib-07-30.parquet`
partition by (box, d)
as 
with
  t1 as (
    select value as precip, 
           extract(month from datetime)*100 + extract(day from datetime) as d, 
           datetime as t, 
           cast(latitude as double) as latitude, 
           cast(longitude as double) longitude
from table(dfs.home.`mrms/grib-07/20210730*csv`(type => 'text', fieldDelimiter => ',', extractHeader => true))
  )
select precip, d, t, latitude, longitude, floor(latitude)*100 - floor(longitude) box 
from t1 
order by box, d, latitude, longitude, t
```
These queries were customized for each day and executed by a shell script for each day.

# The first experiment
As a first experiment, I did a few queries targeting a particular location as in previous Parquet experiments but using the directories created for each day via a wildcard source:
```sql
select precip, t, latitude, longitude
from dfs.home.`mrms/grib-07-*.parquet`
where box = 3289.0 and latitude = 32.025 and longitude = -88.025
```
This query took 11 seconds to plan and 170ms to run. Most of the execution time was in the Parquet scanner itself. This scan was parallelized into multiple scans. It was not immediately apparent how much time the scanner fragments spent getting started and how much time they spent scanning but the combined time was still only 50ms for most fragments with one 100ms straggler.

Examination of the physical plan showed that the planner was able to use the partitioning of the data effectively. It is also clear, however, that Drill's strategy of extracting partition information at runtime causes a large delay before actually scanning the data. I would expect a production server to use a file naming scheme that includes partition information to avoid this.

# Second experiment
The original experimented included multiple directories via a wildcard table. That led to the question of what would happen with a hierarchical directory or if all of the partition files in the daily datasets could be moved into a single directory.

The results for several variations of this experiment were frustrating. First, the files cannot easily be copied into a single directory because many have the same file names. Second, Drill doesn't like symbolic links. Third when a two level directory structure is used, the planning time increases relative to the wildcard query to well over a minute. The partitioning still has the desired effect and the time spent reading data is small, but the results overall are underwhelming.


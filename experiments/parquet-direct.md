# Direct service of data from partitioned parquet

The idea here is that Parquet files have a fair bit of logic
already built in to help queries run pretty fast. In particular,
there is a concept fo a partitioned Parquet data set which is
really just a directory full of Parquet files such that each
file has a limited number of distinct values of some fields.

In Apache Drill, you can create such a partitioned dataset with
a query like this:
```sql
create table dfs.home.`mrms/datadir`
partition by box
select box, other_data
from somewhere
```

# The Experiment
What I did was to take GRIB files containing MRMS data for the 
month of July, 2021. I created a partitioned data set for the
first five days of July with queries like this:
```sql
create table dfs.home.`mrms/grib-07-05.parquet`
partition by (box)
as 
with
t1 as (
select value as precip, datetime as t, cast(latitude as double) as latitude, cast(longitude as double) longitude
from table(dfs.home.`mrms/grib-07-01/20210705*csv`(type => 'text', fieldDelimiter => ',', extractHeader => true))
      )

select precip, t, latitude, longitude, floor(latitude)*100 - floor(longitude) box
from t1
order by box, latitude, longitude, t
```
The 05 string in this query indicates that the table created was for the 5th. On my test 
cluster, this took about 40-70 seconds for each day.

Then I used the following query to concatenate these files:
```sql
create table dfs.home.`grib-week.parquet`
partition by (box)
as (with t1 as (
select precip, t, latitude, longitude, box from dfs.home.`mrms/grib-07-01.parquet`
union all
select precip, t, latitude, longitude, box from dfs.home.`mrms/grib-07-02.parquet`
union all
select precip, t, latitude, longitude, box from dfs.home.`mrms/grib-07-03.parquet`
union all
select precip, t, latitude, longitude, box from dfs.home.`mrms/grib-07-04.parquet`
union all
select precip, t, latitude, longitude, box from dfs.home.`mrms/grib-07-05.parquet`
)
select box, precip, t, latitude, longitude 
from t1 
order by box, latitude, longitude, t
)
```
This concatenation query took about 150 seconds to run and processed about 80M rows
for the five days in the test. Roughly half of the CPU resources
when into the parquet writer and a quarter of the resources were associated with the
sort and an eighth were associated with the merge after the sort.

This query was then used to get a feel for how fast we can expect a query for a single
data point to be:
```sql
select precip, t, latitude, longitude
from dfs.home.`mrms/grib-week.parquet`
where box = 3289.0 and latitude = 32.015 and longitude = -88.025
```
Examination of the diagnostics for this query showed that planning for the query 
took about 500ms and execution took about 12ms. No parallelism was observed in 
the execution as would be expected. This is the physical plan for this query:
```
00-00    Screen : rowType = RecordType(ANY precip, ANY t, ANY latitude, ANY longitude): rowcount = 176.80949999999999, cumulative cost = {105147.29995 rows, 509248.68645 cpu, 261940.0 io, 0.0 network, 0.0 memory}, id = 5333
00-01      Project(precip=[$3], t=[$4], latitude=[$1], longitude=[$2]) : rowType = RecordType(ANY precip, ANY t, ANY latitude, ANY longitude): rowcount = 176.80949999999999, cumulative cost = {105129.619 rows, 509231.00549999997 cpu, 261940.0 io, 0.0 network, 0.0 memory}, id = 5332
00-02        SelectionVectorRemover : rowType = RecordType(ANY box, ANY latitude, ANY longitude, ANY precip, ANY t): rowcount = 176.80949999999999, cumulative cost = {104952.8095 rows, 508523.76749999996 cpu, 261940.0 io, 0.0 network, 0.0 memory}, id = 5331
00-03          Filter(condition=[AND(=($0, 3289.0), =($1, 32.015), =($2, -88.025))]) : rowType = RecordType(ANY box, ANY latitude, ANY longitude, ANY precip, ANY t): rowcount = 176.80949999999999, cumulative cost = {104776.0 rows, 508346.958 cpu, 261940.0 io, 0.0 network, 0.0 memory}, id = 5330
00-04            Scan(table=[[dfs, home, mrms/grib-week.parquet]], 
                     groupscan=[ParquetGroupScan [
                        entries=[ReadEntryWithPath [path=/mapr/c0/user/tdunning/mrms/grib-week.parquet/0_0_356.parquet]], 
                        selectionRoot=maprfs:/mapr/c0/user/tdunning/mrms/grib-week.parquet, 
                        numFiles=1, numRowGroups=1, 
                        filter=booleanAnd(...) ) , columns=[...]]]) ... 
```
Examination of this physical plan shows that the query only looked at one partition file 
and a single rowgroup. It was also able to push down the selection criterion into the Parquet scanner.

Examination of the execution trace for the query indicates that although the selection 
criterion appears to have been pushed down into the scanner, it does not appear to have 
had any effect. Instead, just over 50k rows were passed to the filter operator which 
limited the result to the expected 6 values.

# Conclusions
In an actual server, the partition selection would likely be done more efficiently than is 
done in a general purpose cost-based optimizer as is done with Drill. This would imply that
our total response time would be comparable the time that Drill spent executing the query
after planning it or about 10ms. This compares very favorably with the speed that could be
expected from a database query and is probably not significantly worse than what we could
expect with from an in-memory given other costs in serving a query such as HTTP setup.


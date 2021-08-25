# Weather server dataflow

## From slack discussion
> For MRMS, I download the data using Python on my desktop; process it in WCT on my desktop, which saves the CSV file to the server; then separate the data by truncated lat-lon and import it into PostgreSQL with Python running on the server.  (This is all automated since MRMS data comes in every hour.). I'd like to streamline that process using a single Julia program on the server.  That should be more useful to you than my current convoluted procedure, and I may have source code to share by the end-of-day.  For NLDAS-2, I download the data once a day on my desktop using Python on my desktop.  This program also extracts the data, separating it by truncated lat-lon, and saving to CSV files on the server.  A separate Python script on the server imports the CSV files into PostgreSQL.

> I'd also like to streamline this process using Julia, but cfgrib.jl doesn't like the NLDAS-2 format - perhaps because it's GRIB-1.  However, I seem to be having some success after modifying the cfgrib.jl source.

> MRMS data is available at Iowa State, for example:  https://mtarchive.geol.iastate.edu/2021/08/20/mrms/ncep/MultiSensor_QPE_01H_Pass2/

> NLDAS-2 data is available through https://urs.earthdata.nasa.gov/, but you need an account in order to scrape it.
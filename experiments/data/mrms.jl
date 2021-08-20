using Pkg; Pkg.activate("."); Pkg.instantiate()  # needed since CfGRIB hasn't yet been added to Julia package registry
using CfGRIB
using AxisArrays
using Printf
using Dates
using HTTP
using GZip

"""
readMRMS(fname)

Extracts MRMS data in grib format from a specified file and then extracts specific
data elements into CSV formatted files in a directory call csv (one file per location 
with non-zero precipitation)
"""
function readMRMS(fname)
    fh = GZip.open(fname, "r")
    data = read(fh)
    io = open("data.grib2", "w")
    write(io, data)
    close(io)
    dataset = CfGRIB.DataSet("data.grib2")
    
    # Get datetime from dataset, but we could also parse the fname instead
    datetime = replace(string(unix2datetime(dataset.variables["time"].data)), "T" => " ")
    lats = dataset.variables["latitude"].data
    lons = dataset.variables["longitude"].data
    data = dataset.variables["paramId0"].data[:, :]
    files = Dict()
    # MRMS is 3500 x 7000
    for i = 1:3500, j = 1:7000
        lon = lons[j] - 360
        # Conterminous US data only
        if 25 <= lats[i] <= 50 && -125 <= lon <= -67
            # Exclude non-detectable precipitation to save space
            if data[j, i] > 0
                fn = "$(trunc(Int, lats[i]))_$(trunc(Int, -lon))"
                if !haskey(files, fn)
                    files[fn] = open("csv/$fn.csv", "w");
                    write(files[fn], "precipitation,date,lat,lon\n")
                end
                write(files[fn], "$(data[j, i]),$(datetime),$(@sprintf("%.4f", lats[i])),$(@sprintf("%.4f", lon))\r\n")
            end
        end
    end
    for file in keys(files)
        close(files[file])
    end
    # not yet implemented
    # writeToPostgres()
end

"""
processMRMS()

Downloads data from the Iowa State archive of MRMS data. Subsequent to this
and after a 15 minute delay, the downloaded data is extracted into CSV form.
"""
function processMRMS()
    for date in Dates.Date(2021, 1, 1):Day(1):today()
        year = Dates.year(date)
        m2 = lpad(Dates.month(date), 2, "0")
        d2 = lpad(Dates.day(date),   2, "0")
        for hour in 0:23
            h2 = lpad(hour, 2, "0")
            fname = "MultiSensor_QPE_01H_Pass2_00.00_$year$m2$d2-$(h2)0000.grib2.gz"
            if !isfile("mrms/$fname")
                url = "https://mtarchive.geol.iastate.edu/$year/$m2/$d2/mrms/ncep/MultiSensor_QPE_01H_Pass2/$fname"
                try
                    HTTP.download(url, "mrms/"; update_period = Inf)
                catch
                    rm("mrms/$fname") # file gets saved even if 404 error, so it must be removed
                    println("Couldn't download $url")
                    continue
                end
                println("Downloaded $fname")
                readMRMS("mrms/$fname")
            end
        end
    end
    print("Pausing for 15 minutes")
    sleep(60 * 15)
    processMRMS()
end


processMRMS()

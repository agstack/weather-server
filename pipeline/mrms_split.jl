using GRIB, DataFrames, Query, DataStructures, H3, H3.API, H3Geometry, GZip, Dates
using Arrow, Parquet, TimeZones, Printf, Distributed, Dagger

""" 
Reads downloaded MRMS weather data and arranges it in a dataframe with
columns for latitude, longitude, time, h3 key, tile id and precipitation.

Time is represented as milliseconds since 1970 instead of a proper ZonedDateTime
in order to make storing the result using Parquet easy. Likewise, the H3
fields (h3, tile) are converted to signed integers.

Due to memory constraints, it isn't practical to ingest all of the data
over a long time period into a DataFrame (even one day can be taxing). 
As a result, the normal process is to read each hour of data separately,
group by tile and write the data one file per tile retaining only time, h3 and
precipitation in order to save space. Subsequently, the data for a particular
tile can be concatenated and sorted by h3 and time in order to build a useful
historical dataset.

The DataFrame that we return here uses RepeatVectors for latitude, longitude, 

All of these steps come down to two functions. The first splits a
single GRIB file into files, one for each hex at a specifed level
(levels 3,4 and 5 result in 2025, 13709, and 94718 files respectively). The
second function merges corresponding files across multiple directories
into a composite output. This arrangement means that we only need to
store at most one GRIB file or data for one tile of output.

"""
function readData(inputFile::String, tileLevel::Int)
    t = @elapsed begin
        if endswith(inputFile, ".gz")
            mktemp() do tmp, tmp_io
                GZip.open(inputFile, "r") do fh
                    data = read(fh)
                    write(tmp_io, data)
                end
                dataset = GribFile(tmp)
            end
        else
            dataset = GribFile(inputFile)
        end
        m = first(dataset)
        lons, lats, v = data(m)
        t0 = ZonedDateTime((m[i] for i in ("year", "month", "day", "hour", "minute", "second"))..., tz"UTC")
        t1 = Int((t0 - ZonedDateTime(1970, 1, 1, tz"UTC")) / Millisecond(1))
    end

    @info "read data" size=length(lons) time=t file=inputFile
    
    t = @elapsed begin
        # small cleanups on the data
        lons = collapse(Vector{Float32}(reshape(lons, :)))
        lats = collapse(Vector{Float32}(reshape(lats, :)))
        v = Vector{Float32}(reshape(v, :))
        lons.base[lons.base .> 180.0] .-= 360.0
        time = RepeatVector([t1], length(lons), length(lons))
    end
    @info "cleaned up data" elapsed=t time="$(length(time.base)) $(time.each)" lons="$(length(lons.base)) $(lons.each)" lats="$(length(lats.base)) $(lats.each)"

    # some geo data added
    t = @elapsed begin
        tile = zeros(Int64, length(lons))
        pt = zeros(Int64, length(lons))
        Threads.@threads for i in 1:length(lons)
            tile[i] = Int64(geoToH3(lats[i], lons[i], tileLevel))
            pt[i] = Int64(geoToH3(lats[i], lons[i], 15))
        end
    end
    @info "geo time" t=t
    base = (longitude=lons, latitude=lats, tile=tile, h3=pt, t=time)
    base = (base..., precipitation=v)
    return DataFrame(Tables.columntable(Tables.Schema(keys(base), typeof.(values(base))), base))
end

"""
Output a data frame into files, each containing data for a single geo-tile.
The data frame is assumed to contain latitude, longitude, h3-id, tile-id, 
time plus other data columns. Only h3-id, time and the other data are preserved
in order to minimize the size of the saved data. The data saved in each tile
file is ordered by h3 and then by time before being written out.
"""
function splitData(data, outputDir::String, hashDir::String, tileLevel)
    outCount = Threads.Atomic{Int}(0);
    step = 1
    scale = 1
    mkpath(outputDir)
    mkpath(hashDir)
    t0 = time()
    n0 = 0
    groups = [g for g in data |> @groupby(_.tile)]
    for td in groups
        tileData = DataFrame(td)
        @assert size(tileData)[1] > 0
        tile = tileData[1, :tile]
        fname = joinpath(outputDir, "x-$tileLevel-$tile")
        outputData = tileData |> 
            @orderby(_.h3) |> @thenby(_.t) |> 
            @select(-:longitude, -:latitude, -:tile)
        Parquet.write_parquet(fname, outputData)
        open(fname) do input
            h = hash(read(input))
            open(joinpath(hashDir, "x-$tileLevel-$tile"), "w") do output
                println(output, h)
            end
            try
                df0 = DataFrame(Parquet.Table(fname))
                @assert collapse(tileData[:,:t])==collapse(df0[:,:t])
                @assert length(unique(tileData[:,:h3]))==length(unique(df0[:,:h3]))
            catch e
                @warn "failed" fname
                throw(e)
            end
        end
        
        k = Threads.atomic_add!(outCount, 1)
        if k%(step*scale) == 0
            t1 = time()
            t = t1 - t0
            step = floor(step * 2.5)
            if step >= 10
                scale *= 10
                step = 1
            end
            @info "file" files=k time=t rate=(k-n0)/t s=step*scale
            n0 = k
        end
    end
end


"""
With roughly 25 million points per day and 100 day history, we need to
store about a 60 billion data points. Splitting these up into 1000
files means that each file only has a 25,000 histories containing
about 60 million data points which might be small enough for fast
access, but probably not. On the other hand, splitting into 100,000
files (which is still quite practical) results in each file having (on
average) 100 histories and about 240,000 data points. This is almost
certainly small enough to be fast to scan at query time.

Creating these files is done by splitting up each hourly file into 
tile files and then merging all of the files from each hour into the
daily files when each hourly file becomes available. Due to space
and file count issues, it is a good thing to remove hourly splits
as soon as practical (they can always be recreated).

This merge function works by scanning all of the directories to
get a complete list of all of the files that exist. Then, for
each file name, we concatenate all of the data in the file by
that name in any directory and write that concatenation out under 
the same name in the output directory. When that's all done, the 
output directory can be atomically renamed to be the final output 
directory if we want to avoid exposing half done work.

"""
function mergeData(output::String, dirs::String...)
    mkpath(output)
    dirs = sort([x for x in dirs])
    # compute list of all files
    files = union(readdir.(dirs)...) |> @filter(x -> startswith(x,"x-"))

    k = 0
    for f in files
        df = Vector()
        for fx in joinpath.(dirs, f)
            tbl = Parquet.read_parquet(fx)
            push!(df, DataFrame(tbl))
        end
        all = vcat(df...)
        Parquet.write_parquet(joinpath(output, f), all)
        k += 1
        if k % 100 == 0 || (k < 100 && k % 10 == 0)
            @info "progress" processed=k
        end
    end
end

"""
This scans input directories to find MRMS hourly data files. For 
each file found, that file is split into geo-tiles (unless it has
already been split).

This is done across as many workers as are available because
it can be kind of slow. Once this is done, the hours are
available for merging.
"""
function processDays(source, dest, first, last, tileLevel)
    @info "before"
    files = []
    date = first
    while date <= last
        day = Dates.format(date, "yyyy/mm/dd")
        dailyDir = joinpath(source, day)
        @info "Day" dailyDir
        hour = 0
        if isdir(dailyDir)
            for f in sort(readdir(dailyDir, join=true))
                data = joinpath(dest, "hourly/data", day, @sprintf("%02d", hour))
                if !isdir(data)
                    push!(files, (f,day,hour))
                end
                hour += 1
            end
        end
        date += Dates.Day(1)
    end
    @info "Starting" length(files)
    @distributed for f in 1:length(files)
        fname,day,hour = files[f]
        data = joinpath(dest, "hourly/data", day, @sprintf("%02d", hour))
        hash = joinpath(dest, "hourly/hash", day, @sprintf("%02d", hour))
        mkpath(data)
        mkpath(hash)

        m = readData(fname, tileLevel)
        @info "Read file" size(m)
        @info "Split data" data hash
        splitData(m, data, hash, tileLevel)
    end
end

"""
A RepeatVector looks like a vector with a fixed cycle of repeated elements
but only requires a small amount of storage
"""
struct RepeatVector{T} <: AbstractVector{T}
    base::Vector{T}
    each::Int64
    n::Int64
end

"""
Assuming that a vector contains lots of very regular repetition, 
collapse will extract a RepeatVector vector which requires less
space.
""" 
function collapse(input::Vector{T})::RepeatVector{T} where (T)
    each = 0
    x1 = first(input)
    for x in input
        if x == x1
            each += 1
        else
            break
        end
    end
    base = Vector{T}()
    push!(base, x1)
    x2 = x1
    for x in input[(each+1):end]
        if x == x1
            break
        end
        if x != x2
            push!(base, x)
            x2 = x
        end
    end
    return RepeatVector(base, each, length(input))
end

function Base.getindex(v::RepeatVector, i::Int)
    i -= 1
    i = i ÷ v.each
    return v.base[i % length(v.base) + 1]
end

function Base.size(v::RepeatVector)
    return (v.n,)
end



        

        

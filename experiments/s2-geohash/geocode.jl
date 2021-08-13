# This program reads a day of data in CSV form and computes H3 hash
# for each data point. The final result for the data is written to a 
# feather file for consumption by Arrow (probably from Python)

using H3.API
using CSV
using DataFrames
using Formatting
using Arrow

# read one day of data by accumulating tables, one per CSV file
t = []
for i in 0:23
    tx = DataFrame(CSV.File("grib/20210806" * fmt("02d", i) * ".grib2.csv"))
    push!(t, tx)
end
# now glue those all together
allDay = vcat(t...)


# Add H3 tags to each data point
tiles = Int64[]
for row in eachrow(allDay)
    push!(tiles, geoToH3(GeoCoord(deg2rad(row[:latitude]), deg2rad(row[:longitude])), 3))
end
allDay[!,:tile] = tiles

sort!(allDay, [:tile, :latitude, :longitude, :datetime])

# A tiled frame is grouped on some column for writing out Feather or Parquet files
# this lets us use Table.partitioner
struct TiledFrame 
    v::DataFrame
    col::Any
end

import Base.iterate, Base.getindex, Base.size
function Base.iterate(S::TiledFrame, i=0)
    if i > size(S.v)[1] 
        return nothing
    elseif i == 0
        return Base.iterate(S, 1)
    else 
        r0 = S[i, S.col]
        i0 = i
        while i <= size(S)[1] && S[i, S.col] == r0
            i += 1
        end
        return (S.v[i0:(i-1),:], i)
    end
end

function Base.getindex(S::TiledFrame, i::Any)
    return getindex(S.v, i)
end

function Base.getindex(S::TiledFrame, i::Any, j::Any)
    return getindex(S.v, i, j)
end

function size(S::TiledFrame)
    return size(S.v)
end

Arrow.write("x20210806.parquet", Tables.partitioner(TiledFrame(allDay, :tile)))
using H3, H3Geometry

"""
Finding the history for a point involves computing the fine-grained
H3 reference (typically at level 15) as well as the H3 tile (typically
level 3). The tile is used to form the file name from which to 
read the data and the reference is used to filter the data read from
the file. There may be multiple files to read, each with the same
name, but in different directories. 
"""
function queryData(latitude, longitude, dirs; refLevel=15, tileLevel=3)
    ref = H3.API.geoToH3(latitude, longitude, refLevel)
    queryData(ref, dirs; tileLevel=tileLevel)
end

function queryData(refs::Vector, dirs::Vector{String}; tileLevel=3)
    # the refs will almost always be on a single tile
    tiles = unique(H3.API.h3ToParent.(refs, tileLevel))

    df = Vector()
    for tile in tiles
        fname = "x-$tileLevel-$tile"
        for d in dirs
            fx = Parquet.Table(joinpath(d, fname))
            rx = DataFrame(fx) |> @filter(_.h3 in Int64.(refs)) |> DataFrame
            push!(df, rx)
        end
    end
    r = reduce(vcat, df, init=DataFrame())
    r[!, :time] = Dates.unix2datetime.(r.t ./ 1000)
#    r[!, :lat] .= latitude
#    r[!, :lon] .= longitude
    @info "result" size(r)
    r
end

"""
Reads the index that lets us find the nearest data point from a file.

The structure of the index is that of a dictionary keyed by h3 indexes
at resolution 5 and 6. The value is a list of h3 indexes at level
15 which have been observed in the data files and for which, presumably,
we can find data.
"""
function readIndex(fname)
    Dict(eachrow(DataFrame(Arrow.Table(fname))))
end

"""
Finds the closest grid points in an index to a specified lat/long pair

All grid points which are an equal grid distance from the original point
will be returned. Since we are using grid distance, there will likely
be more than one result and each should be used in querying.
"""
function getGridPoints(latitude, longitude, index)
    ref = H3.API.geoToH3(latitude, longitude, 15)
    candidates = index[H3.API.h3ToParent(ref, 6)]
    distances = H3.API.h3Distance.(ref, candidates)
    candidates[distances .== minimum(distances)]
end

"""
Finds up-leveled hexes that overlap with a ring around the specified point.

This is slightly tricky because the parent of a hex doesn't necessarily contain
the center of the hex.
"""
function uplevel(ref::UInt64; level, radius=2)
    ref |> x->kRing(x, radius) .|> h3ToGeo .|> x -> geoToH3(x, level)
end

"""
Finds up-leveled hexes that overlap with a ring around the specified point.
"""
function uplevel(ref::Int64; level, radius=2)
    uplevel(UInt64(ref), level; radius=radius)
end

"""
Finds up-leveled hexes that overlap with a ring around the specified points.
"""
function uplevel(refs::Vector; level::Int, radius=2)
    unique(reduce(vcat, uplevel.(refs, level, radius=radius)))
end

""" 
Finds the tiles that will hold the nearest grid points. Since we
know that the nearest grid point will be well less than 1000 grid steps
away, we could just form the set of h3 references within that distance
(called a k-ring) and figure out the set of tiles associated with any
of those points. That is a lot of computation, however. Instead, we
find the equivalent neighborhood at a coarser level of resolution and
then find the tiles for that neighborhood.

As a point of reference, the radius 1000 k-ring for a point has about
2-3 million elements while the level 10 k-ring with radius 7 covers 
very nearly the same area with fewer than 200 hexes and these hexes
can be used to find the interesting tiles far faster (84us versus 2.6
seconds) than the 2-3 million with almost precisely the same result.
"""
function findTiles(ref; radius=7, level=10, tileLevel=3)
    locality = uplevel(ref, 10, radius=2)
    uplevel(locality, 3, radius=10)
end


"""
Finds the closest grid points in an index to a specified lat/long pair

All grid points which are an equal grid distance from the original point
will be returned. Since we are using grid distance, there will likely
be more than one result and each should be used in querying.
"""
function getGridPoints(ref; level=6, basedir)
    upref = uplevel(ref, 8, radius=0)[1]
    r = []
    tiles = findTiles(ref)
    for tile in tiles
        fname = joinpath(basedir, "x-3-$tile")
        df = DataFrame(Parquet.Table(fname)) |>
            @mutate(dx = h3Distance(upref, uplevel(UInt64(get(_.h3)), 8, radius=0)[1])) |>
            @filter(10_000 > _.dx >= 0) |>
            @take(10) |>
            @mutate(tile=findTiles(get(_.h3))) |>
            DataFrame
        push!(r, df)
    end
    df = vcat(r...)
    df = df |> @orderby(_.dx) |> DataFrame
    df[!, :origin] .= ref
    df[!, :rank] = 1:size(df)[1]
    df
end

function nearRegion(ref::UInt64, radius::Int, level::Int)
    reduce(vcat, kRing.(reduce(vcat, h3ToParent.(kRing(ref, 2), level)), radius))
end

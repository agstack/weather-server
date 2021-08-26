using H3
using H3.Lib
using H3.Lib: H3Index, GeoCoord, Geofence, GeoPolygon
import H3.API.geoToH3
using Geodesy
using DataStructures

using Plots
using HexGeometry

# The goal here is to search a plausible sized grid for the nearest n point.
# The MRMS data has a grid 3500 x 7000 points which are roughly 1km apart
# Here, we will use a test grid that is evenly spaced in latitude from 20-50 degrees
# and longitude from -55 to -120 degrees

struct Location
    lat::Float64
    lon::Float64
end

"""
grid(res=(7000,3500), r=1000, maxResolution=11, minResolution=6)

Creates a grid of points with a specified number of points in east-west and
north-south directions (defaul 7000ew x 3500ns). Points will have a specified 
spacing (default 1000m = 1km).

In addition to locating points on a grid, an index will be created
so that points can be found using different scales of H3Index.
"""
function grid(;res=(7000,3500), r=1000.0, maxResolution=11, minResolution=6)
    center = (40.0,-95)
    # planar projection tangent to roughly the center of continental US
    tx = ENUfromLLA(LLA(center...,0), wgs84)
    ix = inv(tx)
    grid = Vector{Location}()
    index = DefaultDict{H3Index, Vector{Int32}}(Vector{Int32})
    x0, y0 = res ./ 2
    id = 0
    grid = Vector{Location}(undef, reduce(*, res .+ 1))
    for x = -x0:x0, y = -y0:y0
        id += 1
        px = ix(ENU(x*r, y*r, 0))
        grid[id] = Location(px.lat, px.lon)
        for resolution in minResolution:maxResolution
            hex = geoToH3(px.lat, px.lon, resolution)
            push!(index[hex], id)
        end
    end
    return grid, index
end


function nearest(query, grid, index, range)
    tx = ENUfromLLA(LLA(query...), wgs84)
    for res in max(range...):-1:min(range...)
        q = geoToH3(query..., res)
        r = [grid[z] for u in map(x -> i[x], H3.API.kRing(q, 1)) for z in u]
        if length(r) > 1
            p = map(x -> tx(LLA(x.lat, x.lon, 0.0)), r)
            distance = map(x->sum(x.^2), p)
            best = findfirst(distance .== reduce(min, distance))
            return [r[best], sqrt(distance[best])]
        end
    end
    return nothing
end



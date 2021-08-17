# examples of how to use polygons defined in GeoJSON filled with hexes for indexing and searching
using Plots
using GeoInterface
using LibGEOS
using H3
using H3.Lib
using H3.Lib: H3Index, GeoCoord, Geofence, GeoPolygon
import H3.API.geoToH3
using Geodesy

# common functions we need include:

"""
    geoToH3(p, resolution)

Converts a point p defined in terms of latitude and longitude in degrees into an H3Index.

# Example

geoToH3(LibGEOS.Point(42, -110), 6) => 0x08626b3cafffffff
"""
function geoToH3(p::AbstractPoint, resolution::Int) :: H3Index
    geoToH3(coordinates(p)..., resolution)
end

"""
    geoToH3(latitude, longitude, resolution)

Converts a point defined in terms of latitude and longitude in degrees into an H3Index.

# Example

julia> geoToH3(42, -110, 6)
0x08626b3cafffffff

julia> poly = LibGEOS.Polygon([[[0.0,0.0],[1,1],[2,0],[0,0]]])
julia> (x->geoToH3(x..., 5)).(coordinates(boundary(poly)))
4-element Vector{UInt64}:
 0x085754e67fffffff
 0x0857541affffffff
 0x0857542b7fffffff
 0x085754e67fffffff
"""
function geoToH3(latitude::Number, longitude::Number, resolution::Int) :: H3Index
    geoToH3(H3.API.GeoCoord(deg2rad(latitude), deg2rad(longitude)), resolution)
end

"""
    geoToH3([latitude, longitude], resolution)

Converts a point defined in terms of latitude and longitude in degrees into an H3Index.

# Example

julia> geoToH3(42, -110, 6)
0x08626b3cafffffff

julia> poly = LibGEOS.Polygon([[[0.0,0.0],[1,1],[2,0],[0,0]]])
julia> (x->geoToH3(x..., 5)).(coordinates(boundary(poly)))
4-element Vector{UInt64}:
 0x085754e67fffffff
 0x0857541affffffff
 0x0857542b7fffffff
 0x085754e67fffffff
"""
function geoToH3(point::Vector{Float64}, resolution::Int) :: H3Index
    geoToH3(point..., resolution)
end

"""
    h3ToPolygon(id)

Converts a single H3Index into the corresponding GeoPolygon in lat/long
"""
function h3ToPolygon(id::H3Index)
    px = map(p -> [rad2deg(p.lat), rad2deg(p.lon)], H3.API.h3ToGeoBoundary(id))
    return closedPolygon(px)
end

function closedPolygon(coordinates::Vector{Vector{Float64}}) :: AbstractPolygon
    push!(coordinates, coordinates[1])
    return LibGEOS.Polygon([coordinates])
end

"""
    h3ToPolygon(Vector{H3Index})

Converts a vector of hexagons (represented as H3Index values) into a polygon which 
is the union of all of the hexagons.
"""
function h3ToPolygon(ids::Vector{H3Index})
    return reduce(LibGEOS.union, h3ToPolygon.(ids))
end

"""
    polyfill(p::AbstractPolygon, resolution::Int)

Covers the polygon p (which is expressed in lat/long) with hexagons and returns
the list of H3Indexes for each. Note that this is only an approximate covering.
Some hexagons will extend outside the polygon being covered and not all of the 
polygon will be covered. 

To avoid undercoverage this function expands poly by an amount determined by the
cover parameter. The default value will result in a cover that rarely (if ever)
has any under-coverage. Increasing the setting to 2 will prevent any under-coverage
but will result in lots of overspray. Setting cover=0 will avoid all expansion, but will 
almost always result in significant under-coverage as well. At about cover=1.1, the 
undercoverage should be very small (<< 0.1%), but will still happen on occasion.

# Example
julia> p = LibGEOS.Polygon([[[40.0, -110.0], [41,-110], [41.4,-109], [40,-110]]])
julia> plot(h3ToPolygon(polyfill(p, 2)))
julia> plot!(h3ToPolygon(polyfill(p, 3)))
julia> plot!(h3ToPolygon(polyfill(p, 4)))
julia> plot!(h3ToPolygon(polyfill(p, 5)))
julia> plot!(p)
## Highlight under-coverage
julia> px = h3ToPolygon(polyfill(p, 4, cover=0)
julia> plot(p)
julia> plot(difference(p,px))
julia> plot(difference(px,p))
"""
function polyfill(poly::AbstractPolygon, resolution::Int; cover=1.2)::Vector{H3Index}
    cover = max(0.0, cover)
    if cover > 0.0
        # we expand by an amount related to the size of a hex edge
        fudge = cover * Lib.edgeLengthM(resolution)

        # this projection puts the first vertex of the polygon at the origin
        # and is expressed in meters [east, north, up] against the wgs84 ellipsoid
        origin = coordinates(boundary(poly))[1]
        tx = ENUfromLLA(LLA(origin...,0), wgs84)

        # expand our polygon projected form. We also simplify to avoid large number of vertices
        projected = map(x->tx(LLA(x..., 0))[1:2], coordinates(boundary(poly)))
        expanded = simplify(buffer(LibGEOS.Polygon([projected]), fudge), fudge/5)
        
        # project back
        coords = map(lla-> [lla.lat, lla.lon], map(x->inv(tx)(ENU(x..., 0)), coordinates(boundary(expanded))))
        poly = LibGEOS.Polygon([coords])
    end
    vertices = map(x->GeoCoord(map(deg2rad, x)...), coordinates(boundary(poly)))
    fence = Geofence(length(vertices), pointer(vertices))
    h3poly = GeoPolygon(fence, 0, C_NULL)
    hexagons = Lib.polyfill(Ref(h3poly), resolution)
    return filter(!iszero, hexagons)
end

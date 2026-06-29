package store

import (
	"math"
	"sort"
)

const (
	geoMaxLat = 85.05112878
	geoMinLat = -85.05112878
	geoMaxLon = 180.0
	geoMinLon = -180.0
	geoSteps  = 26 // 52-bit geohash
)

// GeoPoint represents a geographic coordinate.
type GeoPoint struct {
	Longitude float64
	Latitude  float64
}

// GeoMember represents a named geographic point for GeoAdd.
type GeoMember struct {
	Longitude float64
	Latitude  float64
	Name      string
}

// GeoResult is a search result with distance.
type GeoResult struct {
	Member    string
	Dist      float64
	Longitude float64
	Latitude  float64
	Hash      float64
}

// GeoEncode encodes a longitude/latitude pair into a float64 score suitable for sorted set storage.
func GeoEncode(lon, lat float64) float64 {
	// Interleave bits of quantized lon and lat
	lat = math.Max(geoMinLat, math.Min(geoMaxLat, lat))
	lon = math.Max(geoMinLon, math.Min(geoMaxLon, lon))

	latOff := (lat - geoMinLat) / (geoMaxLat - geoMinLat)
	lonOff := (lon - geoMinLon) / (geoMaxLon - geoMinLon)

	latQ := uint32(latOff * float64(1<<geoSteps))
	lonQ := uint32(lonOff * float64(1<<geoSteps))

	var hash uint64
	for i := geoSteps - 1; i >= 0; i-- {
		hash = (hash << 1) | uint64((lonQ>>uint(i))&1)
		hash = (hash << 1) | uint64((latQ>>uint(i))&1)
	}

	return float64(hash)
}

// GeoDecode decodes a geohash score back into longitude/latitude.
func GeoDecode(hash float64) (float64, float64) {
	h := uint64(hash)
	var lonQ, latQ uint32
	for i := geoSteps - 1; i >= 0; i-- {
		bit := 2 * i
		latQ |= uint32((h>>uint(bit))&1) << uint(i)
		lonQ |= uint32((h>>uint(bit+1))&1) << uint(i)
	}

	lon := float64(lonQ)/float64(1<<geoSteps)*(geoMaxLon-geoMinLon) + geoMinLon
	lat := float64(latQ)/float64(1<<geoSteps)*(geoMaxLat-geoMinLat) + geoMinLat
	return lon, lat
}

// GeoDistBetween computes the Haversine distance in meters between two points.
func GeoDistBetween(lon1, lat1, lon2, lat2 float64) float64 {
	const earthRadius = 6372797.560856 // meters

	lat1r := lat1 * math.Pi / 180
	lat2r := lat2 * math.Pi / 180
	dlat := (lat2 - lat1) * math.Pi / 180
	dlon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(dlat/2)*math.Sin(dlat/2) +
		math.Cos(lat1r)*math.Cos(lat2r)*math.Sin(dlon/2)*math.Sin(dlon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

// GeoSearch searches for members within a radius (meters) of a center point.
// It scans the sorted set and filters by distance.
func GeoSearch(members []ZMember, centerLon, centerLat, radius float64) []GeoResult {
	var results []GeoResult
	for _, m := range members {
		lon, lat := GeoDecode(m.Score)
		dist := GeoDistBetween(centerLon, centerLat, lon, lat)
		if dist <= radius {
			results = append(results, GeoResult{
				Member:    m.Member,
				Dist:      dist,
				Longitude: lon,
				Latitude:  lat,
				Hash:      m.Score,
			})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Dist < results[j].Dist
	})
	return results
}

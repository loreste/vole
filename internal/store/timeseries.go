package store

import (
	"math"
	"sort"
)

// TSSample represents a single time-series data point.
type TSSample struct {
	Timestamp int64   // Unix milliseconds
	Value     float64
}

// TimeSeries stores time-ordered samples with optional labels.
type TimeSeries struct {
	Samples []TSSample
	Labels  map[string]string
}

func NewTimeSeries() *TimeSeries {
	return &TimeSeries{
		Labels: make(map[string]string),
	}
}

// Add adds a sample. If timestamp already exists, the value is updated.
func (ts *TimeSeries) Add(timestamp int64, value float64) {
	// Insert in sorted order
	idx := sort.Search(len(ts.Samples), func(i int) bool {
		return ts.Samples[i].Timestamp >= timestamp
	})
	if idx < len(ts.Samples) && ts.Samples[idx].Timestamp == timestamp {
		// Update existing
		ts.Samples[idx].Value = value
		return
	}
	// Insert
	ts.Samples = append(ts.Samples, TSSample{})
	copy(ts.Samples[idx+1:], ts.Samples[idx:])
	ts.Samples[idx] = TSSample{Timestamp: timestamp, Value: value}
}

// Range returns samples between from and to (inclusive).
func (ts *TimeSeries) Range(from, to int64, count int) []TSSample {
	start := sort.Search(len(ts.Samples), func(i int) bool {
		return ts.Samples[i].Timestamp >= from
	})
	var results []TSSample
	for i := start; i < len(ts.Samples); i++ {
		if ts.Samples[i].Timestamp > to {
			break
		}
		results = append(results, ts.Samples[i])
		if count > 0 && len(results) >= count {
			break
		}
	}
	return results
}

// Last returns the latest sample.
func (ts *TimeSeries) Last() (TSSample, bool) {
	if len(ts.Samples) == 0 {
		return TSSample{}, false
	}
	return ts.Samples[len(ts.Samples)-1], true
}

// Len returns the number of samples.
func (ts *TimeSeries) Len() int { return len(ts.Samples) }

// Downsample aggregates samples into buckets.
func (ts *TimeSeries) Downsample(from, to, bucketSize int64, aggType string) []TSSample {
	if bucketSize <= 0 || len(ts.Samples) == 0 {
		return nil
	}

	var results []TSSample
	bucketStart := from

	for bucketStart < to {
		bucketEnd := bucketStart + bucketSize
		samples := ts.Range(bucketStart, bucketEnd-1, 0)

		if len(samples) > 0 {
			var val float64
			switch aggType {
			case "avg":
				sum := 0.0
				for _, s := range samples {
					sum += s.Value
				}
				val = sum / float64(len(samples))
			case "sum":
				for _, s := range samples {
					val += s.Value
				}
			case "min":
				val = math.Inf(1)
				for _, s := range samples {
					if s.Value < val {
						val = s.Value
					}
				}
			case "max":
				val = math.Inf(-1)
				for _, s := range samples {
					if s.Value > val {
						val = s.Value
					}
				}
			case "count":
				val = float64(len(samples))
			case "first":
				val = samples[0].Value
			case "last":
				val = samples[len(samples)-1].Value
			default:
				val = samples[0].Value
			}
			results = append(results, TSSample{Timestamp: bucketStart, Value: val})
		}

		bucketStart = bucketEnd
	}
	return results
}

package store

import (
	"hash/fnv"
	"math"
)

// mix64 applies a finalizer to improve bit distribution (splitmix64 style).
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

const (
	hllPrecision = 14 // 2^14 = 16384 registers
	hllRegisters = 1 << hllPrecision
	hllAlpha     = 0.7213 / (1 + 1.079/float64(hllRegisters))
)

// HyperLogLog implements the HyperLogLog probabilistic cardinality estimator.
type HyperLogLog struct {
	registers [hllRegisters]uint8
}

// NewHyperLogLog creates a new HyperLogLog instance.
func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{}
}

// Add adds an element. Returns true if the internal state changed.
func (h *HyperLogLog) Add(element string) bool {
	hasher := fnv.New64a()
	hasher.Write([]byte(element))
	x := mix64(hasher.Sum64())

	// Use first 14 bits as register index
	idx := x >> (64 - hllPrecision)
	// Count leading zeros in the remaining bits + 1
	remaining := (x << hllPrecision) | (1 << (hllPrecision - 1)) // ensure at least 1 bit set
	rho := uint8(clz64(remaining) + 1)

	if rho > h.registers[idx] {
		h.registers[idx] = rho
		return true
	}
	return false
}

// Count returns the estimated cardinality.
func (h *HyperLogLog) Count() int64 {
	// Harmonic mean of 2^(-register)
	var sum float64
	zeros := 0
	for _, val := range h.registers {
		sum += math.Pow(2, -float64(val))
		if val == 0 {
			zeros++
		}
	}

	estimate := hllAlpha * float64(hllRegisters) * float64(hllRegisters) / sum

	// Small range correction
	if estimate <= 2.5*float64(hllRegisters) && zeros > 0 {
		estimate = float64(hllRegisters) * math.Log(float64(hllRegisters)/float64(zeros))
	}

	return int64(math.Round(estimate))
}

// Registers returns a copy of the internal register array as a slice.
func (h *HyperLogLog) Registers() []uint8 {
	out := make([]uint8, hllRegisters)
	copy(out, h.registers[:])
	return out
}

// LoadRegisters restores the internal registers from a slice.
func (h *HyperLogLog) LoadRegisters(regs []uint8) {
	copy(h.registers[:], regs)
}

// Merge merges another HyperLogLog into this one. Returns true if state changed.
func (h *HyperLogLog) Merge(other *HyperLogLog) bool {
	changed := false
	for i := range h.registers {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
			changed = true
		}
	}
	return changed
}

// clz64 counts leading zeros in a uint64.
func clz64(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	var n uint8
	if x&0xFFFFFFFF00000000 == 0 {
		n += 32
		x <<= 32
	}
	if x&0xFFFF000000000000 == 0 {
		n += 16
		x <<= 16
	}
	if x&0xFF00000000000000 == 0 {
		n += 8
		x <<= 8
	}
	if x&0xF000000000000000 == 0 {
		n += 4
		x <<= 4
	}
	if x&0xC000000000000000 == 0 {
		n += 2
		x <<= 2
	}
	if x&0x8000000000000000 == 0 {
		n += 1
	}
	return n
}

package store

import (
	"sort"
)

// SortedSet maintains a set of members with scores, kept in sorted order
// by (score ASC, member ASC) for O(k) range queries.
type SortedSet struct {
	scores  map[string]float64
	ordered []ZMember // always sorted by (score, member)
}

func NewSortedSet() *SortedSet {
	return &SortedSet{scores: make(map[string]float64)}
}

func (z *SortedSet) Len() int { return len(z.scores) }

// Add inserts or updates a member. Returns true if the member is new.
func (z *SortedSet) Add(member string, score float64) bool {
	if oldScore, exists := z.scores[member]; exists {
		if oldScore == score {
			return false // no change
		}
		// Remove from old position
		z.removeOrdered(member, oldScore)
		// Insert at new position
		z.scores[member] = score
		z.insertOrdered(member, score)
		return false
	}
	z.scores[member] = score
	z.insertOrdered(member, score)
	return true
}

// Remove deletes a member. Returns true if it existed.
func (z *SortedSet) Remove(member string) bool {
	score, exists := z.scores[member]
	if !exists {
		return false
	}
	delete(z.scores, member)
	z.removeOrdered(member, score)
	return true
}

// Score returns the score for a member.
func (z *SortedSet) Score(member string) (float64, bool) {
	score, ok := z.scores[member]
	return score, ok
}

// Range returns members from index start to stop (inclusive), supporting
// negative indices (like Redis). Returns nil if range is empty.
func (z *SortedSet) Range(start, stop int) []ZMember {
	start, stop, ok := normalizeRange(start, stop, len(z.ordered))
	if !ok {
		return nil
	}
	out := make([]ZMember, stop-start+1)
	copy(out, z.ordered[start:stop+1])
	return out
}

// Members returns a copy of all members in sorted order.
func (z *SortedSet) Members() []ZMember {
	out := make([]ZMember, len(z.ordered))
	copy(out, z.ordered)
	return out
}

// findPos returns the insertion index for (score, member) using binary search.
func (z *SortedSet) findPos(score float64, member string) int {
	return sort.Search(len(z.ordered), func(i int) bool {
		if z.ordered[i].Score != score {
			return z.ordered[i].Score > score
		}
		return z.ordered[i].Member >= member
	})
}

func (z *SortedSet) insertOrdered(member string, score float64) {
	pos := z.findPos(score, member)
	z.ordered = append(z.ordered, ZMember{})
	copy(z.ordered[pos+1:], z.ordered[pos:])
	z.ordered[pos] = ZMember{Member: member, Score: score}
}

func (z *SortedSet) removeOrdered(member string, score float64) {
	pos := z.findPos(score, member)
	if pos < len(z.ordered) && z.ordered[pos].Member == member {
		z.ordered = append(z.ordered[:pos], z.ordered[pos+1:]...)
	}
}

// RangeByScore returns members with scores between min and max (inclusive).
// offset and count implement LIMIT; pass 0 for both to return all matches.
func (z *SortedSet) RangeByScore(min, max float64, offset, count int) []ZMember {
	start := sort.Search(len(z.ordered), func(i int) bool {
		return z.ordered[i].Score >= min
	})
	var result []ZMember
	skipped := 0
	for i := start; i < len(z.ordered); i++ {
		if z.ordered[i].Score > max {
			break
		}
		if offset > 0 && skipped < offset {
			skipped++
			continue
		}
		result = append(result, z.ordered[i])
		if count > 0 && len(result) >= count {
			break
		}
	}
	return result
}

// RevRange returns members from rank start to stop in reverse score order
// (index 0 is the element with the highest score).
func (z *SortedSet) RevRange(start, stop int) []ZMember {
	n := len(z.ordered)
	start, stop, ok := normalizeRange(start, stop, n)
	if !ok {
		return nil
	}
	out := make([]ZMember, stop-start+1)
	for i := 0; i < len(out); i++ {
		out[i] = z.ordered[n-1-start-i]
	}
	return out
}

// Rank returns the 0-based rank of member in ascending score order.
func (z *SortedSet) Rank(member string) (int, bool) {
	score, ok := z.scores[member]
	if !ok {
		return 0, false
	}
	pos := z.findPos(score, member)
	if pos < len(z.ordered) && z.ordered[pos].Member == member {
		return pos, true
	}
	return 0, false
}

// PopMin removes and returns the count lowest-scored members.
func (z *SortedSet) PopMin(count int) []ZMember {
	if count <= 0 {
		count = 1
	}
	if count > len(z.ordered) {
		count = len(z.ordered)
	}
	if count == 0 {
		return nil
	}
	result := make([]ZMember, count)
	copy(result, z.ordered[:count])
	for _, m := range result {
		delete(z.scores, m.Member)
	}
	z.ordered = z.ordered[count:]
	return result
}

// PopMax removes and returns the count highest-scored members (highest first).
func (z *SortedSet) PopMax(count int) []ZMember {
	if count <= 0 {
		count = 1
	}
	n := len(z.ordered)
	if count > n {
		count = n
	}
	if count == 0 {
		return nil
	}
	start := n - count
	result := make([]ZMember, count)
	copy(result, z.ordered[start:])
	// Reverse so highest score comes first
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	for _, m := range result {
		delete(z.scores, m.Member)
	}
	z.ordered = z.ordered[:start]
	return result
}

// RangeByLex returns members with names lexicographically between min and max.
// min/max follow Redis lex syntax: "[a", "(a", "-", "+"
func (z *SortedSet) RangeByLex(min, max string, offset, count int) []ZMember {
	var result []ZMember
	skipped := 0
	for _, m := range z.ordered {
		if !lexGTE(m.Member, min) {
			continue
		}
		if !lexLTE(m.Member, max) {
			break
		}
		if offset > 0 && skipped < offset {
			skipped++
			continue
		}
		result = append(result, m)
		if count > 0 && len(result) >= count {
			break
		}
	}
	return result
}

func lexGTE(member, bound string) bool {
	if bound == "-" {
		return true
	}
	if bound == "+" {
		return false
	}
	if len(bound) > 0 && bound[0] == '[' {
		return member >= bound[1:]
	}
	if len(bound) > 0 && bound[0] == '(' {
		return member > bound[1:]
	}
	return member >= bound
}

func lexLTE(member, bound string) bool {
	if bound == "+" {
		return true
	}
	if bound == "-" {
		return false
	}
	if len(bound) > 0 && bound[0] == '[' {
		return member <= bound[1:]
	}
	if len(bound) > 0 && bound[0] == '(' {
		return member < bound[1:]
	}
	return member <= bound
}

// CountByScore counts members with scores between min and max (inclusive).
func (z *SortedSet) CountByScore(min, max float64) int {
	start := sort.Search(len(z.ordered), func(i int) bool {
		return z.ordered[i].Score >= min
	})
	n := 0
	for i := start; i < len(z.ordered); i++ {
		if z.ordered[i].Score > max {
			break
		}
		n++
	}
	return n
}

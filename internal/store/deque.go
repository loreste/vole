package store

// Deque is a double-ended queue backed by a ring buffer.
type Deque struct {
	buf   []string
	head  int
	count int
}

const dequeMinCap = 8

// Len returns the number of elements in the deque.
func (d *Deque) Len() int {
	return d.count
}

// grow doubles the buffer capacity and re-linearizes the elements.
func (d *Deque) grow() {
	newCap := len(d.buf) * 2
	if newCap < dequeMinCap {
		newCap = dequeMinCap
	}
	newBuf := make([]string, newCap)
	for i := 0; i < d.count; i++ {
		newBuf[i] = d.buf[(d.head+i)%len(d.buf)]
	}
	d.buf = newBuf
	d.head = 0
}

// PushFront adds elements to the front of the deque one at a time (left to
// right), so the last value ends up at the very front -- matching Redis LPUSH
// semantics. O(1) amortized per element.
func (d *Deque) PushFront(vals ...string) {
	for _, v := range vals {
		if d.count == len(d.buf) {
			d.grow()
		}
		d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
		d.buf[d.head] = v
		d.count++
	}
}

// PushBack adds elements to the back of the deque. O(1) amortized per element.
func (d *Deque) PushBack(vals ...string) {
	for _, v := range vals {
		if d.count == len(d.buf) {
			d.grow()
		}
		d.buf[(d.head+d.count)%len(d.buf)] = v
		d.count++
	}
}

// PopFront removes and returns the front element. O(1).
func (d *Deque) PopFront() (string, bool) {
	if d.count == 0 {
		return "", false
	}
	val := d.buf[d.head]
	d.buf[d.head] = "" // clear reference
	d.head = (d.head + 1) % len(d.buf)
	d.count--
	return val, true
}

// PopBack removes and returns the back element. O(1).
func (d *Deque) PopBack() (string, bool) {
	if d.count == 0 {
		return "", false
	}
	idx := (d.head + d.count - 1) % len(d.buf)
	val := d.buf[idx]
	d.buf[idx] = "" // clear reference
	d.count--
	return val, true
}

// at returns the element at logical index i.
func (d *Deque) at(i int) string {
	return d.buf[(d.head+i)%len(d.buf)]
}

// Range returns elements from logical index start to stop (inclusive),
// using the same negative-index semantics as Redis LRANGE.
func (d *Deque) Range(start, stop int) []string {
	start, stop, ok := normalizeRange(start, stop, d.count)
	if !ok {
		return nil
	}
	out := make([]string, stop-start+1)
	for i := start; i <= stop; i++ {
		out[i-start] = d.at(i)
	}
	return out
}

// Get returns the element at logical index i. Caller must ensure 0 <= i < count.
func (d *Deque) Get(i int) string {
	return d.buf[(d.head+i)%len(d.buf)]
}

// Set replaces the element at logical index i. Caller must ensure 0 <= i < count.
func (d *Deque) Set(i int, val string) {
	d.buf[(d.head+i)%len(d.buf)] = val
}

// Insert inserts val at logical index i, shifting subsequent elements right.
// Caller must ensure 0 <= i <= count.
func (d *Deque) Insert(i int, val string) {
	if d.count == len(d.buf) {
		d.grow()
	}
	if i <= d.count/2 {
		// Shift left portion left
		d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
		for j := 0; j < i; j++ {
			src := (d.head + 1 + j) % len(d.buf)
			dst := (d.head + j) % len(d.buf)
			d.buf[dst] = d.buf[src]
		}
	} else {
		// Shift right portion right
		for j := d.count; j > i; j-- {
			src := (d.head + j - 1) % len(d.buf)
			dst := (d.head + j) % len(d.buf)
			d.buf[dst] = d.buf[src]
		}
	}
	d.buf[(d.head+i)%len(d.buf)] = val
	d.count++
}

// ToSlice returns a linearized copy of all elements.
func (d *Deque) ToSlice() []string {
	if d.count == 0 {
		return nil
	}
	out := make([]string, d.count)
	for i := 0; i < d.count; i++ {
		out[i] = d.at(i)
	}
	return out
}

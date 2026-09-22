package pipeline

// tx is the buffered transaction handed to a Processor.
//
// The read baseline is the committed state snapshot captured at the start of
// an attempt; buffered puts/deletes override it for reads within the same
// attempt. Nothing is written anywhere until the processor succeeds and the
// store commits the whole mutation atomically with the checkpoint, so a
// failed attempt cannot leave a partial business effect.
type tx struct {
	base    map[string]string
	puts    map[string][]byte
	deletes []string
	skip    bool
}

func newTx(base map[string]string) *tx {
	return &tx{
		base: base,
		puts: make(map[string][]byte),
	}
}

func (t *tx) Get(key string) ([]byte, bool) {
	if v, ok := t.puts[key]; ok {
		return append([]byte(nil), v...), true
	}
	for _, d := range t.deletes {
		if d == key {
			return nil, false
		}
	}
	v, ok := t.base[key]
	if !ok {
		return nil, false
	}
	return []byte(v), true
}

func (t *tx) Put(key string, value []byte) {
	// A Put cancels a prior Delete within the same attempt.
	filtered := t.deletes[:0]
	for _, d := range t.deletes {
		if d != key {
			filtered = append(filtered, d)
		}
	}
	t.deletes = filtered
	t.puts[key] = append([]byte(nil), value...)
}

func (t *tx) Delete(key string) {
	if _, ok := t.puts[key]; ok {
		delete(t.puts, key)
	}
	for _, d := range t.deletes {
		if d == key {
			return
		}
	}
	t.deletes = append(t.deletes, key)
}

func (t *tx) Skip() { t.skip = true }

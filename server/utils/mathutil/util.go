package mathutil

func MaxUint64(a, b uint64) uint64 {
	if a < b {
		return b
	}
	return a
}

func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

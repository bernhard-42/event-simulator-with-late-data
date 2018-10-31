package randpool

import (
	"fmt"
	"math/rand"
)

// RandPool provides a pool of random and normally distributed randum numbers
type RandPool struct {
	vals      []float64
	index     int
	normVals  []float64
	normIndex int
}

func (r *RandPool) pop(norm bool) (rnd float64) {
	if norm {
		if r.index == len(r.vals) {
			fmt.Println("Random pool for NormFloat64 exhausted, starting from 0 again")
			r.normIndex = 0
		}
		rnd = r.normVals[r.normIndex]
		r.normIndex++
	} else {
		if r.index == len(r.vals) {
			fmt.Println("Random pool for Float64 exhausted, starting from 0 again")
			r.index = 0
		}
		rnd = r.vals[r.index]
		r.index++
	}
	return
}

// Create creates a pool of random numbers
func Create(numVals int, numNormVals int) *RandPool {
	rp := RandPool{
		make([]float64, numVals),
		0,
		make([]float64, numNormVals),
		0,
	}

	for i := 0; i < numVals; i++ {
		rp.vals[i] = rand.Float64()
	}
	for i := 0; i < numNormVals; i++ {
		rp.normVals[i] = rand.NormFloat64()
	}
	return &rp
}

// Intn returns the next random int from the pool
// (not concurrency safe, use from one groutine only)
func (r *RandPool) Intn(n int) int {
	return int(r.pop(false) * float64(n))
}

// Float64 returns the next random float64 from the pool
// (not concurrency safe, use from one groutine only)
func (r *RandPool) Float64() float64 {
	return r.pop(false)
}

// NormFloat64 returns the next normally distributed random float64 from the pool
// not concurrency safe, use from one groutine only)
func (r *RandPool) NormFloat64() float64 {
	return r.pop(true)
}

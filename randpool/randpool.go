package main

import (
	"math/rand"

	log "github.com/sirupsen/logrus"
)

// RandPool provides a pool of random and normally distributed randum numbers
type RandPool struct {
	vals      []float64
	normVals  []float64
	index     int
	normIndex int
}

// CreateRandPool creates a pool of random numbers
func CreateRandPool(numVals int, numNormVals int) *RandPool {
	rp := RandPool{
		make([]float64, numVals),
		make([]float64, numNormVals),
		0,
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
	i := int(r.vals[r.index] * float64(n))
	if r.index < len(r.vals)-1 {
		r.index++
	} else {
		log.Warn("Random pool for Int exhausted, starting from 0 again")
		r.index = 0
	}
	return i
}

// Float64 returns the next random float64 from the pool
// (not concurrency safe, use from one groutine only)
func (r *RandPool) Float64() float64 {
	f := r.vals[r.index]
	if r.index < len(r.vals)-1 {
		r.index++
	} else {
		log.Warn("Random pool for Float64 exhausted, starting from 0 again")
		r.index = 0
	}
	return f
}

// NormFloat64 returns the next normally distributed random float64 from the pool
// not concurrency safe, use from one groutine only)
func (r *RandPool) NormFloat64() float64 {
	f := r.normVals[r.normIndex]
	if r.normIndex < len(r.normVals)-1 {
		r.normIndex++
	} else {
		log.Warn("Random pool for NormFloat64 exhausted, starting from 0 again")
		r.normIndex = 0
	}
	return f
}

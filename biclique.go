package pilosa

import "fmt"

type PairSlice []BitmapPair

type Biclique struct {
	Tiles []uint64
	Count uint64 // number of profiles
	Score uint64 // num tiles * Count
}

type BCList []Biclique

func (bcl BCList) Len() int {
	return len(bcl)
}
func (bcl BCList) Less(i, j int) bool {
	return bcl[i].Score > bcl[j].Score
}

func (bcl BCList) Swap(i, j int) {
	bcl[i], bcl[j] = bcl[j], bcl[i]
}

func (f *Fragment) MaxBiclique(n int) []Biclique {
	f.mu.Lock()
	f.cache.Invalidate()
	pairs := f.cache.Top() // slice of bitmapPairs
	f.mu.Unlock()

	topPairs := pairs
	if n < len(pairs) {
		topPairs = pairs[:n]
	}

	return maxBiclique(topPairs)
}

func maxBiclique(topPairs []BitmapPair) []Biclique {
	// generate every permutation of topPairs
	pairChan := make(chan PairSlice, 10)
	ps := PairSlice(topPairs)
	go generateCombinations(ps, pairChan)
	var minCount uint64 = 1

	results := make([]Biclique, 100)
	i := 0

	for comb := range pairChan {
		fmt.Println("Got a combination! ", comb)
		// feed each to intersectPairs
		ret := intersectPairs(comb)
		if ret.Count() > minCount {
			tiles := getTileIDs(comb)
			results[i] = Biclique{
				Tiles: tiles,
				Count: ret.Count(),
				Score: uint64(len(tiles)) * ret.Count(),
			}
			i++
			if i > 99 {
				break
			}
		}
	}
	return results
}

func getTileIDs(pairs PairSlice) []uint64 {
	tileIDs := make([]uint64, len(pairs))
	for i := 0; i < len(pairs); i++ {
		tileIDs[i] = pairs[i].ID
	}
	return tileIDs
}

func generateCombinations(pairs PairSlice, pairChan chan<- PairSlice) {
	gcombs(pairs, pairChan)
	close(pairChan)
}

func gcombs(pairs PairSlice, pairChan chan<- PairSlice) {
	fmt.Println("gcombs, send to pairChan ", pairs)

	pairChan <- pairs

	if len(pairs) == 1 {
		return
	}
	for i := 0; i < len(pairs); i++ {
		pairscopy := make(PairSlice, len(pairs))
		copy(pairscopy, pairs)
		ps := append(pairscopy[:i], pairscopy[i+1:]...)

		gcombs(ps, pairChan)
	}
}

// intersectPairs generates a bitmap which represents all profiles which have all of the tiles in pairs
func intersectPairs(pairs []BitmapPair) *Bitmap {
	result := pairs[0].Bitmap.Clone()
	for i := 1; i < len(pairs); i++ {
		result = result.Intersect(pairs[i].Bitmap)
	}
	result.SetCount(result.BitCount())
	return result
}

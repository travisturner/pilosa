package pilosa

import "fmt"

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

	results := make(chan []BitmapPair, 100)
	go func() {
		bicliqueFind(topPairs, nil, []BitmapPair{}, topPairs, []BitmapPair{}, results)
		close(results)
	}()

	// read results and build []Biclique
	bicliques := []Biclique{}
	for bmPairs := range results {
		tiles := getTileIDs(bmPairs)
		bicliqueBitmap := intersectPairs(bmPairs)
		bicliques = append(bicliques,
			Biclique{
				Tiles: tiles,
				Count: bicliqueBitmap.Count(),
				Score: uint64(len(tiles)) * bicliqueBitmap.Count(),
			})
	}
	return bicliques
}

func maxBiclique(topPairs []BitmapPair) []Biclique {
	// generate every permutation of topPairs
	pairChan := make(chan []BitmapPair, 10)
	ps := []BitmapPair(topPairs)
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

func bicliqueFind(G []BitmapPair, L *Bitmap, R []BitmapPair, P []BitmapPair, Q []BitmapPair, results chan []BitmapPair) {
	// G is topPairs
	// L should start with all bits set (L == U) (it will actually start nil, and we'll special case it below)
	// R starts empty
	// P starts as topPairs (all tiles are candidates)
	// Q starts empty

	for len(P) > 0 {
		// P ← P\{x};
		x := P[0]
		P = P[1:]

		// R ← R ∪ {x};
		newR := append(R, x)

		//  L' ← {u ∈ L | (u, x) ∈ E(G)};
		var newL *Bitmap
		if L == nil {
			newL = x.Bitmap.Clone()
		} else {
			newL = L.Clone()
		}
		newL = newL.Intersect(x.Bitmap)
		newLcnt := newL.BitCount()

		// P' ← ∅; Q' ← ∅;
		newP := []BitmapPair{}
		newQ := []BitmapPair{}

		// Check maximality.
		isMaximal := true
		for _, v := range Q {
			// get the neighbors of v in L'
			neighbors := v.Bitmap.Intersect(newL)
			ncnt := neighbors.BitCount()
			// Observation 4: end of branch
			if ncnt == newLcnt {
				isMaximal = false
				break
			} else if ncnt > 0 {
				newQ = append(newQ, v)
			}
		}

		if isMaximal {
			for _, v := range P {
				// get the neighbors of v in L'
				neighbors := v.Bitmap.Intersect(newL)
				ncnt := neighbors.BitCount()
				// Observation 3: expand to maximal
				if ncnt == newLcnt {
					newR = append(newR, v)
				} else if ncnt > 0 {
					// keep vertice adjacent to some vertex in newL
					newP = append(newP, v)
				}
			}
			// report newR as maximal biclique
			results <- newR
			if len(newP) > 0 {
				bicliqueFind(G, newL, newR, newP, newQ, results)
			}
		}
		Q = append(Q, x)
	}
}

func getTileIDs(pairs []BitmapPair) []uint64 {
	tileIDs := make([]uint64, len(pairs))
	for i := 0; i < len(pairs); i++ {
		tileIDs[i] = pairs[i].ID
	}
	return tileIDs
}

func generateCombinations(pairs []BitmapPair, pairChan chan<- []BitmapPair) {
	gcombs(pairs, pairChan)
	close(pairChan)
}

func gcombs(pairs []BitmapPair, pairChan chan<- []BitmapPair) {
	fmt.Println("gcombs, send to pairChan ", pairs)

	pairChan <- pairs

	if len(pairs) == 1 {
		return
	}
	for i := 0; i < len(pairs); i++ {
		pairscopy := make([]BitmapPair, len(pairs))
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

// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/roaring"
)

// Ensure a bsiGroup can adjust to its baseValue.
func TestBSIGroup_BaseValue(t *testing.T) {
	b0 := &bsiGroup{
		Name: "b0",
		Type: bsiGroupTypeInt,
		Min:  -100,
		Max:  900,
	}
	b1 := &bsiGroup{
		Name: "b1",
		Type: bsiGroupTypeInt,
		Min:  0,
		Max:  1000,
	}

	b2 := &bsiGroup{
		Name: "b2",
		Type: bsiGroupTypeInt,
		Min:  100,
		Max:  1100,
	}

	t.Run("Normal Condition", func(t *testing.T) {

		for _, tt := range []struct {
			f             *bsiGroup
			op            pql.Token
			val           int64
			expBaseValue  uint64
			expOutOfRange bool
		}{
			// LT
			{b0, pql.LT, 5, 105, false},
			{b0, pql.LT, -8, 92, false},
			{b0, pql.LT, -108, 0, true},
			{b0, pql.LT, 1005, 1000, false},
			{b0, pql.LT, 0, 100, false},

			{b1, pql.LT, 5, 5, false},
			{b1, pql.LT, -8, 0, true},
			{b1, pql.LT, 1005, 1000, false},
			{b1, pql.LT, 0, 0, false},

			{b2, pql.LT, 5, 0, true},
			{b2, pql.LT, -8, 0, true},
			{b2, pql.LT, 105, 5, false},
			{b2, pql.LT, 1105, 1000, false},

			// GT
			{b0, pql.GT, -105, 0, false},
			{b0, pql.GT, 5, 105, false},
			{b0, pql.GT, 905, 0, true},
			{b0, pql.GT, 0, 100, false},

			{b1, pql.GT, 5, 5, false},
			{b1, pql.GT, -8, 0, false},
			{b1, pql.GT, 1005, 0, true},
			{b1, pql.GT, 0, 0, false},

			{b2, pql.GT, 5, 0, false},
			{b2, pql.GT, -8, 0, false},
			{b2, pql.GT, 105, 5, false},
			{b2, pql.GT, 1105, 0, true},

			// EQ
			{b0, pql.EQ, -105, 0, true},
			{b0, pql.EQ, 5, 105, false},
			{b0, pql.EQ, 905, 0, true},
			{b0, pql.EQ, 0, 100, false},

			{b1, pql.EQ, 5, 5, false},
			{b1, pql.EQ, -8, 0, true},
			{b1, pql.EQ, 1005, 0, true},
			{b1, pql.EQ, 0, 0, false},

			{b2, pql.EQ, 5, 0, true},
			{b2, pql.EQ, -8, 0, true},
			{b2, pql.EQ, 105, 5, false},
			{b2, pql.EQ, 1105, 0, true},
		} {
			bv, oor := tt.f.baseValue(tt.op, tt.val)
			if oor != tt.expOutOfRange {
				t.Fatalf("baseValue calculation on %s op %s, expected outOfRange %v, got %v", tt.f.Name, tt.op, tt.expOutOfRange, oor)
			} else if !reflect.DeepEqual(bv, tt.expBaseValue) {
				t.Fatalf("baseValue calculation on %s, expected value %v, got %v", tt.f.Name, tt.expBaseValue, bv)
			}
		}
	})

	t.Run("Betwween Condition", func(t *testing.T) {
		for _, tt := range []struct {
			f               *bsiGroup
			predMin         int64
			predMax         int64
			expBaseValueMin uint64
			expBaseValueMax uint64
			expOutOfRange   bool
		}{

			{b0, -205, -105, 0, 0, true},
			{b0, -105, 80, 0, 180, false},
			{b0, 5, 20, 105, 120, false},
			{b0, 20, 1005, 120, 1000, false},
			{b0, 1005, 2000, 0, 0, true},

			{b1, -105, -5, 0, 0, true},
			{b1, -5, 20, 0, 20, false},
			{b1, 5, 20, 5, 20, false},
			{b1, 20, 1005, 20, 1000, false},
			{b1, 1005, 2000, 0, 0, true},

			{b2, 5, 95, 0, 0, true},
			{b2, 95, 120, 0, 20, false},
			{b2, 105, 120, 5, 20, false},
			{b2, 120, 1105, 20, 1000, false},
			{b2, 1105, 2000, 0, 0, true},
		} {
			min, max, oor := tt.f.baseValueBetween(tt.predMin, tt.predMax)
			if oor != tt.expOutOfRange {
				t.Fatalf("baseValueBetween calculation on %s, expected outOfRange %v, got %v", tt.f.Name, tt.expOutOfRange, oor)
			} else if !reflect.DeepEqual(min, tt.expBaseValueMin) || !reflect.DeepEqual(max, tt.expBaseValueMax) {
				t.Fatalf("baseValueBetween calculation on %s, expected min/max %v/%v, got %v/%v", tt.f.Name, tt.expBaseValueMin, tt.expBaseValueMax, min, max)
			}
		}
	})
}

// Ensure field can open and retrieve a view.
func TestField_DeleteView(t *testing.T) {
	f := MustOpenField(OptFieldTypeDefault())
	defer f.Close()

	viewName := viewStandard + "_v"

	// Create view.
	view, err := f.createViewIfNotExists(viewName)
	if err != nil {
		t.Fatal(err)
	} else if view == nil {
		t.Fatal("expected view")
	}

	err = f.deleteView(viewName)
	if err != nil {
		t.Fatal(err)
	}

	if f.view(viewName) != nil {
		t.Fatal("view still exists in field")
	}

	// Recreate view with same name, verify that the old view was not reused.
	view2, err := f.createViewIfNotExists(viewName)
	if err != nil {
		t.Fatal(err)
	} else if view == view2 {
		t.Fatal("failed to create new view")
	}
}

// TestField represents a test wrapper for Field.
type TestField struct {
	*Field
}

// NewTestField returns a new instance of TestField d/0.
func NewTestField(opts FieldOption) *TestField {
	path, err := ioutil.TempDir("", "pilosa-field-")
	if err != nil {
		panic(err)
	}
	field, err := NewField(path, "i", "f", opts)
	if err != nil {
		panic(err)
	}
	return &TestField{Field: field}
}

// MustOpenField returns a new, opened field at a temporary path. Panic on error.
func MustOpenField(opts FieldOption) *TestField {
	f := NewTestField(opts)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the field and removes the underlying data.
func (f *TestField) Close() error {
	defer os.RemoveAll(f.Path())
	return f.Field.Close()
}

// Reopen closes the index and reopens it.
func (f *TestField) Reopen() error {
	var err error
	if err := f.Field.Close(); err != nil {
		return err
	}

	path, index, name := f.Path(), f.Index(), f.Name()
	f.Field, err = NewField(path, index, name, OptFieldTypeDefault())
	if err != nil {
		return err
	}

	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

func (f *TestField) MustSetBit(row, col uint64, ts ...time.Time) {
	if len(ts) == 0 {
		_, err := f.Field.SetBit(row, col, nil)
		if err != nil {
			panic(err)
		}
	}
	for _, t := range ts {
		_, err := f.Field.SetBit(row, col, &t)
		if err != nil {
			panic(err)
		}
	}
}

// Ensure field can open and retrieve a view.
func TestField_CreateViewIfNotExists(t *testing.T) {
	f := MustOpenField(OptFieldTypeDefault())
	defer f.Close()

	// Create view.
	view, err := f.createViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view == nil {
		t.Fatal("expected view")
	}

	// Retrieve existing view.
	view2, err := f.createViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view != view2 {
		t.Fatal("view mismatch")
	}

	if view != f.view("v") {
		t.Fatal("view mismatch")
	}
}

func TestField_SetTimeQuantum(t *testing.T) {
	f := MustOpenField(OptFieldTypeTime(TimeQuantum("")))
	defer f.Close()

	// Set & retrieve time quantum.
	if err := f.setTimeQuantum(TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload field and verify that it is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
	}
}

func TestField_RowTime(t *testing.T) {
	f := MustOpenField(OptFieldTypeTime(TimeQuantum("")))
	defer f.Close()

	if err := f.setTimeQuantum(TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	}

	f.MustSetBit(1, 1, time.Date(2010, time.January, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(1, 2, time.Date(2011, time.January, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(1, 3, time.Date(2010, time.February, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(1, 4, time.Date(2010, time.January, 6, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(1, 5, time.Date(2010, time.January, 5, 13, 0, 0, 0, time.UTC))

	if r, err := f.RowTime(1, time.Date(2010, time.November, 5, 12, 0, 0, 0, time.UTC), "Y"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{1, 3, 4, 5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(1, time.Date(2010, time.February, 7, 13, 0, 0, 0, time.UTC), "YM"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{3}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(1, time.Date(2010, time.February, 7, 13, 0, 0, 0, time.UTC), "M"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{3}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(1, time.Date(2010, time.January, 5, 12, 0, 0, 0, time.UTC), "MD"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{1, 5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(1, time.Date(2010, time.January, 5, 13, 0, 0, 0, time.UTC), "MDH"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

}

func TestField_PersistAvailableShards(t *testing.T) {
	f := MustOpenField(OptFieldTypeDefault())

	// bm represents remote available shards.
	bm := roaring.NewBitmap(1, 2, 3)

	if err := f.AddRemoteAvailableShards(bm); err != nil {
		t.Fatal(err)
	}

	// Reload field and verify that shard data is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), bm.Slice()) {
		t.Fatalf("unexpected available shards (reopen). expected: %v, but got: %v", bm.Slice(), f.remoteAvailableShards.Slice())
	}

}

// Ensure that persisting available shards having a smaller footprint (for example,
// when going from a bitmap to a smaller, RLE representation) succeeds.
func TestField_PersistAvailableShardsFootprint(t *testing.T) {
	f := MustOpenField(OptFieldTypeDefault())

	// bm represents remote available shards.
	bm := roaring.NewBitmap()
	for i := uint64(0); i < 1204; i += 2 {
		bm.Add(i)
	}

	if err := f.AddRemoteAvailableShards(bm); err != nil {
		t.Fatal(err)
	}

	// Reload field and verify that shard data is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), bm.Slice()) {
		t.Fatalf("unexpected available shards (reopen). expected: %v, but got: %v", bm.Slice(), f.remoteAvailableShards.Slice())
	}

	bm1 := roaring.NewBitmap()
	for i := uint64(1); i < 1204; i += 2 {
		bm1.Add(i)
	}

	if err := f.AddRemoteAvailableShards(bm1); err != nil {
		t.Fatal(err)
	}

	// Reload field and verify that shard data is persisted.
	result := bm.Union(bm1)
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), result.Slice()) {
		t.Fatalf("unexpected available shards (reopen). expected: %v, but got: %v", bm.Slice(), f.remoteAvailableShards.Slice())
	}

}

// Ensure that ploat64 values are generated correctly.
func TestField_Ploat64(t *testing.T) {
	tests := []struct {
		flt   float64
		sig   uint64
		shift uint
		neg   bool
		err   string
	}{
		{7.0e+0, 7881299347898368, 1026, false, ""},
		{-1.12e+308, 5611671408025124, 2047, true, ""},
		{1.12e-292, 5033540777614485, 54, false, ""},
		{flt: math.NaN(), err: "float provided is NaN"},
		{flt: math.Inf(0), err: "float provided is infinity"},

		// The following came from ftoa.go tests: https://gist.github.com/rsc/1057873
		{math.Ldexp(5201988407066741, -824), 5201988407066741, 252, false, ""},
		{math.Ldexp(6406892948269899, 237), 6406892948269899, 1313, false, ""},
		{math.Ldexp(8431154198732492, 72), 8431154198732492, 1148, false, ""},
		{math.Ldexp(6475049196144587, 99), 6475049196144587, 1175, false, ""},
		{math.Ldexp(8274307542972842, 726), 8274307542972842, 1802, false, ""},
		{math.Ldexp(5381065484265332, -456), 5381065484265332, 620, false, ""},
		{math.Ldexp(6761728585499734, -1057), 6761728585499734, 19, false, ""},
		{math.Ldexp(7976538478610756, 376), 7976538478610756, 1452, false, ""},
		{math.Ldexp(5982403858958067, 377), 5982403858958067, 1453, false, ""},
		{math.Ldexp(5536995190630837, 93), 5536995190630837, 1169, false, ""},
		{math.Ldexp(7225450889282194, 710), 7225450889282194, 1786, false, ""},
		{math.Ldexp(7225450889282194, 709), 7225450889282194, 1785, false, ""},
		{math.Ldexp(8703372741147379, 117), 8703372741147379, 1193, false, ""},
		{math.Ldexp(8944262675275217, -1001), 8944262675275217, 75, false, ""},
		{math.Ldexp(7459803696087692, -707), 7459803696087692, 369, false, ""},
		{math.Ldexp(6080469016670379, -381), 6080469016670379, 695, false, ""},
		{math.Ldexp(8385515147034757, 721), 8385515147034757, 1797, false, ""},
		{math.Ldexp(7514216811389786, -828), 7514216811389786, 248, false, ""},
		{math.Ldexp(8397297803260511, -345), 8397297803260511, 731, false, ""},
		{math.Ldexp(6733459239310543, 202), 6733459239310543, 1278, false, ""},
		{math.Ldexp(8091450587292794, -473), 8091450587292794, 603, false, ""},
		{math.Ldexp(6567258882077402, 952), 6567258882077402, 2028, false, ""},
		{math.Ldexp(6712731423444934, 535), 6712731423444934, 1611, false, ""},
		{math.Ldexp(6712731423444934, 534), 6712731423444934, 1610, false, ""},
		{math.Ldexp(5298405411573037, -957), 5298405411573037, 119, false, ""},
		{math.Ldexp(5137311167659507, -144), 5137311167659507, 932, false, ""},
		{math.Ldexp(6722280709661868, 363), 6722280709661868, 1439, false, ""},
		{math.Ldexp(5344436398034927, -169), 5344436398034927, 907, false, ""},
		{math.Ldexp(8369123604277281, -853), 8369123604277281, 223, false, ""},
		{math.Ldexp(8995822108487663, -780), 8995822108487663, 296, false, ""},
		{math.Ldexp(8942832835564782, -383), 8942832835564782, 693, false, ""},
		{math.Ldexp(8942832835564782, -384), 8942832835564782, 692, false, ""},
		{math.Ldexp(8942832835564782, -385), 8942832835564782, 691, false, ""},
		{math.Ldexp(6965949469487146, -249), 6965949469487146, 827, false, ""},
		{math.Ldexp(6965949469487146, -250), 6965949469487146, 826, false, ""},
		{math.Ldexp(6965949469487146, -251), 6965949469487146, 825, false, ""},
		{math.Ldexp(7487252720986826, 548), 7487252720986826, 1624, false, ""},
		{math.Ldexp(5592117679628511, 164), 5592117679628511, 1240, false, ""},
		{math.Ldexp(8887055249355788, 665), 8887055249355788, 1741, false, ""},
		{math.Ldexp(6994187472632449, 690), 6994187472632449, 1766, false, ""},
		{math.Ldexp(8797576579012143, 588), 8797576579012143, 1664, false, ""},
		{math.Ldexp(7363326733505337, 272), 7363326733505337, 1348, false, ""},
		{math.Ldexp(8549497411294502, -448), 8549497411294502, 628, false, ""},

		{12345000, 6627671408640000, 1047, false, ""},
	}
	for i, test := range tests {
		// Generate ploat64 based on float64.
		plt, err := newPloat64(test.flt)
		if test.err != "" {
			if !strings.Contains(err.Error(), test.err) {
				t.Errorf("test %d expected error: %s, but got: %s", i, test.err, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		} else if plt.sig != test.sig {
			t.Errorf("test %d expected sig: %v, but got: %v", i, test.sig, plt.sig)
		} else if plt.shift != test.shift {
			t.Errorf("test %d expected shift: %v, but got: %v", i, test.shift, plt.shift)
		} else if plt.neg != test.neg {
			t.Errorf("test %d expected neg: %v, but got: %v", i, test.neg, plt.neg)
		}
	}
}

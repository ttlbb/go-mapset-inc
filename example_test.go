package mapsetinc_test

import (
	"fmt"

	mapsetinc "github.com/ttlbb/go-mapset-inc"
)

func Example() {
	// Size for the expected number of distinct keys to avoid regrowth.
	views := mapsetinc.NewTable[uint64, uint16](1_000)

	views.Add(1001)
	views.Add(1001)
	views.AddMany([]uint64{2002, 1001, 3003})

	fmt.Println(views.Count(1001), views.Len(), views.Total())
	fmt.Println(views.MostCommon(1))
	// Output:
	// 3 3 5
	// [{1001 3}]
}

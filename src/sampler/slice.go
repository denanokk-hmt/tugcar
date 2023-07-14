/*======================
Sample
========================*/
package sampler

import (
	"fmt"
)

func SliceExec() {

	a := [2]int{1, 1}
	fmt.Println(a)
	b := a
	fmt.Println(a)      // [1 1]
	fmt.Println(b)      // [1 1]
	fmt.Println(a == b) // true
	b[0] = 2
	fmt.Println(a) // [1 1]
	fmt.Println(b) // [2 1]

	aa := []int{0}
	bb := aa
	bb[0] = 1
	fmt.Println(aa) // 1
	fmt.Println(bb) // 1

	a2 := make([]string, 1, 2)
	a2[0] = "A"
	b2 := a2
	fmt.Println(a2)
	fmt.Println(b2)
	b2[0] = "B"
	fmt.Println(a2)
	fmt.Println(b2)
	b2 = append(b2, "B")
	a2[0] = "C"
	fmt.Println(a2)
	fmt.Println(b2)
	b2 = append(b2, "B")
	fmt.Println(a2)
	fmt.Println(b2)

}

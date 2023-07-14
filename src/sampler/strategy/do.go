/*======================
Sample
========================*/
package strategy

import "fmt"

type ConcreteStrategy struct {
}

func (sd *ConcreteStrategy) DoSomething() {
	// Do something
	fmt.Println("SS")
	//return "SS"
}

func Init(s string) int {
	fmt.Println(s)
	return 111
}

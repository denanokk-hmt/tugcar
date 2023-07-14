/*======================
Sample
========================*/
package datastore

import (
	"fmt"

	REQ "bwing.app/src/http/request"
)

type Robot struct {
	Name  string
	birth int
}

type Robots struct {
	Robots []Robot
}

///////////////////////////////////////////////////
func CastSample() {

	var rbs Robots
	var rb Robot

	rb = Robot{Name: "Dorami", birth: 2122}
	rbs.Robots = append(rbs.Robots, rb)
	dorami := rb

	rb = Robot{Name: "Dora", birth: 2110}
	rbs.Robots = append(rbs.Robots, rb)
	dora_dorami := rbs

	Anything := map[string]interface{}{
		"valString":   "文字列",
		"valInt":      1234,
		"valBool":     true,
		"dorami":      dorami,      // Robot (struct)
		"dora_dorami": dora_dorami, // Robot (struct)
	}

	Anything["valInt"] = Anything["valInt"].(int) + 1
	stringValue := Anything["valString"].(string)
	fmt.Println(stringValue)

	name := Anything["dorami"].(Robot).Name
	fmt.Println(name)

	names := Anything["dora_dorami"].(Robots)
	fmt.Println(names.Robots[0].Name, names.Robots[1].Name)

}

///////////////////////////////////////////////////
func TypeCheckSmaple(i interface{}) {

	_, ok := i.(*[]REQ.GetParameter)
	println("*[]REQ.GetParameter:", ok)

	switch i.(type) {
	case string:
		s := i.(string)
		println("i is string:", s)
	case bool:
		b := i.(bool)
		println("i is boolean:", b)
	case int:
		n := i.(int)
		println("i is integer:", n) // i is integer: 100
	case *[]REQ.GetParameter:
		println("i is *[]REQ.GetParameter:true")
	default:

	}
}

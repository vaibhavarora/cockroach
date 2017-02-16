package main

import (
	"fmt"
)

func modify(d []int) {
	d = append(d, 1)
}
func main() {
	var c []int
	modify(c)
	fmt.Println(c)
}

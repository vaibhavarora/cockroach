package main

import (
	"fmt"
)

type ex struct {
	a int
	b []int
}

func main() {
	c := []int{6, 7, 8}
	one := ex{a: 1, b: c}
	two := one

	two.b = append(two.b, 5)

	fmt.Println(&one)
	fmt.Println(&two)
	fmt.Println(&one == &two)
}

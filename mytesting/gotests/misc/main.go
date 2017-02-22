package main

import (
	"fmt"
)

type lock struct {
	id int
}

func main() {

	Q := make([]lock, 0)
	Q = append(Q, lock{id: 2})
	Q = append(Q, lock{id: 3})
	Q = append(Q, lock{id: 4})

	P := make([]lock, 0)
	//copy(P, Q)
	fmt.Println("Q", Q)
	fmt.Println("P", P)

	for _, each := range Q {
		P = append(P, each)
	}

	fmt.Println("Q", Q)
	fmt.Println("P", P)

}

package main

import (
	"fmt"
	"math/rand"
)

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

func main() {
	total := 1000

	for i := 0; i < 20; i++ {
		acc := random(1, total)
		fmt.Println("orig acc", acc)
		acc += 10
		fmt.Println("modified 1 acc", acc)
		acc %= total
		fmt.Println("modified 2 acc", acc)
	}

}

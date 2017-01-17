package main
 
import (
    "strconv"
    "fmt"
)

func main() {

    a := 50
    b := 50

    c := strconv.Itoa(a)
    d := strconv.Itoa(b)

    e := c + ":" + d

    fmt.Println(e)

}

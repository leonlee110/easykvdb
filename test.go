package main

import "fmt"
import "github.com/leonlee110/easykvdb/store"

func main() {
    db := store.NewStoreObject("", nil)

    err := db.Put("foo", "bar")
    if err == nil {
    }

    value, err := db.Get("foo")
    if err == nil {
        fmt.Println("Get sucess")
        //fmt.Printf("Value is %v\n", value.Data())
        fmt.Println(value)
    } else {
        fmt.Println("Get error")
    }

    err = db.Delete("foo")
}

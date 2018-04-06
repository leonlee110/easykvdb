package node

import "fmt"
import "github.com/leonlee110/easykvdb/store"

type NodeObject struct {
    stores []*store.StoreObject
    utils []float64
} 

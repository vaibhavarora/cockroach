package main

import (
    "fmt"
    "github.com/cockroachdb/cockroach/pkg/roachpb"
    "github.com/cockroachdb/cockroach/pkg/util/hlc"
    "container/heap"
    "reflect"
    "github.com/cockroachdb/cockroach/pkg/util/uuid"
    //"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

// var txnStates map[string]int32 {
//     "VALIDATED": 0,
//     "COMMITTED": 1,
//     "ABORTED":   2
// }


type vtnc struct {
    txnId uuid.UUID
    lowerBoundTS hlc.Timestamp
}

var TxnStateMap map[uuid.UUID]string
var visibleReadTnc hlc.Timestamp
type vtncHeap []vtnc

func makeTS(nanos int64, logical int32) hlc.Timestamp {
    return hlc.Timestamp{
        WallTime: nanos,
        Logical:  logical,
    }
}

func makeVtncHeap(nanos int64, logical int32) vtnc {
    return vtnc{
        uuid.MakeV4(),
        makeTS(nanos, logical),
    }
}

func (h vtncHeap) Len() int           { return len(h) }
func (h vtncHeap) Less(i, j int) bool { return h[i].lowerBoundTS.Less(h[j].lowerBoundTS) }
func (h vtncHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *vtncHeap) Push(x interface{}) {
    *h = append(*h, x.(vtnc))
}

func (h *vtncHeap) Pop() interface{} {
    oldHeap := *h
    length := len(oldHeap)
    value := oldHeap[length-1]
    *h = oldHeap[0 : length-1]
    return value
}


func (h *vtncHeap) updateVtnc() {
    update := true
    //var heapTop vtnc
    for update == true {
        heapTop := (*h)[0]
        txnId := heapTop.txnId
        switch {
        case TxnStateMap[txnId] == "COMMITTED":
            // If the top of heap is committed, move the visible read tnc to this value
            top := heap.Pop(h)
            visibleReadTnc.Forward(top.(vtnc).lowerBoundTS)
        case TxnStateMap[txnId] == "ABORTED":
            // If the top txn was aborted, just pop from the heap
            heap.Pop(h)
        case TxnStateMap[txnId] == "VALIDATED":
            // If the top of heap is still in validated state, don't update visible tnc; wait till its 
            //committed or aborted
            update = false
        }
    }
}


func updateTxnState(txnId uuid.UUID, state string) {

    //if _, ok := TxnStateMap[txnId]; ok {
    TxnStateMap[txnId] = state
    //}
}


// This example inserts several ints into an vtncHeap, checks the minimum,
// and removes them in order of priority.
func main() {
    TxnStateMap = make(map[uuid.UUID]string)
    visibleReadTnc = makeTS(0, 0)
    ts := []vtnc{makeVtncHeap(0, 1), makeVtncHeap(0, 2), makeVtncHeap(0, 5), makeVtncHeap(0, 4)}
    for _, t := range ts {
        TxnStateMap[t.txnId] = "VALIDATED"
    }
    fmt.Println("Mapping is %v", TxnStateMap)
    h := &vtncHeap{ts[0], ts[1], ts[2]}
    heap.Init(h)
    heap.Push(h, ts[3])
    
    updateTxnState(ts[1].txnId, "COMMITTED")
    h.updateVtnc()
    fmt.Println("VTNC is ", visibleReadTnc)
    updateTxnState(ts[0].txnId, "COMMITTED")
    h.updateVtnc()
    fmt.Println("VTNC is ", visibleReadTnc)
    //fmt.Println(roachpb.PENDING == "PENDING")
    fmt.Println()
}


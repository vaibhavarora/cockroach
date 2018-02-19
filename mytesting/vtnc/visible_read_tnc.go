package main

import (
    
    //"github.com/cockroachdb/cockroach/pkg/roachpb"
    "github.com/cockroachdb/cockroach/pkg/util/syncutil"
    "github.com/cockroachdb/cockroach/pkg/util/hlc"
    "github.com/cockroachdb/cockroach/pkg/util/uuid"
    //"github.com/cockroachdb/cockroach/pkg/util/log"
    //"golang.org/x/net/context"
    "container/heap"
    "fmt"
)

const VALIDATED = "VALIDATED"
const COMMITTED = "COMMITTED"
const ABORTED = "ABORTED"


type vtnc struct {
    txnId uuid.UUID
    lowerBoundTS hlc.Timestamp
}

//type TxnStateMap map[uuid.UUID]string
//var visibleReadTS hlc.Timestamp
type vtncHeap []vtnc

type VisibleTNC struct {
    syncutil.Mutex
    h vtncHeap
    TxnStateMap map[uuid.UUID]string
    visibleReadTS hlc.Timestamp
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


func (v *VisibleTNC) updateVtnc() {
    update := true
    
    h := &v.h
    for update == true {
        if len(*h) > 0 {
            heapTop := (*h)[0]
            txnId := heapTop.txnId

            switch {
            case v.TxnStateMap[txnId] == COMMITTED:
                // If the top of heap is committed, move the visible read tnc to this value
                top := heap.Pop(h)
                
                fmt.Println("Moving readtnc to ", top.(vtnc).lowerBoundTS)
                
                v.visibleReadTS.Forward(top.(vtnc).lowerBoundTS)
                delete(v.TxnStateMap, txnId)
            case v.TxnStateMap[txnId] == ABORTED:
                // If the top txn was aborted, just pop from the heap
                heap.Pop(h)
                delete(v.TxnStateMap, txnId)
            case v.TxnStateMap[txnId] == VALIDATED:
                // If the top of heap is still in validated state, don't update visible tnc; wait till its 
                //committed or aborted
                update = false
            }
        } else {
            update = false
        }
    }
    //fmt.Println( "Updated read tnc to %v", v.visibleReadTS)
    
}


func (v *VisibleTNC) updateTxnState(txnId uuid.UUID, state string, lowerBound hlc.Timestamp) {
    v.Lock()
    defer v.Unlock()

    txnExists := false
    if _, ok := v.TxnStateMap[txnId]; ok {
        txnExists = true
    }

    v.TxnStateMap[txnId] = state

    
    fmt.Println("Updated state of txn ", txnId, " to ", state)
    
    if state != VALIDATED {
        v.updateVtnc()
    } else {
        if !txnExists {
            heap.Push(&(v.h), vtnc{
                txnId,
                lowerBound,
                })
            fmt.Println( "Added to txn ", txnId, " to heap.")
        }

    }
}


func NewVisibleTNC() *VisibleTNC {
    visTNC := &VisibleTNC{}

    visTNC.TxnStateMap = make(map[uuid.UUID]string)
    visTNC.h = vtncHeap{}
    visTNC.visibleReadTS = hlc.ZeroTimestamp
    return visTNC
}


// This example inserts several ints into an vtncHeap, checks the minimum,
// and removes them in order of priority.
func main() {
    v := NewVisibleTNC()
    //visTNC.visibleReadTnc = makeTS(0, 0)
    ts := []vtnc{makeVtncHeap(0, 1), makeVtncHeap(0, 5), makeVtncHeap(0, 2), makeVtncHeap(0, 4), makeVtncHeap(0, 3)}
    for _, t := range ts {
        v.TxnStateMap[t.txnId] = "VALIDATED"
        fmt.Println("Adding to heap: txn=", t.txnId, " Timestamp=" ,  t.lowerBoundTS)
        heap.Push(&(v.h), t)
    }
    //fmt.Println("Mapping is ", v.TxnStateMap)
    //v.h = &vtncHeap{ts[0], ts[1], ts[2]}
    ///heap.Init(v.h)
    
    fmt.Println("\nInitial ReadVTNC is ", v.visibleReadTS)
    fmt.Println("Current top of minHeap is ", v.h[0].lowerBoundTS , "\n")
    v.updateTxnState(ts[2].txnId, "COMMITTED", hlc.ZeroTimestamp)
    v.updateVtnc()
    fmt.Println("ReadVTNC is ", v.visibleReadTS , "\n")
    v.updateTxnState(ts[0].txnId, "COMMITTED", hlc.ZeroTimestamp)
    v.updateVtnc()

    fmt.Println()
    v.updateTxnState(ts[4].txnId, "COMMITTED", hlc.ZeroTimestamp)
    v.updateVtnc()

    fmt.Println("\nCurrent top of minHeap is ", v.h[0].lowerBoundTS)

    v.updateTxnState(ts[3].txnId, "ABORTED", hlc.ZeroTimestamp)
    fmt.Println("\nCurrent top of minHeap is ", v.h[0].lowerBoundTS)
    fmt.Println("\nFinal ReadVTNC is ", v.visibleReadTS)
    //fmt.Println(roachpb.PENDING == "PENDING")
    fmt.Println()
}

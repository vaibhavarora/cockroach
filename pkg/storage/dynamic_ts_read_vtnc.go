package storage

import (
    
    //"github.com/cockroachdb/cockroach/pkg/roachpb"
    "github.com/cockroachdb/cockroach/pkg/util/syncutil"
    "github.com/cockroachdb/cockroach/pkg/util/hlc"
    "github.com/cockroachdb/cockroach/pkg/util/uuid"
    "github.com/cockroachdb/cockroach/pkg/util/log"
    "golang.org/x/net/context"
    "container/heap"
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


func (v *VisibleTNC) updateVtnc(ctx context.Context) {
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
                if log.V(2) {
                    log.Infof(ctx, "Moving readtnc to %v", top.(vtnc).lowerBoundTS)
                }
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
    if log.V(2) {
        log.Infof(ctx, "Updated read tnc to %v", v.visibleReadTS)
        log.Infof(ctx, "Updated heap to %v", v.h)
    }
}


func (v *VisibleTNC) updateTxnState(ctx context.Context, txnId uuid.UUID, state string, lowerBound hlc.Timestamp) {
    v.Lock()
    defer v.Unlock()

    txnExists := false
    if _, ok := v.TxnStateMap[txnId]; ok {
        txnExists = true
    }

    v.TxnStateMap[txnId] = state

    if log.V(2) {
        log.Infof(ctx, "Updated state of txn %v to %s", txnId, state)
    }
    if state != VALIDATED {
        v.updateVtnc(ctx)
    } else {
        if !txnExists {
            heap.Push(&(v.h), vtnc{
                txnId,
                lowerBound,
                })
        }

        if log.V(2) {
            log.Infof(ctx, "Added to heap txn %v; heap is %v", txnId, v.h)
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



// // This example inserts several ints into an vtncHeap, checks the minimum,
// // and removes them in order of priority.
// func main() {
//     TxnStateMap = make(map[uuid.UUID]string)
//     visibleReadTS = makeTS(0, 0)
//     ts := []vtnc{makeVtncHeap(0, 1), makeVtncHeap(0, 2), makeVtncHeap(0, 5), makeVtncHeap(0, 4)}
//     for _, t := range ts {
//         TxnStateMap[t.txnId] = VALIDATED
//     }
//     fmt.Println("Mapping is %v", TxnStateMap)
//     h := &vtncHeap{ts[0], ts[1], ts[2]}
//     heap.Init(h)
//     heap.Push(h, ts[3])
    
//     updateTxnState(ts[1].txnId, COMMITTED)
//     h.updateVtnc()
//     fmt.Println("VTNC is ", visibleReadTS)
//     updateTxnState(ts[0].txnId, COMMITTED)
//     h.updateVtnc()
//     fmt.Println("VTNC is ", visibleReadTS)
//     //fmt.Println(roachpb.PENDING == "PENDING")
//     fmt.Println()
// }


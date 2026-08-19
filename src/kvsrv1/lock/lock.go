package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type LockState struct {
	name  string
	owned bool
	myID  string
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	state LockState
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.state = LockState{name: l, owned: false, myID: "lockclient" + kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		owner, version, err := lk.ck.Get(lk.state.name)
		switch err {
		case rpc.OK:
			switch owner {
			case "":
				err := lk.ck.Put(lk.state.name, lk.state.myID, version)
				if err == rpc.OK {
					lk.state.owned = true
					return
				}
			case lk.state.myID:
				lk.state.owned = true
				return
			}
		case rpc.ErrNoKey:
			err := lk.ck.Put(lk.state.name, lk.state.myID, 0)
			if err == rpc.OK {
				lk.state.owned = true
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	if !lk.state.owned {
		return
	}
	for {
		owner, version, err := lk.ck.Get(lk.state.name)
		switch err {
		case rpc.OK:
			if owner == lk.state.myID {
				for {
					err := lk.ck.Put(lk.state.name, "", version)
					if err == rpc.OK {
						lk.state.owned = false
						return
					}
				}
			}
			lk.state.owned = false
			return
		case rpc.ErrNoKey:
			lk.state.owned = false
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

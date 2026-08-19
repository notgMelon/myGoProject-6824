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
				if err == rpc.ErrMaybe {
					// The Put may have succeeded, but the response was lost.
					// We don't know if we own the lock or not, so we have to
					// check again.
					continue
				}
			case lk.state.myID:
				lk.state.owned = true
				return
			default:
				// Someone else owns the lock.  Wait and try again.
			}
		case rpc.ErrNoKey:
			err := lk.ck.Put(lk.state.name, lk.state.myID, 0)
			if err == rpc.OK {
				lk.state.owned = true
				return
			}
			if err == rpc.ErrMaybe {
				continue
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
				err := lk.ck.Put(lk.state.name, "", version)
				if err == rpc.OK {
					lk.state.owned = false
					return
				}
				if err == rpc.ErrMaybe {
					// The Put may have succeeded, but the response was lost.
					// We don't know if we still own the lock or not, so we have to
					// check again.
					break
				}
			}
			lk.state.owned = false
			return
		case rpc.ErrNoKey:
			lk.state.owned = false
			return
		default:
			// Some other error occurred.  Wait and try again.
		}
		time.Sleep(100 * time.Millisecond)
	}
}

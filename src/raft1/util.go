package raft

import "log"

// Debugging
const Debug = false
// const Debug = true


func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

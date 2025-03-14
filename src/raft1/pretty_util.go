package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dCommit        logTopic = "CMIT"
	dSendAppend    logTopic = "APP1"
	dReplyAppend   logTopic = "APP2"
	dReceiveAppend logTopic = "APP3"
	dLeader        logTopic = "LEAD" // for leader transitions (to and from leader)
	dElection      logTopic = "ELEC"
	dVote          logTopic = "VOTE"
	dDisconnect    logTopic = "DISC"
	dTerm          logTopic = "TERM"
	dHeartbeat     logTopic = "HERT"
	dTime          logTopic = "TIME"
	dLock          logTopic = "LOCK"
	dError         logTopic = "ERRO"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugPrint(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

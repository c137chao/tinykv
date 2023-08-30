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
	CLNT logTopic = "CLNT"
	CMIT logTopic = "CMIT"
	DROP logTopic = "DROP"
	ERRO logTopic = "ERRO"
	INFO logTopic = "INFO"
	LEAD logTopic = "LEAD"
	LOG1 logTopic = "LOG1"
	LOG2 logTopic = "LOG2"
	PERS logTopic = "PERS"
	SNAP logTopic = "SNAP"
	TERM logTopic = "TERM"
	TEST logTopic = "TEST"
	TIMR logTopic = "TIMR"
	TRCE logTopic = "TRCE"
	VOTE logTopic = "VOTE"
	APED logTopic = "APED"
)

var debugStart time.Time
var debugVerbosity int

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

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func (r *Raft) printf(level int, topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= level {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v R%v <T%v> tick %v ", time, string(topic), r.id, r.Term, r.ticks)
		format = prefix + format + "\n"
		log.Printf(format, a...)
	}
}

package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"../mr"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	file := "./master-" + strconv.Itoa(rand.Intn(99999)) + ".log"

	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetPrefix("[mr-master]")
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	return
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}

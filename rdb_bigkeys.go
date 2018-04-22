package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/cupcake/rdb"
)

func main() {
	var sizeThreshold, processThreads uint
	var resultFile, rdbFile string
	var resultSep int
	var sortResult bool

	flag.UintVar(&sizeThreshold, "bytes", 1024, "only output keys used memory equal or greater than this size(in byte)")
	flag.StringVar(&resultFile, "file", "", "the file the result write to, default sys stdout")
	flag.UintVar(&processThreads, "threads", 2, "threads to parsing rdb file")
	flag.IntVar(&resultSep, "sep", 0, "seperator of result, 1: space, otherelse: comma, default 0")
	flag.BoolVar(&sortResult, "sorted", false, "sort keys in descending order by memory")
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}
	rdbFile = flag.Args()[0]

	//fmt.Printf("%v %v %v %v\n",sizeThreshold, processThreads,  resultFile, rdbFile)

	rdbFh, err := os.Open(rdbFile)
	if err != nil {
		panic(fmt.Sprintf("Fail to open rdb file %s: %v", rdbFile, err))
	} else {
		defer rdbFh.Close()
	}

	var resultFh *os.File
	if resultFile == "" {
		resultFh = os.Stdout
	} else {
		resultFh, err = os.OpenFile(resultFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			panic(fmt.Sprintf("Fail to open  file %s: %v", resultFile, err))
		}
		defer resultFh.Close()
	}
	var resultTitle string
	if resultSep == 1 {
		resultTitle = strings.Join(ResultTitleColumns, "	")
	} else {
		resultTitle = strings.Join(ResultTitleColumns, ",")
	}
	resultTitle += "\n"
	resultFh.WriteString(resultTitle)

	printChan := make(chan printStruct, 256)

	memCalback := MemCallback{}
	memCalback.Init()

	var wgCal, wgPrint sync.WaitGroup

	wgPrint.Add(1)
	if sortResult {
		go printResultSorted(&memCalback, bufio.NewWriter(resultFh), printChan, int(sizeThreshold), &wgPrint, resultSep)
	} else {
		go printResult(&memCalback, bufio.NewWriter(resultFh), printChan, int(sizeThreshold), &wgPrint, resultSep)
	}

	for i := uint(0); i < processThreads; i++ {
		wgCal.Add(1)
		go countMemory(&memCalback, printChan, &wgCal)
	}

	err = rdb.Decode(rdbFh, &memCalback)
	if err != nil {
		panic(err)
	}
	wgCal.Wait()
	close(printChan)

	wgPrint.Wait()
	//fmt.Println("Exit!")
}

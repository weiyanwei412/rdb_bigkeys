package main

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	REDIS_DB_NUMB    = 16
	REDIS_DATA_TYPES = 5
	TIMEFORMAT       = "2006-01-02_15:04:05"
	TIMEFORMAT_NSEC  = "2006-01-02_15:04:05.999999999"
	//RESULT_COLUMN_NAMES = "database,type,key,size_in_bytes,num_elements,largest_element,largest_element_size_in_bytes,expire_datetime\n"
)

var ResultTitleColumns []string = []string{"database", "type", "key", "size_in_bytes", "num_elements", "largest_element", "largest_element_size_in_bytes", "expire_datetime"}

type CmdOptions struct {
	SizeThreshold  int
	ResultFile     string
	RdbFile        string
	processThreads int
}

type redisTypeGen struct {
	db        int
	datType   string
	action    string
	key       []byte
	expiry    int64
	length    int64
	field     []byte
	valueSize int
}

type printStruct struct {
	db                 int
	rdatType           string
	key                string
	expirySecond       int64
	expiryNsecond      int64
	size               int
	elements           int64
	largestElement     string
	largestElementSize int

	currentElemCnt int64
	ifEndRecived   bool
}

func countMemory(mcBack *MemCallback, printChan chan printStruct, wg *sync.WaitGroup) {
	//fmt.Println("countMemory thread starts")

	defer wg.Done()
	var expSecond, expNsecond int64
	for dat := range mcBack.redisChan {
		expSecond = dat.expiry / 1000
		expNsecond = (dat.expiry % 1000) * 1000000

		switch dat.datType {
		case "string":
			//fmt.Println("string:", string(dat.key), dat.expiry)
			strKey := bytes.NewBuffer(dat.key).String()
			strSize := len(dat.key) + dat.valueSize
			printChan <- printStruct{
				db: dat.db, rdatType: dat.datType, key: strKey,
				expirySecond: expSecond, expiryNsecond: expNsecond,
				size: strSize, elements: 1,
				largestElement: strKey, largestElementSize: strSize,
			}
		default:
			processCompoundType(mcBack, dat, printChan, expSecond, expNsecond)

		}

	}
	//fmt.Println("countMemory thread exits")
}

func processCompoundType(mcBack *MemCallback, dat redisTypeGen, printChan chan printStruct, expSecond int64, expNsecond int64) {

	keyHsh := bytes.NewBuffer(dat.key).String()
	//fmt.Println(keyHsh, dat)
	mcBack.memLock.Lock()
	_, ok := mcBack.memCalMap[dat.db][keyHsh]
	if !ok {
		mcBack.memCalMap[dat.db][keyHsh] = &printStruct{
			db: dat.db, rdatType: dat.datType, key: keyHsh,
			currentElemCnt: 0, largestElement: "",
			largestElementSize: 0, size: 0, elements: 0,
			ifEndRecived: false, expirySecond: expSecond, expiryNsecond: expNsecond,
		}
	}
	switch dat.action {
	case "start":
		mcBack.memCalMap[dat.db][keyHsh].expirySecond = expSecond
		mcBack.memCalMap[dat.db][keyHsh].expiryNsecond = expNsecond
		mcBack.memCalMap[dat.db][keyHsh].elements = dat.length

	case "element":

		mcBack.memCalMap[dat.db][keyHsh].currentElemCnt += 1
		var valSize int
		var elementName string
		if dat.datType == "hash" {
			valSize = len(dat.field) + dat.valueSize
			elementName = bytes.NewBuffer(dat.field).String()
		} else {
			valSize = dat.valueSize
			elementName = strconv.Itoa(int(mcBack.memCalMap[dat.db][keyHsh].currentElemCnt))
		}
		mcBack.memCalMap[dat.db][keyHsh].size += valSize

		if valSize > mcBack.memCalMap[dat.db][keyHsh].largestElementSize {
			mcBack.memCalMap[dat.db][keyHsh].largestElement = elementName
			mcBack.memCalMap[dat.db][keyHsh].largestElementSize = valSize
		}
	case "end":

		mcBack.memCalMap[dat.db][keyHsh].ifEndRecived = true
	}
	if mcBack.memCalMap[dat.db][keyHsh].ifEndRecived {

		if mcBack.memCalMap[dat.db][keyHsh].currentElemCnt == mcBack.memCalMap[dat.db][keyHsh].elements {

			printChan <- *mcBack.memCalMap[dat.db][keyHsh]

			delete(mcBack.memCalMap[dat.db], keyHsh)
		} /*else {
			fmt.Printf("hash key %s is finish parsing, but element cnt not match\n", keyHsh)
		}*/
	}
	mcBack.memLock.Unlock()

}

func escapeComma(s string) string {
	if strings.Contains(s, ",") {
		return fmt.Sprintf("\"%s\"", s)
	} else {
		return s
	}
}

func escapeSpace(s string) string {
	arr := strings.Fields(s)
	if len(arr) > 1 {
		return fmt.Sprintf("\"%s\"", s)
	} else {
		return s
	}
}

func formatPrintStrComma(pStruct printStruct) string {
	var expiryStr string = ""
	if pStruct.expirySecond > 0 {
		expiryStr = time.Unix(pStruct.expirySecond, pStruct.expiryNsecond).Format(TIMEFORMAT)
		//fmt.Println(pStruct.expiry, expiryStr)
	}
	msg := fmt.Sprintf("%d,%s,%s,%d,%d,%s,%d,%s\n",
		pStruct.db,
		pStruct.rdatType,
		escapeComma(pStruct.key),
		pStruct.size,
		pStruct.elements,
		escapeComma(pStruct.largestElement),
		pStruct.largestElementSize,
		expiryStr)
	return msg
}

func formatPrintStrSpace(pStruct printStruct) string {
	var expiryStr string = ""
	if pStruct.expirySecond > 0 {
		expiryStr = time.Unix(pStruct.expirySecond, pStruct.expiryNsecond).Format(TIMEFORMAT)
		//fmt.Println(pStruct.expiry, expiryStr)
	}
	msg := fmt.Sprintf("%d	%s	%s	%d	%d	%s	%d	%s\n",
		pStruct.db,
		pStruct.rdatType,
		escapeSpace(pStruct.key),
		pStruct.size,
		pStruct.elements,
		escapeSpace(pStruct.largestElement),
		pStruct.largestElementSize,
		expiryStr)
	return msg
}

func printResult(mcBack *MemCallback, fd *bufio.Writer, printChan chan printStruct, sizeThreshold int, wg *sync.WaitGroup, resultSep int) {
	//fmt.Println("printResult thread starts")

	defer wg.Done()
	defer fd.Flush()
	var msg string
	for pStruct := range printChan {
		if resultSep == 1 {
			msg = formatPrintStrSpace(pStruct)
		} else {
			msg = formatPrintStrComma(pStruct)
		}

		if pStruct.size >= sizeThreshold {
			_, err := fd.WriteString(msg)
			if err != nil {
				panic("error when write parse result")
			}
			//fd.Flush()
		}
		//fmt.Println(msg)
	}

	//fmt.Println("printResult thread exits")

}

func printResultSorted(mcBack *MemCallback, fd *bufio.Writer, printChan chan printStruct, sizeThreshold int, wg *sync.WaitGroup, resultSep int) {
	//fmt.Println("printResult thread starts")

	defer wg.Done()
	defer fd.Flush()
	var msg string
	var printSorted []printStruct
	for pStruct := range printChan {

		if pStruct.size >= sizeThreshold {
			printSorted = append(printSorted, pStruct)
		}

	}
	sort.SliceStable(printSorted, func(i, j int) bool { return printSorted[i].size > printSorted[j].size })

	for _, pStruct := range printSorted {
		if resultSep == 1 {
			msg = formatPrintStrSpace(pStruct)
		} else {
			msg = formatPrintStrComma(pStruct)
		}
		_, err := fd.WriteString(msg)
		if err != nil {
			panic("error when write parse result")
		}
	}

}

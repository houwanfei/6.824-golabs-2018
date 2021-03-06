package main

import (
	"6.824/src/mapreduce"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapWCF(filename string, contents string) []mapreduce.KeyValue {
	// Your code here (Part II).
	var result = make([]mapreduce.KeyValue, 0)
	var wordMap = make(map[string]int)
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	contentSplits := strings.FieldsFunc(contents, f)
	for _, value := range contentSplits {
		if wordMap[value] != 0 {
			wordMap[value]++
		} else {
			wordMap[value] = 1
		}
	}
	for key, value := range wordMap {
		result = append(result, mapreduce.KeyValue{key, strconv.Itoa(value)})
	}
	return result
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceWCF(key string, values []string) string {
	// Your code here (Part II).
	var count = 0
	for _, value := range values {
		value_num, err := strconv.Atoi(value)
		if err == nil {
			count = count + value_num
		}
	}
	log.Printf("process key:%s finish count:%d", key, count)
	return strconv.Itoa(count)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapWCF, reduceWCF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapWCF, reduceWCF, 100, nil)
	}
}

package mapreduce

import (
	"os"
	"encoding/json"
	"log"
	"fmt"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	kvMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		file, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			debug("Reduce: read file error", err)
			return
		}
		dec := json.NewDecoder(file)
		var iRes []KeyValue
		err = dec.Decode(&iRes)
		if err != nil {
			debug("Reduce: decode error", err)
			fmt.Printf("Erro :%q\n", err)
			log.Fatal("Reduce: decode error", err)
			break
		}
		for _, kv := range iRes {
			if _, ok := kvMap[kv.Key]; !ok {
				var list []string
				kvMap[kv.Key] = list
			} else {
				kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
			}
		}
		file.Close()
	}
	outFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		debug("Reduce: write file error", err)
	}
	var keys []string
	for k := range kvMap {
		keys = append(keys, k)
	}
	enc := json.NewEncoder(outFile)
	for _, k := range keys {
		result := reduceF(k, kvMap[k])
		enc.Encode(KeyValue{k, result})
	}
	outFile.Close()
}

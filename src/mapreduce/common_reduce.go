package mapreduce


import (
        "encoding/json"
        "io/ioutil"
	"os"
	"sort"
	"strings"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	var outData []byte
	var intermediateKeyValueStrList []string
	var intermediateKeyValueStr string
	var intermediateKeyValue KeyValue
        outputKeyValueMap := make(map[string][]string)

	// for each mapper generated intermediate file for the reducer task
	// split file data into list of string and unmarshal it in list of KeyValue struct
	// len-1 to strip the last newline from the file
	for i := 0; i < nMap; i++ {
		outData, _ = ioutil.ReadFile(reduceName(jobName, i, reduceTask))
		outData = outData[:len(outData)-1]
		intermediateKeyValueStrList = strings.Split(string(outData), "\n")
		for _, intermediateKeyValueStr = range intermediateKeyValueStrList {
			json.Unmarshal([]byte(intermediateKeyValueStr), &intermediateKeyValue)
			outputKeyValueMap[intermediateKeyValue.Key] = append(outputKeyValueMap[intermediateKeyValue.Key], intermediateKeyValue.Value)
		}
	}

	outputKeys := make([]string, 0, len(outputKeyValueMap))
	for outputKey := range outputKeyValueMap {
		outputKeys = append(outputKeys, outputKey)
	}
	sort.Strings(outputKeys)

	f, _ := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644);
	defer f.Close()
	for i, outputKey := range outputKeys {
		kv := KeyValue{outputKey, reduceF(outputKey, outputKeyValueMap[outputKey])}
		b, _ := json.Marshal(kv)
		f.Write(b)
		if i < len(outputKeys)-1 {
			f.Write([]byte("\n"))
		}
	}
	f.Close()
}

package mapreduce

import (
	"fmt"
	"os"
	"sort"
	"encoding/json"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
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

	// 获取所有中间文件
	files := make([]*os.File, nMap)
	for i := range files {
		f, err := os.Open(reduceName(jobName, i, reduceTask))
		if err != nil {
			fmt.Printf("doReduce.Open(%s) failed, err= %v", reduceName(jobName, i, reduceTask), err)
			return
		}
		files[i] = f
	}

	// 读取所有中间文件内的kv
	kvmap := make(map[string][]string)
	for _, file := range files {
		enc := json.NewDecoder(file)
		var kv KeyValue
		for {
			err := enc.Decode(&kv)
			if err != nil {
				break
			}
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
		}
	}
	// 对key排序
	var keys []string
	for k := range kvmap {
		keys = append(keys, k)
	}

	// reduce排序后的kv，将结果写入outFile
	f, err := os.Create(outFile)
	if err != nil {
		fmt.Printf("doReduce.Create(%s) failed, err= %v", outFile, err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	sort.Strings(keys)
	for _, k := range keys {
		v := reduceF(k, kvmap[k])
		err := enc.Encode(KeyValue{k, v})
		if err != nil {
			fmt.Printf("doReduce.Encode(%s) failed, err= %v", outFile, err)
			return
		}
	}
	for _, file := range files {
		file.Close()
	}
}

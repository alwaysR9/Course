package mapreduce

import (
	"os"
	"strconv"
	"encoding/json"
	"log"
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

	// TODO
	// read all kv from map output
	var kv_map map[string][]string
	kv_map = make(map[string][]string)

	const MAP_FILE_PREFIX = "mrtmp.test"
	for i := 0; i < nMap; i++ {
		mapFileName := MAP_FILE_PREFIX + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTask)
		file, err := os.Open(mapFileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		decoder := json.NewDecoder(file)

		for decoder.More() {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			k := kv.Key
			v := kv.Value
			kv_map[k] = append(kv_map[k], v)
		}
	}

	file, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	enc := json.NewEncoder(file)

	for k, v := range kv_map {
		res := reduceF(k, v)
		kv := KeyValue{k, res}

		err := enc.Encode(kv)
		if err != nil {
			log.Fatal(err)
		}
	}
}

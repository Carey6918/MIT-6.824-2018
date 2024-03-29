package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	wg := sync.WaitGroup{}
	wg.Add(ntasks)
	for task := 0; task < ntasks; task++ {
		args := &DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    task,
			NumOtherPhase: n_other,
		}
		if phase == mapPhase {
			args.File = mapFiles[task]
		}
		// 我这里之前好像又写了个bug...go匿名函数没有传参..所以拿到的值都是最后一次的
		go func(args *DoTaskArgs, registerChan chan string) {
			for {
				// Part 4做的改动，是在这里取worker，一个worker失败以后，就直接扔掉了，换一个worker重试
				worker := <-registerChan
				success := call(worker, "Worker.DoTask", args, nil)
				// 如果成功了，则将worker放回chan，如果失败了，就一直重新执行
				if success {
					wg.Done()
					registerChan <- worker
					break
				}
			}

		}(args, registerChan)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

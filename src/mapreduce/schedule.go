package mapreduce

import (
	"fmt"
	"log"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	doneChannel := make(chan int, ntasks)

	for i := 0; i < ntasks; i++ {
		fmt.Println("Current task %d",i)
		go func(ntask int, nio int, p jobPhase) {
			for {
				worker := <-mr.registerChannel

				args := new(DoTaskArgs)
				args.Phase = p
				args.JobName = mr.jobName
				args.TaskNumber = ntask
				args.NumOtherPhase = nio
				args.File = mr.files[ntask]

				ok := call(worker, "Worker.DoTask", args, new (struct {}))
				if ok == false {
					log.Fatal("Schedule: RPC call error.")
				}
				go func() {
					doneChannel <- ntask
					mr.registerChannel <- worker
				}()
				return
			}
		}(i, nios, phase)
	}

	for i := 0; i < ntasks; i++ {
		<-doneChannel
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

package mapreduce

import "fmt"

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

	// get worker list
	workerArray := []RegisterArgs {}
	workerArray = append(workerArray, <- mr.registerChannel)

	for i := 0; i < ntasks; i++ {
		id := i % len(workerArray)
		args := new(DoTaskArgs)
		args.Phase = phase
		args.JobName = mr.jobName
		args.TaskNumber = i
		args.NumOtherPhase = nios
		args.File = mr.files[i]
		
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}

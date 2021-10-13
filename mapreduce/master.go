package mapreduce

import (
	"container/list"
	"fmt"
	"log"
    "sync/atomic"
)


type WorkerInfo struct {
	address string
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

// Handle scheduling, taking in a pointer to a jobs queue/channel 
func (mr *MapReduce) HandleScheduling(jobs chan *DoJobArgs) {
	var completeJobs uint64 = 0
	var totalJobs uint64 = uint64(len(jobs))

	log.Printf("Starting total of %d jobs...", totalJobs)
	// while there are still jobs to complete
	// https://tour.golang.org/concurrency/6
	for completeJobs < totalJobs {
		select {
			case job := <-jobs:
				go func() {
					/* registerChannel starts by containing all free workers.
					We might have to wait for the channel to have, and give us, a free one,
					but once we are finished with a job, we put the worker address back in the channel.*/ 
					address := <-mr.registerChannel
					var reply DoJobReply
					//RPC call to DoJob defined in worker
					ok := call(address, "Worker.DoJob", job, &reply)

					// job failed! 🥲
					if ok==false {
						log.Printf("Job failed, will retry...")
						// add job back into channel for retry
						jobs <- job
					// job complete! 🙌
					} else {
						// need to *atomically* increment the completeJobs counter (must be safe to access)
						atomic.AddUint64(&completeJobs, uint64(1))
						log.Printf("Completed %d / %d jobs...", completeJobs, totalJobs)
					}
					// add worker address back to registerChannel (acting as a queue of available workers)
					mr.registerChannel <- address
				}()
			// no case is ready (out of jobs); break.
			default:
				log.Printf("Completed %d / %d jobs...", completeJobs, totalJobs)
				break
		}
	}
}

/* Structs from common.go*/
// // RPC arguments and replies.  Field names must start with capital letters,
// // otherwise RPC will break.

// type DoJobArgs struct {
// 	File          string
// 	Operation     JobType
// 	JobNumber     int // this job's number
// 	NumOtherPhase int // total number of jobs in other phase (map or reduce)
// }

// type DoJobReply struct {
// 	OK bool
// }

func (mr *MapReduce) RunMaster() *list.List {
	// create buffered channel for mapping jobs
	// https://tour.golang.org/concurrency/3
	mapJobs := make(chan *DoJobArgs, mr.nMap)
	// push mapping jobs into channel (one for each in mr.nMap)
	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.Operation = "Map"
		args.JobNumber = i
		args.NumOtherPhase = mr.nReduce
		mapJobs <- args
	}
	log.Printf("Created mapping buffer...")

	// create buffered channel for reduce jobs
	reduceJobs := make(chan *DoJobArgs, mr.nReduce)
	// push reduce jobs into channel (one for each in mr.nReduce)
	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.Operation = "Reduce"
		args.JobNumber = i
		args.NumOtherPhase = mr.nMap
		reduceJobs <- args
	}
	log.Printf("Created reduce buffer...")

	// schedule map jobs
	log.Printf("Starting mapping jobs...")
	mr.HandleScheduling(mapJobs)
	log.Printf("Finished mapping jobs...")

	// schedule reduce jobs
	log.Printf("Starting reduce jobs...")
	mr.HandleScheduling(reduceJobs)
	log.Printf("Starting reduce jobs...")

	return mr.KillWorkers()
}

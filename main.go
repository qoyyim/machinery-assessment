package main

import (
	"fmt"
	"github.com/RichardKnop/machinery/v1/tasks"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
)

func main() {

	redisURL := "redis://0.0.0.0:6379"
	//cnf, err := config.New(Broker: )
	//if redisURL == "" {
	//	t.Skip("REDIS_URL is not defined")
	//}

	// Redis broker, Redis result backend
	cnf := &config.Config{
		Broker:        fmt.Sprintf(redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf(redisURL),
	}

	server, err := machinery.NewServer(cnf)
	if err != nil {
		fmt.Printf("not ok")
		return
		// do something with the error
	}
	server.RegisterTask("add", Add)

	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	defer worker.Quit()

	signature := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 1,
			},
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	asyncResult, err := server.SendTask(signature)
	if err != nil {
		fmt.Printf("cant send task", err)
		return
		// failed to send the task
		// do something with the error
	}
	results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
	if err != nil {
		// getting result of a task failed
		// do something with the error
	}
	for _, result := range results {
		fmt.Println(result.Interface())
	}

	fmt.Printf("all ok")
}

func Add(args ...int64) (int64, error) {
	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}
	fmt.Printf("return")
	return sum, nil
}

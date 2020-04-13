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
	server.RegisterTask("success", AlwaysSuccess)
	server.RegisterTask("fail", AlwaysFail)

	worker := server.NewWorker("test_worker", 1)
	go worker.Launch()
	defer worker.Quit()

	start := time.Now()
	signature := &tasks.Signature{
		Name: "fail",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: start.Unix(),
			}, {
				Type:  "int64",
				Value: start.UnixNano(),
			},
		},
	}
	// If the task fails, retry it up to 3 times
	signature.RetryCount = 4

	// Delay the task by 5 seconds
	//eta := time.Now().UTC().Add(time.Second * 5)
	//signature.ETA = &eta

	asyncResult, err := server.SendTask(signature)

	signature = &tasks.Signature{
		Name: "success",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: start.Unix(),
			}, {
				Type:  "int64",
				Value: start.UnixNano(),
			},
		},
	}

	asyncResult, err = server.SendTask(signature)
	if err != nil {
		fmt.Printf("cant send task", err)
		return
		// failed to send the task
		// do something with the error
	}
	results, err := asyncResult.Get(time.Duration(time.Second * 15))
	if err != nil {
		// getting result of a task failed
		// do something with the error
	}
	for _, result := range results {
		fmt.Println(result.Interface())
	}

	fmt.Printf("all ok")
}

func AlwaysSuccess(sec int64, nano int64) error {
	fmt.Print("success")
	fmt.Printf("\nqueued at", time.Unix(sec, 0))
	time.Sleep(1 * time.Second)
	end := time.Now()
	fmt.Printf("\nexecuted at", end)
	return nil
}

func AlwaysFail(sec int64, nano int64) error {
	fmt.Print("fail")
	fmt.Printf("\nqueued at", time.Unix(sec, 0))
	time.Sleep(1 * time.Second)
	end := time.Now()
	fmt.Printf("\nexecuted at", end)
	return fmt.Errorf("testing retries")
}

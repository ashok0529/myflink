package main

import (
	"cisco/common/kafka"
	"cisco/common/log"
	"flag"
	"os"
	"time"
        "fmt"
)

var (
	logger        *log.Logger
	producer      kafka.MessageProducer
)

func init() {

	address := flag.String("address", "localhost", "Kafka IP")
	port := flag.String("port", "9092", "Kafka Port")
	topic := flag.String("topic", "test1", "Kafka Topic")
	flag.Parse()
	if len(*address) ==0 || len(*port)==0 || len(*topic) ==0{
		fmt.Println("Usage: run main.go -address=<localhost> -port=<9092> -topic=<test1>")
                os.Exit(1)
	}
        fmt.Sprintf("Address=%s, Port=%s, Topic=%s",address,port,topic)
	logger = log.NewLogger(os.Stdout, log.Verbose)
	var err error
	producer, err = kafka.NewMessageProducer(*address+":"+*port, *topic, logger)
	if err != nil {
		logger.Error("***** Error creating Producer *****" + err.Error())
                os.Exit(1)
	} else {
		producer.LogSuccesses(true)
		producer.LogErrors(true)
		producer.LogQueued(true)
		producer.RunLogging()
	}
}

func main() {
	logger.Info("Starting Random String Producer")
	go queue1()
	go queue2()
        select {}

}

func queue1(){
	for {
		fmt.Println("Starting queue1")
		message := []byte(`To be, or not to be,--that is the question:--",
		"Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
		"Or to take arms against a sea of troubles,`)
		producer.QueueMessage(message)
		time.Sleep(time.Second * 10)
	}
}


func queue2(){
        i := 1
	for {
		fmt.Println("Starting queue2")
		message := []byte(fmt.Sprintf("Test : %d",i))
                i++
		producer.QueueMessage(message)
		time.Sleep(time.Second * 10)
	}
}

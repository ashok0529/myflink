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
	port := flag.String("port", "9091", "Kafka Port")
	topic := flag.String("topic", "test1", "Kafka Topic")
	flag.Parse()
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
	logger.Info("Starting iStreamer Producer")

	for {
		// TODO-SG: Perhaps, produced randomized data.
		message := []byte(`To be, or not to be,--that is the question:--",
		"Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
		"Or to take arms against a sea of troubles,`)
		producer.QueueMessage(message)
		time.Sleep(time.Second * 10)

	}

}

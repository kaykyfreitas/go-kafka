package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	// docs for config: github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "go-kafka-kafka-1:9092",
		"client.id":         "go-app-consumer",
		"group.id":          "go-app-group",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		fmt.Println(err.Error())
	}

	topics := []string{"test"}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}

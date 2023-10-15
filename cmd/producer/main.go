package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	err := Publish("Hello, world from Go!", "test", producer, []byte("hello"), deliveryChan)
	if err != nil {
		log.Fatal(err.Error())
	}

	go DeliveryReport(deliveryChan) // async

	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	// docs for config: github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "go-kafka-kafka-1:9092",
		"delivery.timeout.ms": "0",    // can be: any time value in milliseconds
		"acks":                "all",  // can be: "0", "1" or "all"
		"enable.idempotence":  "true", // can be: "true" or "false" (for "true", acks must be "all")
	}
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return producer
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		Key:   key,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch event := e.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				fmt.Println("error on message sending")
				// Develop a logic to retry message sent...
			} else {
				fmt.Println("message successfully sent: ", event.TopicPartition)
				// Save message status into database for example...
			}
		}
	}
}

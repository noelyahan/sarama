package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	brokers := []string{"localhost:9092"}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true


	//cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	prod, err := sarama.NewSyncProducer(brokers, cfg)
	handlerErr(err)

	// key = 111, value: null
	// header = user-id, creted-by

	msg := sarama.ProducerMessage{
		Topic:     "user-test",
		Key:       sarama.StringEncoder("1651816847"),
		Value:     sarama.StringEncoder("foo user"),
		Headers:   []sarama.RecordHeader{
			{
				Key:   []byte("user-id"),
				Value: []byte("123"),
			},
		},
		//Metadata:  nil,
		//Offset:    0,
		//Partition: 0,
		//Timestamp: time.Time{},
	}

	part, offset, err := prod.SendMessage(&msg)
	handlerErr(err)
	fmt.Printf("partiton: [%v], offset: [%v]", part, offset)
}

func handlerErr(err error) {
	if err != nil {
		log.Println(err)
		return
	}
}

package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func main() {
	addrs := []string{"localhost:9092"}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	// NACK/ACK
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// Routing data
	cfg.Producer.Partitioner = sarama.NewHashPartitioner


	p, err := sarama.NewSyncProducer(addrs, cfg)
	if err != nil {
		log.Println(err)
		return
	}

	msg := sarama.ProducerMessage{
		Topic:     "test",
		Key:       sarama.StringEncoder(fmt.Sprintf("%v", time.Now().Unix())),
		Value:     sarama.StringEncoder("custom data"),
		//Headers:   nil,
		//Metadata:  nil,
		//Offset:    0,
		Partition: -1,
		//Timestamp: time.Time{},
	}
	i, offset, err := p.SendMessage(&msg)
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Printf("partition: [%v], offset: [%v]\n", i, offset)
}

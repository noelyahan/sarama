package main

import (
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	addrs := []string{"localhost:9092"}
	cfg := sarama.NewConfig()

	c, err := sarama.NewConsumer(addrs, cfg)
	if err != nil {
		log.Println(err)
		return
	}

	// 13
	//
	pc, err := c.ConsumePartition("test", 0, sarama.OffsetNewest)
	if err != nil {
		log.Println(err)
		return
	}
	for true {
		select {
			case msg := <- pc.Messages():
				log.Println(string(msg.Key), string(msg.Value), msg.Offset)
				//if pc.HighWaterMarkOffset() == msg.Offset + 1 {
				//	return
				//}
		}
	}
}
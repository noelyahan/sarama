package main

import (
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	brokers := []string{"localhost:9092"}

	cfg := sarama.NewConfig()

	c, err := sarama.NewConsumer(brokers, cfg)
	handlerErr(err)

	pc, err := c.ConsumePartition("user-test", 0, sarama.OffsetOldest)
	handlerErr(err)

	for true {
		select {
		case msg := <- pc.Messages():
			log.Printf("key: [%v], value: [%v], offset: [%v]", string(msg.Key), string(msg.Value), msg.Offset)
		}
	}

}
func handlerErr(err error) {
	if err != nil {
		log.Println(err)
		return
	}
}
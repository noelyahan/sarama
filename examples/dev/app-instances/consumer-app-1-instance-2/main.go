package main

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
)

type myHandler struct {

}

func (myHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (myHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (myHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Println("instance-2", string(msg.Key), string(msg.Value), msg.Offset, msg.Partition)
		//sess.Commit()
		sess.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	addrs := []string{"localhost:9092"}
	cfg := sarama.NewConfig()

	c, err := sarama.NewConsumerGroup(addrs, "app-1", cfg)
	if err != nil {
		log.Println(err)
		return
	}

	handler := myHandler{}
	err = c.Consume(context.Background(), []string{"test"},  handler)
	if err != nil {
		log.Println(err)
		return
	}

}
package main

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
)

type customHander struct {
	
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (customHander) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (customHander) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (customHander) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("key: [%v], value: [%v], offset: [%v]", string(msg.Key), string(msg.Value), msg.Offset)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9092"}

	cfg := sarama.NewConfig()

	//cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	appId := "app-1"
	groupC, err := sarama.NewConsumerGroup(brokers, appId, cfg)
	handlerErr(err)

	handler := customHander{}
	err = groupC.Consume(context.Background(), []string{"user-test"}, handler)
	handlerErr(err)

}
func handlerErr(err error) {
	if err != nil {
		log.Println(err)
		return
	}
}
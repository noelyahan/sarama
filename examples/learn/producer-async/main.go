package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func main() {
	brokers := []string{"localhost:9092"}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true


	//cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	prod, err := sarama.NewAsyncProducer(brokers, cfg)
	handlerErr(err)

	// key = 111, value: null
	// header = user-id, creted-by

	msg := sarama.ProducerMessage{
		Topic:     "user-test",
		Key:       sarama.StringEncoder("1651815930"),
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

	msgs := make([]sarama.ProducerMessage, 0)
	for i := 0; i < 100; i++ {
		msg.Key = sarama.StringEncoder(fmt.Sprintf("%v", time.Now().Unix()))
		msgs = append(msgs, msg)
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			select {
			case <-prod.Successes():
			case err := <-prod.Errors():
				log.Println(err)
			}
		}
		done <- struct{}{}
	}()

	for _, m := range msgs {
		prod.Input() <- &m
	}

	<-done
	close(done)
	fmt.Println("done!")
}

func handlerErr(err error) {
	if err != nil {
		log.Println(err)
		return
	}
}

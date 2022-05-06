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

	// Routing data (load balance)
	cfg.Producer.Partitioner = sarama.NewHashPartitioner


	p, err := sarama.NewAsyncProducer(addrs, cfg)
	if err != nil {
		log.Println(err)
		return
	}

	msg := sarama.ProducerMessage{
		Topic:     "test",
		Key:       sarama.StringEncoder(fmt.Sprintf("%v", time.Now().Unix())),
		Value:     sarama.StringEncoder("custom data"),
		Headers:   []sarama.RecordHeader{
			{
				Key:   []byte("account-id"),
				Value: []byte("123"),
			},
		},
		//Metadata:  nil,
		//Offset:    0,
		Partition: -1,
		//Timestamp: time.Time{},
	}

	msgs := []sarama.ProducerMessage{}


	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			select {
			case <-p.Successes():
			case err := <- p.Errors():
				log.Println(err)
			}
		}
		done <- struct{}{}
	}()

	for i := 0; i < 150; i++ {
		msgs = append(msgs, msg)
	}

	for _, m := range msgs {
		p.Input() <- &m
	}
	<-done
	close(done)
}
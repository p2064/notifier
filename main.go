package main

import (
	"context"
	"log"

	"github.com/p2064/pkg/config"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	log.Print("Start notifier")
	if config.Status != config.GOOD {
		log.Fatalf("failed to get config")
	}

	topic := "notify"
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		Topic:     topic,
		Partition: 0,
	})

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Printf("Got in Notifier from KAFKA: %s", string(msg.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}

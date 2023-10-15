package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/p2064/pkg/config"
	"github.com/p2064/pkg/logs"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	logs.InfoLogger.Print("Start notifier")
	if config.Status != config.GOOD {
		logs.ErrorLogger.Fatalf("failed to get config")
	}

	topic := "notify"
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{fmt.Sprintf("localhost:%s", os.Getenv("KAFKA_PORT"))},
		Topic:   topic,
		GroupID: "my-group",
	})

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		logs.InfoLogger.Printf("Got in Notifier from KAFKA: %s", string(msg.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}

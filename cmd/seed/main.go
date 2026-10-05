package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	kafkax "github.com/zao/flight-fanout/internal/kafka"
	"github.com/zao/flight-fanout/internal/models"
)

func main() {
	cfg := kafkax.DefaultConfig()
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_INPUT_TOPIC"); v != "" {
		cfg.InputTopic = v
	}

	if err := kafkax.EnsureTopics(cfg); err != nil {
		log.Printf("warn: ensure topics: %v", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.InputTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	msg, err := sampleFlightRegistration()
	if err != nil {
		log.Fatalf("build sample: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("LA3090"),
		Value: msg,
	}); err != nil {
		log.Fatalf("produce: %v", err)
	}

	log.Printf("seeded 1 flight registration into topic %s (%d bytes)", cfg.InputTopic, len(msg))
}

func sampleFlightRegistration() ([]byte, error) {
	dep := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	eta := time.Date(2026, 10, 10, 9, 5, 0, 0, time.UTC)
	arr := time.Date(2026, 10, 10, 9, 10, 0, 0, time.UTC)

	env := models.Envelope[models.FlightRegistration]{
		SchemaVersion: models.SchemaVersionV1,
		Payload: models.FlightRegistration{
			FlightNumber: "LA3090",
			Origin:       "GRU",
			Destination:  "GIG",
			DepartureAt:  dep,
			ETA:          eta,
			ArrivalAt:    arr,
			Passengers: []models.Passenger{
				{Name: "Ana Silva", Seat: "12A"},
				{Name: "Bruno Costa", Seat: "12B"},
				{Name: "Carla Mendes", Seat: "14C"},
				{Name: "Diego Rocha", Seat: "15A"},
				{Name: "Elena Souza", Seat: "16F"},
				{Name: "Felipe Nunes", Seat: "18B"},
				{Name: "Gabriela Lima", Seat: "19D"},
				{Name: "Henrique Alves", Seat: "21A"},
			},
		},
	}
	return json.MarshalIndent(env, "", "  ")
}

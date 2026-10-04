package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	kafkax "github.com/zao/flight-fanout/internal/kafka"
	"github.com/zao/flight-fanout/internal/models"
	"github.com/zao/flight-fanout/internal/transform"
)

func main() {
	cfg := kafkax.DefaultConfig()
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_INPUT_TOPIC"); v != "" {
		cfg.InputTopic = v
	}
	if v := os.Getenv("KAFKA_OUTPUT_TOPIC"); v != "" {
		cfg.OutputTopic = v
	}
	if v := os.Getenv("KAFKA_CONSUMER_GROUP"); v != "" {
		cfg.ConsumerGroup = v
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := kafkax.EnsureTopics(cfg); err != nil {
		log.Printf("warn: ensure topics: %v", err)
	}

	reader := kafkax.NewReader(cfg)
	defer reader.Close()

	writer := kafkax.NewWriter(cfg)
	defer writer.Close()

	log.Printf("consumer started brokers=%v input=%s output=%s group=%s",
		cfg.Brokers, cfg.InputTopic, cfg.OutputTopic, cfg.ConsumerGroup)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("shutdown")
				return
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		reg, decodeErr := models.DecodeFlightRegistration(msg.Value)
		if decodeErr != nil {
			// Poison message: log and advance offset so the consumer does not stall.
			log.Printf("skip poison message topic=%s partition=%d offset=%d: %v",
				msg.Topic, msg.Partition, msg.Offset, decodeErr)
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit error: %v", err)
			}
			continue
		}

		for {
			if err := publishFanOut(ctx, writer, reg); err != nil {
				if errors.Is(err, context.Canceled) {
					log.Println("shutdown")
					return
				}
				log.Printf("produce error offset=%d: %v (retrying)", msg.Offset, err)
				select {
				case <-ctx.Done():
					log.Println("shutdown")
					return
				case <-time.After(time.Second):
				}
				continue
			}
			break
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

func publishFanOut(ctx context.Context, writer *kafka.Writer, reg *models.FlightRegistration) error {
	passengers := transform.FanOut(reg)
	out := make([]kafka.Message, 0, len(passengers))
	for _, pf := range passengers {
		body, err := models.EncodePassengerFlight(pf)
		if err != nil {
			return err
		}
		out = append(out, kafka.Message{
			Key:   []byte(pf.Seat),
			Value: body,
		})
	}

	if err := kafkax.WriteMessages(ctx, writer, out...); err != nil {
		return err
	}

	log.Printf("fan-out ok flight=%s passengers=%d", reg.FlightNumber, len(out))
	return nil
}

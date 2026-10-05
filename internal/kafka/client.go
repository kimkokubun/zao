package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	DefaultBrokers         = "localhost:9092"
	DefaultInputTopic      = "flight-registrations"
	DefaultOutputTopic     = "passenger-flights"
	DefaultConsumerGroup   = "flight-fanout"
	DefaultPartitionCount  = 1
	DefaultReplicationFact = 1
)

// Config holds Kafka connection settings.
type Config struct {
	Brokers       []string
	InputTopic    string
	OutputTopic   string
	ConsumerGroup string
}

// DefaultConfig returns sensible local defaults.
func DefaultConfig() Config {
	return Config{
		Brokers:       []string{DefaultBrokers},
		InputTopic:    DefaultInputTopic,
		OutputTopic:   DefaultOutputTopic,
		ConsumerGroup: DefaultConsumerGroup,
	}
}

// EnsureTopics creates input and output topics if they do not exist.
func EnsureTopics(cfg Config) error {
	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}

	ctrlConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer ctrlConn.Close()

	topics := []kafka.TopicConfig{
		{Topic: cfg.InputTopic, NumPartitions: DefaultPartitionCount, ReplicationFactor: DefaultReplicationFact},
		{Topic: cfg.OutputTopic, NumPartitions: DefaultPartitionCount, ReplicationFactor: DefaultReplicationFact},
	}
	if err := ctrlConn.CreateTopics(topics...); err != nil {
		// Topic already exists is fine for repeated starts.
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return fmt.Errorf("create topics: %w", err)
		}
	}
	return nil
}

// NewReader creates a consumer-group reader for the input topic.
func NewReader(cfg Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.InputTopic,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // manual commit after successful produce
		StartOffset:    kafka.FirstOffset,
		MaxWait:        time.Second,
	})
}

// NewWriter creates a writer for the output topic.
func NewWriter(cfg Config) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.OutputTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
}

// WriteMessages publishes messages and waits for acknowledgements.
func WriteMessages(ctx context.Context, w *kafka.Writer, msgs ...kafka.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	return w.WriteMessages(ctx, msgs...)
}

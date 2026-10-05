package models

import (
	"encoding/json"
	"fmt"
	"time"
)

const SchemaVersionV1 = "1.0"

// Envelope wraps every Kafka message with a schema version.
type Envelope[T any] struct {
	SchemaVersion string `json:"schema_version"`
	Payload       T      `json:"payload"`
}

// FlightRegistration is the inbound flight-centric payload.
type FlightRegistration struct {
	FlightNumber string      `json:"flight_number"`
	Origin       string      `json:"origin"`
	Destination  string      `json:"destination"`
	DepartureAt  time.Time   `json:"departure_at"`
	ETA          time.Time   `json:"eta"`
	ArrivalAt    time.Time   `json:"arrival_at"`
	Passengers   []Passenger `json:"passengers"`
}

// Passenger is a passenger on a flight registration.
type Passenger struct {
	Name string `json:"name"`
	Seat string `json:"seat"`
}

// FlightInfo is the nested flight data on outbound messages.
type FlightInfo struct {
	FlightNumber string    `json:"flight_number"`
	Origin       string    `json:"origin"`
	Destination  string    `json:"destination"`
	DepartureAt  time.Time `json:"departure_at"`
	ETA          time.Time `json:"eta"`
	ArrivalAt    time.Time `json:"arrival_at"`
}

// PassengerFlight is the outbound passenger-centric payload.
type PassengerFlight struct {
	Name   string     `json:"name"`
	Seat   string     `json:"seat"`
	Flight FlightInfo `json:"flight"`
}

// DecodeFlightRegistration parses and validates an inbound envelope.
func DecodeFlightRegistration(data []byte) (*FlightRegistration, error) {
	var env Envelope[FlightRegistration]
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshal flight registration: %w", err)
	}
	if env.SchemaVersion != SchemaVersionV1 {
		return nil, fmt.Errorf("unsupported schema_version %q", env.SchemaVersion)
	}
	if env.Payload.FlightNumber == "" {
		return nil, fmt.Errorf("flight_number is required")
	}
	if len(env.Payload.Passengers) == 0 {
		return nil, fmt.Errorf("passengers must not be empty")
	}
	return &env.Payload, nil
}

// EncodePassengerFlight wraps a passenger flight payload in a versioned envelope.
func EncodePassengerFlight(pf PassengerFlight) ([]byte, error) {
	env := Envelope[PassengerFlight]{
		SchemaVersion: SchemaVersionV1,
		Payload:       pf,
	}
	return json.Marshal(env)
}

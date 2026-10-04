package transform

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/zao/flight-fanout/internal/models"
)

func TestFanOut(t *testing.T) {
	dep := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	eta := time.Date(2026, 10, 10, 9, 5, 0, 0, time.UTC)
	arr := time.Date(2026, 10, 10, 9, 10, 0, 0, time.UTC)

	reg := &models.FlightRegistration{
		FlightNumber: "LA3090",
		Origin:       "GRU",
		Destination:  "GIG",
		DepartureAt:  dep,
		ETA:          eta,
		ArrivalAt:    arr,
		Passengers: []models.Passenger{
			{Name: "Ana Silva", Seat: "12A"},
			{Name: "Bruno Costa", Seat: "12B"},
		},
	}

	got := FanOut(reg)
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}

	if got[0].Name != "Ana Silva" || got[0].Seat != "12A" {
		t.Fatalf("unexpected first passenger: %+v", got[0])
	}
	if got[0].Flight.FlightNumber != "LA3090" || got[0].Flight.Origin != "GRU" {
		t.Fatalf("unexpected nested flight: %+v", got[0].Flight)
	}
	if !got[0].Flight.DepartureAt.Equal(dep) || !got[0].Flight.ETA.Equal(eta) {
		t.Fatalf("unexpected flight times: %+v", got[0].Flight)
	}

	if got[1].Name != "Bruno Costa" || got[1].Seat != "12B" {
		t.Fatalf("unexpected second passenger: %+v", got[1])
	}
	if got[1].Flight.Destination != "GIG" {
		t.Fatalf("unexpected destination: %s", got[1].Flight.Destination)
	}
}

func TestDecodeFlightRegistration_UnsupportedVersion(t *testing.T) {
	raw := []byte(`{"schema_version":"9.9","payload":{"flight_number":"X","passengers":[{"name":"A","seat":"1A"}]}}`)
	_, err := models.DecodeFlightRegistration(raw)
	if err == nil {
		t.Fatal("expected error for unsupported schema_version")
	}
}

func TestDecodeAndEncodeRoundTrip(t *testing.T) {
	raw := []byte(`{
		"schema_version": "1.0",
		"payload": {
			"flight_number": "LA3090",
			"origin": "GRU",
			"destination": "GIG",
			"departure_at": "2026-10-10T08:00:00Z",
			"eta": "2026-10-10T09:05:00Z",
			"arrival_at": "2026-10-10T09:10:00Z",
			"passengers": [
				{"name": "Ana Silva", "seat": "12A"}
			]
		}
	}`)

	reg, err := models.DecodeFlightRegistration(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	msgs := FanOut(reg)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	encoded, err := models.EncodePassengerFlight(msgs[0])
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var env models.Envelope[models.PassengerFlight]
	if err := json.Unmarshal(encoded, &env); err != nil {
		t.Fatalf("unmarshal encoded: %v", err)
	}
	if env.SchemaVersion != models.SchemaVersionV1 {
		t.Fatalf("unexpected schema_version: %s", env.SchemaVersion)
	}
	if env.Payload.Name != "Ana Silva" || env.Payload.Flight.FlightNumber != "LA3090" {
		t.Fatalf("unexpected payload: %+v", env.Payload)
	}
}

package transform

import "github.com/zao/flight-fanout/internal/models"

// FanOut converts one flight registration into one message per passenger,
// with the passenger as parent and the flight nested as child.
func FanOut(reg *models.FlightRegistration) []models.PassengerFlight {
	flight := models.FlightInfo{
		FlightNumber: reg.FlightNumber,
		Origin:       reg.Origin,
		Destination:  reg.Destination,
		DepartureAt:  reg.DepartureAt,
		ETA:          reg.ETA,
		ArrivalAt:    reg.ArrivalAt,
	}

	out := make([]models.PassengerFlight, 0, len(reg.Passengers))
	for _, p := range reg.Passengers {
		out = append(out, models.PassengerFlight{
			Name:   p.Name,
			Seat:   p.Seat,
			Flight: flight,
		})
	}
	return out
}

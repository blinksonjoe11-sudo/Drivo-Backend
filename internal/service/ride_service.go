package service

import (
	"context"
	"drivo/internal/jobs"
	"drivo/internal/models"
	"drivo/internal/repository"
	"drivo/internal/workers"
	"drivo/internal/ws"
	"drivo/pkg/fcm"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type RideService struct {
	rideRepo   *repository.RideRepo
	driverRepo *repository.DriverRepo
	hub        *ws.Hub
	riderHub   *ws.RiderHub
	chatSvc    *ChatService
	// userRepo *repository.UserRepo
}

func NewRideService(rideRepo *repository.RideRepo, driverRepo *repository.DriverRepo, hub *ws.Hub, riderHub *ws.RiderHub, chatSvc *ChatService) *RideService {
	return &RideService{
		rideRepo:   rideRepo,
		driverRepo: driverRepo,
		hub:        hub,
		riderHub:   riderHub,
		chatSvc:    chatSvc,
		// userRepo:   userRepo,

	}
}

const (
	baseFare    = 500.0
	perKmRate   = 150.0
	perMinRate  = 20.0
	avgSpeedKmH = 30.0
)

func HaversineDistance(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusKm = 6371.0

	dLat := toRadians(lat2 - lat1)
	dLng := toRadians(lng2 - lng1)

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(toRadians(lat1))*math.Cos(toRadians(lat2))*
			math.Sin(dLng/2)*math.Sin(dLng/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusKm * c
}

func toRadians(deg float64) float64 {
	return deg * math.Pi / 180
}

func calculateFare(distanceKm float64) float64 {
	estimatedMinutes := (distanceKm / avgSpeedKmH) * 60
	fare := baseFare + (distanceKm * perKmRate) + (estimatedMinutes * perMinRate)
	return math.Round(fare/50) * 50
}

func (s *RideService) notifyRider(riderID uuid.UUID, msg ws.Message) {
	bytes, _ := json.Marshal(msg)
	s.riderHub.SendToRider(riderID, bytes)

}

func (s *RideService) notifyDriver(driverID uuid.UUID, msg ws.Message) {
	bytes, _ := json.Marshal(msg)
	s.hub.SendToDriver(driverID, bytes)
}

func (s *RideService) RequestRide(ctx context.Context, riderID uuid.UUID, input models.RideRequestInput) (models.Ride, error) {

	distanceKm := HaversineDistance(
		input.PickupLat, input.PickupLng,
		input.DropoffLat, input.DropoffLng,
	)

	if distanceKm < 0.5 {
		return models.Ride{}, errors.New("pickup and dropoff are too close")
	}

	estimatedFare := calculateFare(distanceKm)

	isScheduled := input.ScheduledAt != nil
	status := models.RideStatusPending

	if isScheduled {

		now := time.Now().UTC()
		minTime := now.Add(1 * time.Minute)
		maxTime := now.Add(7 * 24 * time.Hour)

		if input.ScheduledAt.Before(minTime) {
			return models.Ride{}, errors.New("scheduled time must be at least 1 minute from now")
		}
		if input.ScheduledAt.After(maxTime) {
			return models.Ride{}, errors.New("scheduled time cannot be more than 7 days in advance")
		}
		status = models.RideStatusScheduled
	}

	ride := models.Ride{
		RiderID:        riderID,
		PickupLat:      input.PickupLat,
		PickupLng:      input.PickupLng,
		DropoffLat:     input.DropoffLat,
		DropoffLng:     input.DropoffLng,
		PickupAddress:  input.PickupAddress,
		DropoffAddress: input.DropoffAddress,
		Status:         status,
		EstimatedFare:  estimatedFare,
		DistanceKm:     distanceKm,
		IsScheduled:    isScheduled,
		ScheduledAt:    input.ScheduledAt,
	}

	createdRide, err := s.rideRepo.CreateRide(ctx, ride)
	if err != nil {
		return models.Ride{}, err
	}

	if !isScheduled {
		go s.FindAndNotifyDrivers(context.Background(), createdRide)
	}

	return createdRide, nil
}

func (s *RideService) CancelRide(ctx context.Context, riderUserID uuid.UUID, rideID uuid.UUID) error {

	ride, err := s.rideRepo.GetRideByID(ctx, rideID)

	if err != nil {
		return fmt.Errorf("ride not found: %v", err)
	}

	// confirm the rider is the owner of the ride

	if riderUserID != ride.RiderID {
		return errors.New("youre not the owner of the ride")
	}

	// cancel only pending or accepted ride
	if ride.Status != models.RideStatusAccepted && ride.Status != models.RideStatusPending {
		return fmt.Errorf("cannot cancel a ride with status: %s", ride.Status)
	}

	if err := s.rideRepo.UpdateRideStatus(ctx, rideID, models.RideStatusCancelled, nil); err != nil {
		return err
	}

	// Scenero 1, cancel while pending

	if ride.Status == models.RideStatusPending {
		// remove candidates from redis
		_ = s.rideRepo.DeleteRideCandidates(ctx, rideID)

		// notify rider
		s.notifyRider(ride.RiderID, ws.Message{
			Type: ws.MessageTypeRideCancelledByRider,
			Payload: map[string]string{
				"ride_id": rideID.String(),
				"message": "Your ride has been cancelled",
			},
		})

		fmt.Printf("Ride %s cancelled by rider while pending\n", rideID)
		return nil

	}

	// scenero 2 - cancelled after driver accepted

	if ride.Status == models.RideStatusAccepted && ride.DriverID != nil {

		driver, err := s.driverRepo.GetDriverByID(*ride.DriverID)
		if err == nil {
			// notify driver
			s.notifyDriver(driver.UserID, ws.Message{
				Type: ws.MessageTypeRideCancelledByRider,
				Payload: map[string]string{
					"ride_id": rideID.String(),
					"message": "Rider cancelled the ride",
				},
			})
		}
		s.notifyRider(ride.RiderID, ws.Message{
			Type: ws.MessageTypeRideCancelledByRider,
			Payload: map[string]string{
				"ride_id": rideID.String(),
				"message": "Your ride has been cancelled",
			},
		})

		if ride.RideMode != "pool" {
			go s.chatSvc.CloseSession(context.Background(), rideID, ride.RiderID, driver.UserID)
		}

		fmt.Printf("Ride %s cancelled by rider after acceptance\n", rideID)
	}

	return nil

}

func (s *RideService) DriverCancelRide(ctx context.Context, driverUserID uuid.UUID, rideID uuid.UUID) error {
	ride, err := s.rideRepo.GetRideByID(ctx, rideID)
	if err != nil {
		return fmt.Errorf("ride not found: %v", err)
	}

	// Confirm this driver owns the ride
	driver, err := s.driverRepo.GetDriverByUserID(driverUserID)
	if err != nil {
		return fmt.Errorf("driver not found: %v", err)
	}

	if ride.DriverID == nil || *ride.DriverID != driver.ID {
		return errors.New("you are not the driver on this ride")
	}

	// Can only cancel accepted rides
	if ride.Status != models.RideStatusAccepted {
		return fmt.Errorf("cannot cancel a ride with status: %s", ride.Status)
	}

	// update status back to pending
	if err := s.rideRepo.UpdateRideStatus(ctx, rideID, models.RideStatusPending, nil); err != nil {
		return err
	}

	// Update driver cancellation rate
	if err := s.driverRepo.IncrementCancellationRate(ctx, driver.ID); err != nil {
		fmt.Printf("failed to update cancellation rate: %v\n", err)
	}

	// Notify rider
	s.notifyRider(ride.RiderID, ws.Message{
		Type: ws.MessageTypeRideCancelledByDriver,
		Payload: map[string]string{
			"ride_id": rideID.String(),
			"message": "Your driver cancelled. Finding you a new driver...",
		},
	})

	// Try next candidate
	err = s.notifyNextDriver(ctx, ride)
	if err != nil {
		// No more candidates — cancel the ride fully
		s.rideRepo.UpdateRideStatus(ctx, rideID, models.RideStatusCancelled, nil)
		s.notifyRider(ride.RiderID, ws.Message{
			Type: ws.MessageTypeRideCancelledByDriver,
			Payload: map[string]string{
				"ride_id": rideID.String(),
				"message": "Sorry, no drivers are available right now. Please try again.",
			},
		})
	}

	fmt.Printf("Ride %s cancelled by driver %s\n", rideID, driverUserID)
	return nil

}

func (s *RideService) findNearestDrivers(ctx context.Context, pickupLat, pickupLng float64, limit int) ([]uuid.UUID, error) {

	onlineUserIDs := s.hub.GetOnlineDriverIDs()

	fmt.Printf("Online userIDs from hub: %v\n", onlineUserIDs)

	if len(onlineUserIDs) == 0 {
		return nil, errors.New("no online drivers")
	}

	type driverDistance struct {
		driverID uuid.UUID
		distance float64
	}

	var candidates []driverDistance

	for _, userID := range onlineUserIDs {

		loc, err := s.driverRepo.GetLocationFromRedis(ctx, userID)
		if err != nil {
			fmt.Printf("No location for userID %s: %v\n", userID, err)
			continue
		}

		driver, err := s.driverRepo.GetDriverByUserID(userID)
		if err != nil {
			fmt.Printf("No driver record for userID %s: %v\n", userID, err)
			continue
		}

		_, err = s.rideRepo.GetOngoingRide(ctx, driver.ID)
		if err == nil {

			fmt.Printf("Skipping driver %s — already on a ride\n", driver.ID)
			continue
		}

		distance := HaversineDistance(pickupLat, pickupLng, loc.Latitude, loc.Longitude)
		fmt.Printf("Driver %s is %.2fkm away\n", driver.ID, distance)

		if distance <= 10.0 {
			candidates = append(candidates, driverDistance{
				driverID: driver.ID,
				distance: distance,
			})
		}
	}

	if len(candidates) == 0 {
		return nil, errors.New("no drivers within range")
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].distance < candidates[j].distance
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	result := make([]uuid.UUID, len(candidates))
	for i, c := range candidates {
		result[i] = c.driverID
	}

	fmt.Printf("Final candidates (driver IDs): %v\n", result)
	return result, nil
}

func (s *RideService) notifyNextDriver(ctx context.Context, ride models.Ride) error {
	driverID, err := s.rideRepo.GetNextCandidate(ctx, ride.ID)
	if err != nil {

		// fecch current ride

		currentRide, fetchErr := s.rideRepo.GetRideByID(ctx, ride.ID)

		if fetchErr != nil {
			return fetchErr
		}

		// cancel ride only if the current ride is pending

		if currentRide.Status == models.RideStatusPending {
			s.notifyRider(currentRide.RiderID, ws.Message{
				Type: ws.MessageTypeNoCandidates,
				Payload: map[string]string{
					"ride_id": ride.ID.String(),
					"message": "No drivers available right now. Please try again.",
				},
			})
			s.rideRepo.UpdateRideStatus(ctx, ride.ID, models.RideStatusCancelled, nil)
		}

		return errors.New("no drivers accepted the ride")
	}

	ride, err = s.rideRepo.GetRideByID(ctx, ride.ID)

	if err != nil {
		return fmt.Errorf("notifyNextDriver: ride %s not found", ride.ID)

	}
	if ride.Status != models.RideStatusPending {
		return fmt.Errorf("notifyNextDriver: ride %s is no longer pending (%s), stopping", ride.ID, ride.Status)
	}

	driver, err := s.driverRepo.GetDriverByID(driverID)
	if err != nil {
		fmt.Printf("Could not find driver %s: %v — trying next\n", driverID, err)
		return s.notifyNextDriver(ctx, ride)
	}

	_, err = s.rideRepo.GetOngoingRide(ctx, driver.ID)
	if err == nil {
		fmt.Printf("Driver %s is on another ride — skipping\n", driver.ID)
		return s.notifyNextDriver(ctx, ride)
	}

	user, err := s.driverRepo.GetUserByDriverID(driver.ID)
	if err != nil {
		fmt.Printf("Could not find user for driver %s: %v — trying next\n", driver.ID, err)
		return s.notifyNextDriver(ctx, ride)
	}

	fmt.Printf("Notifying driver %s (userID: %s)\n", driver.ID, driver.UserID)

	notification := models.RideRequestNotification{
		RideID:         ride.ID,
		PickupLat:      ride.PickupLat,
		PickupLng:      ride.PickupLng,
		DropoffLat:     ride.DropoffLat,
		DropoffLng:     ride.DropoffLng,
		PickupAddress:  ride.PickupAddress,
		DropoffAddress: ride.DropoffAddress,
		EstimatedFare:  ride.EstimatedFare,
		DistanceKm:     ride.DistanceKm,
		RiderName:      user.User.Name,
	}

	msg := ws.Message{
		Type:    ws.MessageTypeRideRequest,
		Payload: notification,
	}

	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal ride request: %v", err)
	}

	sent := s.hub.SendToDriver(driver.UserID, bytes)
	if !sent {
		fmt.Printf("Driver %s not reachable in hub — trying next\n", driver.UserID)
		return s.notifyNextDriver(ctx, ride)
	}

	fmt.Printf("Ride request sent to driver %s\n", driver.UserID)

	go s.startDriverTimeout(driver.UserID, ride)

	return nil
}

func (s *RideService) startDriverTimeout(driverID uuid.UUID, ride models.Ride) {
	time.Sleep(20 * time.Second)

	ctx := context.Background()

	currentRide, err := s.rideRepo.GetRideByID(ctx, ride.ID)
	if err != nil || currentRide.Status != models.RideStatusPending {
		return
	}

	s.notifyNextDriver(ctx, currentRide)
}

func (s *RideService) HandleRideResponse(ctx context.Context, userID uuid.UUID, input models.RideResponseInput) error {
	ride, err := s.rideRepo.GetRideByID(ctx, input.RideID)
	if err != nil {
		return fmt.Errorf("ride not found: %v", err)
	}

	if ride.Status != models.RideStatusPending {
		return errors.New("ride is no longer available")
	}

	driver, err := s.driverRepo.GetDriverByUserID(userID)
	if err != nil {
		return fmt.Errorf("driver not found for userID %s: %v", userID, err)
	}

	fmt.Printf("userID: %s → driver.ID: %s\n", userID, driver.ID)

	switch input.Action {
	case "accept":
		return s.acceptRide(ctx, ride, driver.ID)
	case "reject":
		return s.rejectRide(ctx, ride)
	default:
		return errors.New("invalid action, must be accept or reject")
	}
}

func (s *RideService) acceptRide(ctx context.Context, ride models.Ride, driverID uuid.UUID) error {

	if err := s.rideRepo.UpdateRideStatus(ctx, ride.ID, models.RideStatusAccepted, &driverID); err != nil {
		return err
	}

	s.rideRepo.DeleteRideCandidates(ctx, ride.ID)

	// if ride.RideMode == "pool" && ride.PoolGroupID != nil {

	// }
	if ride.RideMode != "pool" {
		driver, err := s.driverRepo.GetUserByDriverID(driverID)
		if err != nil {
			return fmt.Errorf("failed to get driver: %v", err)
		}

		go s.chatSvc.OpenSession(context.Background(), ride.ID, driver.UserID, ride.RiderID)
	}

	driver, err := s.driverRepo.GetUserByDriverID(driverID)
	if err != nil {
		return fmt.Errorf("failed to get driver: %v", err)
	}

	vehicleMake, vehicleModel, plateNumber, vehicleColor := "", "", "", ""
	vehicle, err := s.driverRepo.GetDriverVehicle(driverID)
	if err == nil {
		vehicleMake = vehicle.Make
		vehicleModel = vehicle.Model
		plateNumber = vehicle.PlateNumber
		vehicleColor = vehicle.Color
	}

	etaMinutes := 5
	driverLoc, err := s.driverRepo.GetLocationFromRedis(ctx, driver.UserID)
	if err == nil && driverLoc != nil {
		distanceKm := HaversineDistance(driverLoc.Latitude, driverLoc.Longitude, ride.PickupLat, ride.PickupLng)
		etaMinutes = int((distanceKm / 30.0) * 60)
		if etaMinutes < 1 {
			etaMinutes = 1
		}
	}

	payload := ws.RideAcceptedPayload{
		RideID:       ride.ID.String(),
		DriverName:   driver.User.Name,
		DriverPhone:  driver.User.Phone,
		Rating:       driver.Rating,
		ETA:          etaMinutes,
		VehicleMake:  vehicleMake,
		VehicleModel: vehicleModel,
		PlateNumber:  plateNumber,
		VehicleColor: vehicleColor,
	}

	msg := ws.Message{
		Type:    ws.MessageTypeRideAccepted,
		Payload: payload,
	}

	bytes, _ := json.Marshal(msg)

	sent := s.riderHub.SendToRider(ride.RiderID, bytes)

	go fcm.Send(ctx, ride.Rider.FCMToken, "Ride Accepted", fmt.Sprintf("Your ride has been accepted by %s. ETA: %d minutes", driver.User.Name, etaMinutes), map[string]string{
		"type":    string(models.RideStatusAccepted),
		"ride_id": ride.ID.String(),
	})

	riderEmail := ""
	if ride.Rider.Email != nil {
		riderEmail = *ride.Rider.Email
	}

	if riderEmail != "" {
		workers.EmailQueue <- jobs.EmailJob{
			Type: jobs.EmailTypeRideConfirmation,
			To:   riderEmail,
			Name: ride.Rider.Name,
			RideConfirmationData: jobs.RideConfirmationData{
				RiderName:      ride.Rider.Name,
				DriverName:     driver.User.Name,
				VehicleMake:    vehicleMake,
				VehicleModel:   vehicleModel,
				PlateNumber:    plateNumber,
				VehicleColor:   vehicleColor,
				PickupAddress:  ride.PickupAddress,
				DropoffAddress: ride.DropoffAddress,
				EstimatedFare:  ride.EstimatedFare,
				ETA:            etaMinutes,
			},
		}
	}

	fmt.Printf("Ride %s accepted by driver %s - rider notified: %v\n", ride.ID, driverID, sent)

	return nil
}

func (s *RideService) rejectRide(ctx context.Context, ride models.Ride) error {

	return s.notifyNextDriver(ctx, ride)
}

func (s *RideService) DriverArrived(ctx context.Context, driverUserID uuid.UUID, rideID uuid.UUID) error {

	// Fetch driver and ride concurrently so we don't pay two sequential
	// Neon round-trips on the latency-critical path.
	var (
		driver  models.Driver
		ride    models.Ride
		driErr  error
		rideErr error
		wg      sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		driver, driErr = s.driverRepo.GetDriverByUserID(driverUserID)
	}()
	go func() {
		defer wg.Done()
		ride, rideErr = s.rideRepo.GetRideByID(ctx, rideID)
	}()
	wg.Wait()

	if driErr != nil {
		return fmt.Errorf("driver not found for userID %s: %v", driverUserID, driErr)
	}
	if rideErr != nil {
		return fmt.Errorf("ride not found: %v", rideErr)
	}

	if ride.Status != models.RideStatusAccepted {
		return errors.New("ride is not in accepted state")
	}

	if ride.DriverID == nil || *ride.DriverID != driver.ID {
		return errors.New("you are not assigned to this ride")
	}

	// Notify rider that driver has arrived — this is the line the rider's
	// dashboard is waiting on, so it runs before any other network call.
	msg := ws.Message{
		Type: ws.MessageTypeDriverIsHere,
		Payload: map[string]string{
			"ride_id": ride.ID.String(),
			"message": "Your driver has arrived at the pickup location.",
		},
	}

	bytes, _ := json.Marshal(msg)
	s.riderHub.SendToRider(ride.RiderID, bytes)

	// Push notification is a call out to Google's FCM servers; never let it
	// sit in front of (or block) the WebSocket event.
	fcmToken := ride.Rider.FCMToken
	rideIDStr := ride.ID.String()
	go func() {
		fcm.Send(context.Background(), fcmToken, "Your driver has arrived", "Your driver is waiting for you at the pickup location.", map[string]string{
			"type":    "driver_arrived",
			"ride_id": rideIDStr,
		})
	}()

	fmt.Printf("Driver %s arrived for ride %s\n", driver.ID, rideID)

	return nil
}

func (s *RideService) StartTrip(ctx context.Context, driverUserID uuid.UUID, rideID uuid.UUID) error {

	var (
		driver  models.Driver
		ride    models.Ride
		driErr  error
		rideErr error
		wg      sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		driver, driErr = s.driverRepo.GetDriverByUserID(driverUserID)
	}()
	go func() {
		defer wg.Done()
		ride, rideErr = s.rideRepo.GetRideByID(ctx, rideID)
	}()
	wg.Wait()

	if driErr != nil {
		return fmt.Errorf("driver not found for userID %s: %v", driverUserID, driErr)
	}
	if rideErr != nil {
		return fmt.Errorf("ride not found: %v", rideErr)
	}

	if ride.Status != models.RideStatusAccepted {
		return errors.New("ride is not in accepted state")
	}

	if ride.DriverID == nil || *ride.DriverID != driver.ID {
		return errors.New("you are not assigned to this ride")
	}

	// Notify the rider FIRST. The "trip started" event carries no data that
	// depends on the DB write, so there's no reason to make the rider wait
	// for Neon to commit before their screen updates.
	msg := ws.Message{
		Type: ws.MessageTypeRideStarted,
		Payload: map[string]string{
			"ride_id": ride.ID.String(),
			"message": "Your trip has started.",
		},
	}
	bytes, _ := json.Marshal(msg)
	s.riderHub.SendToRider(ride.RiderID, bytes)

	// Persist the status change off the hot path. If it fails we log it;
	// it does not need to block the rider's UI.
	rideIDCopy := rideID
	go func() {
		if err := s.rideRepo.UpdateRideStatus(context.Background(), rideIDCopy, models.RideStatusOngoing, nil); err != nil {
			log.Printf("failed to update ride %s status to ongoing: %v", rideIDCopy, err)
		}
	}()

	fcmToken := ride.Rider.FCMToken
	rideIDStr := ride.ID.String()
	go func() {
		fcm.Send(context.Background(), fcmToken, "Your trip has started", "Have a safe trip!", map[string]string{
			"type":    "trip_started",
			"ride_id": rideIDStr,
		})
	}()

	fmt.Printf("Trip started for ride %s\n", rideID)

	return nil

}

func (s *RideService) EndTrip(ctx context.Context, driverUserID uuid.UUID, rideID uuid.UUID) error {

	driver, err := s.driverRepo.GetDriverByUserID(driverUserID)
	if err != nil {
		return fmt.Errorf("driver not found for userID %s: %v", driverUserID, err)
	}

	ride, err := s.rideRepo.GetRideByID(ctx, rideID)

	if err != nil {
		return fmt.Errorf("ride not found: %v", err)
	}

	if ride.Status != models.RideStatusOngoing {
		return errors.New("ride is not in ongoing state")
	}

	if ride.DriverID == nil || *ride.DriverID != driver.ID {
		return errors.New("you are not assigned to this ride")
	}

	if ride.RideMode != "pool" {
		go s.chatSvc.CloseSession(
			context.Background(),
			rideID,
			ride.RiderID,
			driverUserID,
		)
	}

	actualFare := ride.EstimatedFare

	// Update ride status to Completed
	// if err := s.rideRepo.UpdateRideStatus(ctx, rideID, models.RideStatusCompleted, nil); err != nil {
	// 	return fmt.Errorf("failed to update ride status: %v", err)
	// }

	if err := s.rideRepo.CompleteRide(ctx, rideID, actualFare); err != nil {
		return fmt.Errorf("failed to complete ride: %v", err)
	}

	// update driver total trips

	if err := s.updateDriverNoOFTrips(ctx, driver.ID); err != nil {
		fmt.Printf("failed to update driver's total trips: %v", err)
	}

	if err := s.rideRepo.UpdateRideStatus(ctx, rideID, models.RideStatusCompleted, nil); err != nil {
		return fmt.Errorf("failed to update ride status: %v", err)
	}

	completedMsg := ws.Message{
		Type: ws.MessageTypeRideCompleted,
		Payload: ws.RideCompletedPayload{
			RideID:     rideID.String(),
			ActualFare: actualFare,
			DistanceKm: ride.DistanceKm,
		},
	}

	completedBytes, _ := json.Marshal(completedMsg)
	s.riderHub.SendToRider(ride.RiderID, completedBytes)

	ratingPrompt := ws.Message{
		Type: ws.MessageTypeRateDriver,
		Payload: map[string]string{
			"ride_id": rideID.String(),
			"message": "How was your trip? Rate your driver",
		},
	}
	ratingBytes, _ := json.Marshal(ratingPrompt)
	s.riderHub.SendToRider(ride.RiderID, ratingBytes)

	// FCM is a call to Google's servers — run it async so it can't add
	// latency to the events the rider's app is waiting on.
	fcmToken := ride.Rider.FCMToken
	rideIDStr := ride.ID.String()
	go func() {
		fcm.Send(context.Background(), fcmToken, "Your trip has completed", "Thank you for riding with us!", map[string]string{
			"type":    string(models.RideStatusCompleted),
			"ride_id": rideIDStr,
		})
	}()

	// driverRatingPrompt := ws.Message{
	//     Type: ws.MessageTypeRateRider,
	//     Payload: map[string]string{
	//         "ride_id": rideID.String(),
	//         "message": "Rate your rider",
	//     },
	// }
	// driverRatingBytes, _ := json.Marshal(driverRatingPrompt)
	// s.hub.SendToDriver(driver.UserID, driverRatingBytes)

	riderEmail := ""
	if ride.Rider.Email != nil {
		riderEmail = *ride.Rider.Email
	}

	if riderEmail != "" {
		workers.EmailQueue <- jobs.EmailJob{
			Type: jobs.EmailTypeRideCompleted,
			To:   riderEmail,
			Name: ride.Rider.Name,
			RideCompletedData: jobs.RideCompletedData{
				RiderName:      ride.Rider.Name,
				PickupAddress:  ride.PickupAddress,
				DropoffAddress: ride.DropoffAddress,
				ActualFare:     actualFare,
				DistanceKm:     ride.DistanceKm,
			},
		}
	}

	fmt.Printf("Trip completed for ride %s with fare ₦%.2f\n", rideID, actualFare)
	return nil

}

func (s *RideService) updateDriverNoOFTrips(ctx context.Context, driverID uuid.UUID) error {

	return s.driverRepo.IncreaseDriverTrips(ctx, driverID)
}

func (s *RideService) GetRiderHistory(ctx context.Context, riderID uuid.UUID) ([]models.Ride, error) {

	return s.rideRepo.RiderHistory(ctx, riderID)
}

func (s *RideService) GetDriverHistory(ctx context.Context, driverUserID uuid.UUID) ([]models.Ride, error) {

	driver, err := s.driverRepo.GetDriverByUserID(driverUserID)
	if err != nil {
		return nil, fmt.Errorf("driver not found: %v", err)
	}

	return s.rideRepo.DriverHistory(ctx, driver.ID)
}

func (s *RideService) PushLocationToRider(ctx context.Context, driverUserID uuid.UUID, lat, lng float64) {
	driver, err := s.driverRepo.GetDriverByUserID(driverUserID)
	if err != nil {
		return
	}

	ride, err := s.rideRepo.GetOngoingRide(ctx, driver.ID)
	if err != nil {
		return
	}

	msg := ws.Message{
		Type: ws.MessageTypeDriverLocation,
		Payload: map[string]float64{
			"latitude":  lat,
			"longitude": lng,
		},
	}

	bytes, _ := json.Marshal(msg)
	s.riderHub.SendToRider(ride.RiderID, bytes)
}

func (s *RideService) FindAndNotifyDrivers(ctx context.Context, ride models.Ride) {
	nearestDrivers, err := s.findNearestDrivers(ctx, ride.PickupLat, ride.PickupLng, 3)
	if err != nil || len(nearestDrivers) == 0 {
		s.rideRepo.UpdateRideStatus(ctx, ride.ID, models.RideStatusCancelled, nil)
		s.notifyRider(ride.RiderID, ws.Message{
			Type: ws.MessageTypeNoCandidates,
			Payload: map[string]string{
				"ride_id": ride.ID.String(),
				"message": "No drivers available at this time.",
			},
		})
		fmt.Printf("No drivers found for ride %s\n", err)
		return
	}

	if err := s.rideRepo.SaveRideCandidates(ctx, ride.ID, nearestDrivers); err != nil {
		fmt.Printf("Failed to save candidates for ride %s: %v\n", ride.ID, err)
		return
	}

	if err := s.notifyNextDriver(ctx, ride); err != nil {

		fmt.Printf("Failed to notify driver for ride %s: %v\n", ride.ID, err)
	}

}

func (s *RideService) RunScheduledRides() {

	ctx := context.Background()
	rides, err := s.rideRepo.GetDueScheduledRides(ctx)
	if err != nil {
		fmt.Printf("[ScheduledRideWorker] error: %v\n", err)
		return
	}

	for _, ride := range rides {
		if err := s.rideRepo.UpdateRideStatus(ctx, ride.ID, models.RideStatusPending, nil); err != nil {
			fmt.Printf("failed to update ride %s: %v\n", ride.ID, err)
			continue
		}

		ride.Status = models.RideStatusPending

		go s.tryNotifyDriversWithRetry(ctx, ride, 3, 20*time.Second)
	}
}

func (s *RideService) tryNotifyDriversWithRetry(ctx context.Context, ride models.Ride, maxRetries int, delay time.Duration) {
	for i := 0; i < maxRetries; i++ {
		nearestDrivers, err := s.findNearestDrivers(ctx, ride.PickupLat, ride.PickupLng, 3)
		if err == nil && len(nearestDrivers) > 0 {
			s.rideRepo.SaveRideCandidates(ctx, ride.ID, nearestDrivers)
			s.notifyNextDriver(ctx, ride)
			return
		}

		time.Sleep(delay)
	}

	s.rideRepo.UpdateRideStatus(ctx, ride.ID, models.RideStatusCancelled, nil)
	s.notifyRider(ride.RiderID, ws.Message{
		Type: ws.MessageTypeNoCandidates,
		Payload: map[string]string{
			"ride_id": ride.ID.String(),
			"message": "Sorry, no drivers available at this time.",
		},
	})

	fmt.Printf("Ride %s cancelled after %d retries\n", ride.ID, maxRetries)
}

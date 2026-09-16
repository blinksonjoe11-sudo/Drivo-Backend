package handler

import (
	"context"
	"drivo/internal/middleware"
	"drivo/internal/models"
	"drivo/internal/service"
	"drivo/internal/ws"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type WSHandler struct {
	hub       *ws.Hub
	riderHub  *ws.RiderHub
	driverSvc *service.DriverService
	rideSvc   *service.RideService
	poolSvc   *service.PoolService
	chatSvc   *service.ChatService
}

func NewWSHandler(hub *ws.Hub, riderHub *ws.RiderHub, driverSvc *service.DriverService, rideSvc *service.RideService, poolSvc *service.PoolService, chatSvc *service.ChatService) *WSHandler {
	return &WSHandler{
		hub:       hub,
		riderHub:  riderHub,
		driverSvc: driverSvc,
		rideSvc:   rideSvc,
		poolSvc:   poolSvc,
		chatSvc:   chatSvc,
	}
}

func (h *WSHandler) DriverConnect(c *gin.Context) {
	userID, ok := middleware.GetUserId(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}

	driverID, _ := uuid.Parse(userID)

	fmt.Printf("driver connected id is : %v", driverID)

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	if err := h.driverSvc.OnlineStatus(models.StatusUpdate{IsOnline: true}, driverID); err != nil {
		log.Printf("failed to set driver online: %v", err)
	}

	client := &ws.Client{
		Hub:      h.hub,
		DriverID: driverID,
		Conn:     conn,
		Send:     make(chan []byte, 256),
	}
	h.hub.Register <- client

	go client.WritePump()

	client.ReadPump(func(driverID uuid.UUID, msg ws.Message) {
		h.handleMessage(driverID, msg)
	})

	if err := h.driverSvc.OnlineStatus(models.StatusUpdate{IsOnline: false}, driverID); err != nil {
		log.Printf("failed to set driver offline: %v", err)
	}
}

func (h *WSHandler) RiderConnect(c *gin.Context) {
	userID, ok := middleware.GetUserId(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}

	riderID, _ := uuid.Parse(userID)

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Rider WebSocket upgrade failed: %v", err)
		return
	}

	client := &ws.RiderClient{
		Hub:     h.riderHub,
		RiderID: riderID,
		Send:    make(chan []byte, 256),
	}

	h.riderHub.Register <- client

	go h.riderWritePump(client, conn)
	h.riderReadPump(client, conn)
}

func (h *WSHandler) riderWritePump(client *ws.RiderClient, conn *websocket.Conn) {
	ticker := time.NewTicker(50 * time.Second)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()

	for {
		select {
		case message, ok := <-client.Send:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			conn.WriteMessage(websocket.TextMessage, message)

		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *WSHandler) riderReadPump(client *ws.RiderClient, conn *websocket.Conn) {
	defer func() {
		h.riderHub.Unregister <- client
		conn.Close()
	}()

	conn.SetReadLimit(10240)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, data, err := conn.ReadMessage()

		if err != nil {
			break
		}

		var msg ws.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("invalid message from rider %s: %v", client.RiderID, err)
			continue
		}

		h.handleRiderMessage(client.RiderID, msg)

	}
}

func (h *WSHandler) handleMessage(driverID uuid.UUID, msg ws.Message) {
	switch msg.Type {
	case ws.MessageTypeLocation:
		h.handleLocationUpdate(driverID, msg.Payload)
	case ws.MessageTypeRideResponse:
		h.handleRideResponse(driverID, msg.Payload)
	case ws.MessageTypeDriverArrived:
		h.handleDriverArrived(driverID, msg.Payload)
	case ws.MessageTypeStartTrip:
		h.handleStartTrip(driverID, msg.Payload)
	case ws.MessageTypeEndTrip:
		h.handleEndTrip(driverID, msg.Payload)
	case ws.MessageTypePoolRideStarted:
		h.handlePoolStartTrip(driverID, msg.Payload)
	case ws.MessageTypePoolCancelled:
		h.handlePoolCancelTrip(driverID, msg.Payload)
	case ws.MessageTypePoolRideCompleted:
		h.handlePoolCompleteTrip(driverID, msg.Payload)
	case ws.MessageTypeChatMessage:
		h.handleDriverChatMessage(driverID, msg.Payload)

	case "ping":

	default:
		log.Printf("unknown message type from driver %s: %s", driverID, msg.Type)
	}
}

func (h *WSHandler) handleRiderMessage(riderID uuid.UUID, msg ws.Message) {
	switch msg.Type {
	case ws.MessageTypeRiderLocation:
		h.handleRiderLocationUpdate(riderID, msg.Payload)
	case ws.MessageTypeChatMessage:
		h.handleRiderChatMessage(riderID, msg.Payload)
	case "ping":

	default:
		log.Printf("unknown message type from driver %s: %s", riderID, msg.Type)
	}
}

func (h *WSHandler) handleRiderLocationUpdate(riderID uuid.UUID, payload interface{}) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	var loc ws.LocationPayload
	if err := json.Unmarshal(data, &loc); err != nil {
		return
	}
	ctx := context.Background()

	if err := h.driverSvc.UpdateRiderLocation(ctx, riderID, loc.Latitude, loc.Longitude); err != nil {
		log.Printf("failed to save location for rider %s: %v", riderID, err)
	}
}

func (h *WSHandler) handleLocationUpdate(driverID uuid.UUID, payload interface{}) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}

	var loc ws.LocationPayload
	if err := json.Unmarshal(raw, &loc); err != nil {
		log.Printf("invalid location from driver %s: %v", driverID, err)
		return
	}

	ctx := context.Background()

	if err := h.driverSvc.UpdateLocation(ctx, driverID, loc.Latitude, loc.Longitude); err != nil {
		log.Printf("failed to save location for driver %s: %v", driverID, err)
	}

	h.rideSvc.PushLocationToRider(ctx, driverID, loc.Latitude, loc.Longitude)
}

func (h *WSHandler) handleRideResponse(driverUserID uuid.UUID, payload interface{}) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}

	var input models.RideResponseInput
	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("invalid ride response: %v", err)
		return
	}

	if err := h.rideSvc.HandleRideResponse(context.Background(), driverUserID, input); err != nil {
		log.Printf("failed to handle ride response from userID %s: %v", driverUserID, err)
	}
}

func (h *WSHandler) handleDriverArrived(driverUserID uuid.UUID, payload interface{}) {
	raw, _ := json.Marshal(payload)

	var input ws.TripActionPayload
	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("invalid driver arrived payload from driver %s: %v", driverUserID, err)
		return
	}

	rideID, err := uuid.Parse(input.RideID)

	if err != nil {
		log.Printf("invalid ride ID in driver arrived payload from driver %s: %v", driverUserID, err)
		return
	}

	if err := h.rideSvc.DriverArrived(context.Background(), driverUserID, rideID); err != nil {
		log.Printf("failed to handle driver arrived from driver %s: %v", driverUserID, err)
	}

}

func (h *WSHandler) handleStartTrip(driverUserID uuid.UUID, payload interface{}) {

	raw, _ := json.Marshal(payload)
	var input ws.TripActionPayload
	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("invalid start_trip payload: %v", err)
		return
	}

	rideID, err := uuid.Parse(input.RideID)

	if err != nil {
		log.Printf("invalid ride_id: %v", err)
		return
	}

	if err := h.rideSvc.StartTrip(context.Background(), driverUserID, rideID); err != nil {
		log.Printf("failed to start trip for driver %s: %v", driverUserID, err)
	}

}

func (h *WSHandler) handlePoolStartTrip(driverUserID uuid.UUID, payload interface{}) {

	raw, _ := json.Marshal(payload)

	var input ws.PoolTripActionPayload

	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("invalid pool start_trip payload from driver %s: %v", driverUserID, err)
		return
	}

	poolID, err := uuid.Parse(input.PoolID)

	if err != nil {
		log.Printf("invalid pool_id: %v", err)
		return
	}

	if err := h.poolSvc.StartPoolTrip(context.Background(), poolID, driverUserID); err != nil {
		log.Printf("failed to start pool trip for driver %s: %v", driverUserID, err)
	}
}

func (h *WSHandler) handleEndTrip(driverUserID uuid.UUID, payload interface{}) {
	raw, _ := json.Marshal(payload)
	var input ws.TripActionPayload
	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("invalid end_trip payload: %v", err)
		return
	}

	rideID, err := uuid.Parse(input.RideID)
	if err != nil {
		log.Printf("invalid ride_id: %v", err)
		return
	}

	if err := h.rideSvc.EndTrip(context.Background(), driverUserID, rideID); err != nil {
		log.Printf("end_trip error: %v", err)
	}
}

func (h *WSHandler) handlePoolCompleteTrip(driverUserID uuid.UUID, payload interface{}) {

	raw, _ := json.Marshal(payload)
	var input ws.PoolTripActionPayload
	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("invalid end_trip payload: %v", err)
		return
	}

	poolID, err := uuid.Parse(input.PoolID)
	if err != nil {
		log.Printf("invalid ride_id: %v", err)
		return
	}

	if err := h.poolSvc.CompleteTrip(context.Background(), poolID, driverUserID); err != nil {
		log.Printf("end_trip error: %v", err)
	}
}

func (h *WSHandler) handlePoolCancelTrip(driverUserID uuid.UUID, payload interface{}) {

	raw, _ := json.Marshal(payload)
	var input ws.PoolTripActionPayload
	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("invalid end_trip payload: %v", err)
		return
	}

	poolID, err := uuid.Parse(input.PoolID)
	if err != nil {
		log.Printf("invalid ride_id: %v", err)
		return
	}

	if err := h.poolSvc.CancelPoolTrip(context.Background(), poolID, driverUserID); err != nil {
		log.Printf("end_trip error: %v", err)
	}
}

func (h *WSHandler) handleDriverChatMessage(driverUserID uuid.UUID, payload interface{}) {
	raw, _ := json.Marshal(payload)
	var input struct {
		RideID  string `json:"ride_id"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("Chat invalid payload from driver %s: %v", driverUserID, err)
		return
	}
	rideID, err := uuid.Parse(input.RideID)
	if err != nil {
		return
	}
	if err := h.chatSvc.SendMessage(context.Background(), service.SendMessageInput{
		RideID:     rideID,
		SenderID:   driverUserID,
		SenderType: models.SenderTypeDriver,
		Message:    input.Message,
	}); err != nil {
		log.Printf("Chat driver send error: %v", err)
	}
}

func (h *WSHandler) handleRiderChatMessage(riderID uuid.UUID, payload interface{}) {
	raw, _ := json.Marshal(payload)
	var input struct {
		RideID  string `json:"ride_id"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		log.Printf("[Chat] invalid payload from rider %s: %v", riderID, err)
		return
	}
	rideID, err := uuid.Parse(input.RideID)
	if err != nil {
		return
	}
	if err := h.chatSvc.SendMessage(context.Background(), service.SendMessageInput{
		RideID:     rideID,
		SenderID:   riderID,
		SenderType: models.SenderTypeRider,
		Message:    input.Message,
	}); err != nil {
		log.Printf("[Chat] rider send error: %v", err)
	}
}

func (h *WSHandler) GetChatHistory(c *gin.Context) {
	userID, ok := middleware.GetUserId(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	requesterID, _ := uuid.Parse(userID)

	rideID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ride id"})
		return
	}

	messages, err := h.chatSvc.GetHistory(c.Request.Context(), rideID, requesterID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"messages": messages})
}

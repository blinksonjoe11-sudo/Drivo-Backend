package service

import (
	"context"
	"drivo/internal/models"
	"drivo/internal/repository"
	"drivo/internal/ws"
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
)

type ChatService struct {
	chatRepo  *repository.ChatRepo
	riderHub  *ws.RiderHub
	driverHub *ws.Hub
}

func NewChatService(
	chatRepo *repository.ChatRepo,
	riderHub *ws.RiderHub,
	driverHub *ws.Hub,
) *ChatService {
	return &ChatService{
		chatRepo:  chatRepo,
		riderHub:  riderHub,
		driverHub: driverHub,
	}
}

type SendMessageInput struct {
	RideID     uuid.UUID
	SenderID   uuid.UUID
	SenderType models.SenderType
	Message    string
}

func (s *ChatService) notifyRider(riderID uuid.UUID, msg ws.Message) {
	bytes, _ := json.Marshal(msg)
	ok := s.riderHub.SendToRider(riderID, bytes)
	log.Printf("[CHAT] notifyRider %s delivered=%v", riderID, ok)
}

func (s *ChatService) notifyDriver(driverUserID uuid.UUID, msg ws.Message) {
	bytes, _ := json.Marshal(msg)
	ok := s.driverHub.SendToDriver(driverUserID, bytes)
	log.Printf("[CHAT] notifyDriver %s delivered=%v", driverUserID, ok)
}

func (s *ChatService) OpenSession(ctx context.Context, rideID, driverUserID, riderID uuid.UUID) (*models.ChatSession, error) {
	session, err := s.chatRepo.CreateSession(ctx, rideID, driverUserID, riderID)
	if err != nil {
		return nil, fmt.Errorf("failed to create chat session: %v", err)
	}

	fmt.Printf("Chat session opened for ride %s\n", rideID)

	payload := map[string]interface{}{
		"session_id": session.ID,
		"ride_id":    rideID,
		"message":    "Chat is now available",
	}

	s.notifyRider(riderID, ws.Message{Type: ws.MessageTypeChatMessage, Payload: payload})
	s.notifyDriver(driverUserID, ws.Message{Type: ws.MessageTypeChatMessage, Payload: payload})

	return session, nil
}

func (s *ChatService) SendMessage(ctx context.Context, input SendMessageInput) error {
	if input.Message == "" {
		return fmt.Errorf("message cannot be empty")
	}
	if len(input.Message) > 500 {
		return fmt.Errorf("message too long, max 500 characters")
	}

	session, err := s.chatRepo.GetSessionByRideID(ctx, input.RideID)
	if err != nil {
		return fmt.Errorf("chat session not found for ride %s", input.RideID)
	}

	if !session.IsActive {
		return fmt.Errorf("chat is closed, trip has ended")
	}

	log.Printf("[CHAT] SendMessage from %s (%s) on ride %s → broadcasting", input.SenderID, input.SenderType, input.RideID)

	if input.SenderType == models.SenderTypeDriver && session.DriverID != input.SenderID {
		return fmt.Errorf("you are not the driver on this ride")
	}
	if input.SenderType == models.SenderTypeRider && session.RiderID != input.SenderID {
		return fmt.Errorf("you are not the rider on this ride")
	}

	msg := &models.ChatMessage{
		SessionID:  session.ID,
		SenderID:   input.SenderID,
		SenderType: input.SenderType,
		Message:    input.Message,
		IsRead:     false,
	}

	if err := s.chatRepo.SaveMessage(ctx, *msg); err != nil {
		return fmt.Errorf("failed to save message: %v", err)
	}

	payload := map[string]interface{}{
		"id":          msg.ID,
		"session_id":  session.ID,
		"ride_id":     input.RideID,
		"sender_id":   input.SenderID,
		"sender_type": input.SenderType,
		"message":     input.Message,
		"created_at":  msg.CreatedAt,
	}

	if input.SenderType == models.SenderTypeDriver {
		s.notifyRider(session.RiderID, ws.Message{Type: ws.MessageTypeChatMessage, Payload: payload})
	} else {
		s.notifyDriver(session.DriverID, ws.Message{Type: ws.MessageTypeChatMessage, Payload: payload})
	}

	return nil

}

func (s *ChatService) GetHistory(ctx context.Context, rideID uuid.UUID, requesterID uuid.UUID) ([]models.ChatMessage, error) {

	session, err := s.chatRepo.GetSessionByRideID(ctx, rideID)
	if err != nil {
		return nil, fmt.Errorf("chat session not found for ride %s", rideID)
	}

	if session.DriverID != requesterID && session.RiderID != requesterID {
		return nil, fmt.Errorf("you are not a participant in this chat")
	}

	messages, err := s.chatRepo.GetMessages(ctx, session.ID)
	if err != nil {
		return nil, err
	}

	go s.chatRepo.MarkMessagesRead(context.Background(), session.ID, requesterID)

	return messages, nil
}

func (s *ChatService) CloseSession(ctx context.Context, rideID uuid.UUID, riderID uuid.UUID, driverUserID uuid.UUID) error {

	if err := s.chatRepo.CloseSession(ctx, rideID); err != nil {
		return fmt.Errorf("failed to close chat session: %v", err)
	}

	closedMsg := ws.Message{
		Type: ws.MessageTypeChatClosed,
		Payload: map[string]interface{}{
			"ride_id": rideID,
			"message": "Chat has been closed",
		},
	}

	s.notifyDriver(driverUserID, closedMsg)
	s.notifyRider(riderID, closedMsg)

	return nil
}

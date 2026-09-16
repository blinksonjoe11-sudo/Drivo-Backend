package ws

import (
	"log"
	"sync"

	"github.com/google/uuid"
)

type RiderHub struct {
	clients    map[uuid.UUID]*RiderClient
	Register   chan *RiderClient
	Unregister chan *RiderClient
	mu         sync.RWMutex
}

type RiderClient struct {
	Hub     *RiderHub
	RiderID uuid.UUID
	Send    chan []byte
}

func NewRiderHub() *RiderHub {
	return &RiderHub{
		clients:    make(map[uuid.UUID]*RiderClient),
		Register:   make(chan *RiderClient),
		Unregister: make(chan *RiderClient),
	}
}

func (h *RiderHub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.mu.Lock()
			h.clients[client.RiderID] = client
			h.mu.Unlock()

		case client := <-h.Unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.RiderID]; ok {
				delete(h.clients, client.RiderID)
				close(client.Send)
			}
			h.mu.Unlock()
		}
	}
}

func (h *RiderHub) SendToRider(riderID uuid.UUID, message []byte) bool {
	h.mu.RLock()
	client, ok := h.clients[riderID]
	h.mu.RUnlock()

	if !ok {
		return false
	}

	// Try to enqueue. If the buffer is momentarily full (e.g. a burst of
	// location frames while the client reads slowly), drop the OLDEST queued
	// message to make room rather than disconnecting the rider — otherwise a
	// brief backlog silently kills the connection and important events like
	// "driver arrived" never arrive until the client reconnects.
	for i := 0; i < 2; i++ {
		select {
		case client.Send <- message:
			return true
		default:
			log.Printf("rider %s send buffer full, dropping message", riderID)
			return false
		}
	}
	return false
}

func (h *RiderHub) IsOnline(riderID uuid.UUID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.clients[riderID]
	return ok
}

func (h *RiderHub) GetOnlineRiderIDs() []uuid.UUID {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]uuid.UUID, 0, len(h.clients))
	for id := range h.clients {
		ids = append(ids, id)
	}
	return ids
}

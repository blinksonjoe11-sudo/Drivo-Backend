package ws

import (
	"log"
	"sync"

	"github.com/google/uuid"
)

type Hub struct {
	clients    map[uuid.UUID]*Client
	Register   chan *Client
	Unregister chan *Client
	mu         sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[uuid.UUID]*Client),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.mu.Lock()
			h.clients[client.DriverID] = client
			h.mu.Unlock()

		case client := <-h.Unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.DriverID]; ok {
				delete(h.clients, client.DriverID)
				close(client.Send)
			}
			h.mu.Unlock()
		}
	}
}

func (h *Hub) SendToDriver(driverID uuid.UUID, message []byte) bool {
	h.mu.RLock()
	client, ok := h.clients[driverID]
	h.mu.RUnlock()

	if !ok {
		return false
	}

	// Drop the oldest queued message on a momentarily full buffer instead of
	// disconnecting the driver (see RiderHub.SendToRider for rationale).
	for i := 0; i < 2; i++ {
		select {
		case client.Send <- message:
			return true
		default:
			log.Printf("driver %s send buffer full, dropping message", driverID)
			return false
		}
	}
	return false
}

func (h *Hub) IsOnline(driverID uuid.UUID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.clients[driverID]
	return ok
}

func (h *Hub) GetOnlineDriverIDs() []uuid.UUID {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]uuid.UUID, 0, len(h.clients))
	for id := range h.clients {
		ids = append(ids, id)
	}
	return ids
}

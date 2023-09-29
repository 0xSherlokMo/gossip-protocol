package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Empty = struct{}

type broadcastBody struct {
	Type      string `json:"type"`
	Message   int64  `json:"message"`
	MessageID int64  `json:"msg_id"`
	Sender    string `json:"-"`
}

type MessageKeeper struct {
	messages    []int64
	broadcasted map[int64]Empty
	mu          sync.RWMutex
}

func NewMessageKeeper() MessageKeeper {
	return MessageKeeper{
		messages:    []int64{},
		broadcasted: make(map[int64]Empty),
	}
}

func (m *MessageKeeper) Append(message int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, message)
}

func (m *MessageKeeper) SetBroadcasted(message int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broadcasted[message] = Empty{}
}

func (m *MessageKeeper) Broadcasted(message int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.broadcasted[message]
	return ok
}

func (m *MessageKeeper) All() []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.messages
}

type NodeState struct {
	Messages MessageKeeper
	Topology map[string][]string
}

func (s *NodeState) Gossip(node *maelstrom.Node, body broadcastBody) {
	s.Messages.SetBroadcasted(body.Message)
	request := map[string]any{
		"type":    "broadcast",
		"message": body.Message,
	}

	for _, neighbor := range s.Topology[node.ID()] {
		if neighbor == body.Sender {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()
		for {
			_, err := node.SyncRPC(ctx, neighbor, request)
			if err == nil {
				break
			}
			time.Sleep(time.Duration(500) * time.Millisecond)
		}
	}
}

func NewState() *NodeState {
	return &NodeState{
		Messages: NewMessageKeeper(),
	}
}

var State = NewState()

func main() {
	node := maelstrom.NewNode()

	node.Handle("broadcast", func(request maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(request.Body, &body); err != nil {
			return err
		}
		body.Sender = request.Src

		node.Reply(request, map[string]any{
			"type": "broadcast_ok",
		})

		if ok := State.Messages.Broadcasted(body.Message); ok {
			return nil
		}

		State.Messages.Append(body.Message)
		State.Gossip(node, body)

		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{
			"type":     "read_ok",
			"messages": State.Messages.All(),
		}

		return node.Reply(msg, response)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		type topologyMessage struct {
			Type     string              `json:"type"`
			Topology map[string][]string `json:"topology"`
		}
		var body topologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		State.Topology = body.Topology

		response := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, response)
	})

	node.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

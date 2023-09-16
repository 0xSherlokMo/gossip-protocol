package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Empty = struct{}

type broadcastBody struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type NodeState struct {
	Messages    []int64
	Broadcasted map[int64]Empty
	Neghbors    []string
}

func (s *NodeState) AckMessage(message int64) {
	if _, ok := s.Broadcasted[message]; ok {
		return
	}

	s.Messages = append(s.Messages, message)
	s.Broadcasted[message] = Empty{}
}

func (s *NodeState) Gossip(node *maelstrom.Node, message int64) {
	if _, ok := s.Broadcasted[message]; ok {
		return
	}

	request := map[string]any{
		"type":    "broadcast",
		"message": message,
	}

	for _, neighbor := range s.Neghbors {
		node.Send(neighbor, request)
	}

	s.Broadcasted[message] = Empty{}
}

func NewState() *NodeState {
	return &NodeState{
		Messages:    []int64{},
		Broadcasted: make(map[int64]Empty, 0),
	}
}

var State = NewState()

func main() {
	node := maelstrom.NewNode()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		State.AckMessage(body.Message)
		State.Gossip(node, body.Message)
		response := map[string]any{
			"type": "broadcast_ok",
		}

		return node.Reply(msg, response)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{
			"type":     "read_ok",
			"messages": State.Messages,
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

		for id, neighbors := range body.Topology {
			if node.ID() == id {
				State.Neghbors = neighbors
				break
			}
		}

		response := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

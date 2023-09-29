package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Empty = struct{}

type broadcastBody struct {
	Type      string `json:"type"`
	Message   int64  `json:"message"`
	MessageID int64  `json:"msg_id"`
	Sender    string `json:"-"`
}

type NodeState struct {
	Messages    []int64
	Broadcasted map[int64]Empty
	Topology    map[string][]string
}

func (s *NodeState) Gossip(node *maelstrom.Node, body broadcastBody) {
	s.Broadcasted[body.Message] = Empty{}
	request := map[string]any{
		"type":    "broadcast",
		"message": body.Message,
	}

	for _, neighbor := range s.Topology[node.ID()] {
		if neighbor == body.Sender {
			continue
		}
		node.Send(neighbor, request)
	}
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

	node.Handle("broadcast", func(request maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(request.Body, &body); err != nil {
			return err
		}
		body.Sender = request.Src

		if _, ok := State.Broadcasted[body.Message]; ok {
			return nil
		}

		State.Messages = append(State.Messages, body.Message)
		State.Gossip(node, body)

		if body.MessageID != 0 {
			return node.Reply(request, map[string]any{
				"type": "broadcast_ok",
			})
		}

		return nil
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

		State.Topology = body.Topology

		response := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

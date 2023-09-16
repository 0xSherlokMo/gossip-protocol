package main

import (
	"encoding/json"
	"errors"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastBody struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

var nodeNeighbors []string

var messages []int64

func main() {
	node := maelstrom.NewNode()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages = append(messages, body.Message)
		response := map[string]any{
			"type": "broadcast_ok",
		}

		return node.Reply(msg, response)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}

		return node.Reply(msg, response)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		bodyNeighbors, ok := body["neighbors"].(map[string][]string)
		if !ok {
			return errors.New("malformed_request")
		}

		for id, neighbors := range bodyNeighbors {
			if node.ID() == id {
				nodeNeighbors = neighbors
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

package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	wsconn "github.com/shiwano/websocket-conn/v3"
)

type (
	SubscriptionClient struct {
		mut           sync.Mutex
		conn          *wsconn.Conn
		subscriptions map[string]*Subscription
		nextSubID     int
	}

	SubscriptionPayload struct {
		Data  json.RawMessage
		Error json.RawMessage
	}

	Subscription struct {
		id      string
		Channel chan SubscriptionPayload
	}

	subscriptionMessageType string

	subscriptionMessage struct {
		ID      *string                 `json:"id"`
		Type    subscriptionMessageType `json:"type"`
		Payload json.RawMessage         `json:"payload"`
	}

	subscriptionRequestBody struct {
		Query     string                 `json:"query"`
		Variables map[string]interface{} `json:"variables"`
	}
)

const (
	subscriptionMessageTypeConnectionInit  = subscriptionMessageType("connection_init")      // Client -> Server
	subscriptionMessageTypeStart           = subscriptionMessageType("start")                // Client -> Server
	subscriptionMessageTypeStop            = subscriptionMessageType("stop")                 // Client -> Server
	subscriptionMessageTypeAck             = subscriptionMessageType("connection_ack")       // Server -> Client
	subscriptionMessageTerminate           = subscriptionMessageType("connection_terminate") // Client -> Server
	subscriptionMessageTypeConnectionError = subscriptionMessageType("connection_error")     // Server -> Client
	subscriptionMessageTypeData            = subscriptionMessageType("data")                 // Server -> Client
	subscriptionMessageTypeError           = subscriptionMessageType("error")                // Server -> Client
	subscriptionMessageTypeComplete        = subscriptionMessageType("complete")             // Server -> Client
)

func NewSubscriptionClient(ctx context.Context, endpoint string, header http.Header, initOptions interface{}) (*SubscriptionClient, error) {
	url := strings.Replace(endpoint, "http", "ws", 1)
	header.Set("Sec-WebSocket-Protocol", "graphql-ws")
	header.Set("Content-Type", "application/json")

	conn, _, err := wsconn.Connect(ctx, wsconn.DefaultSettings(), url, header)
	if err != nil {
		return nil, err
	}

	cli := &SubscriptionClient{
		conn:          conn,
		subscriptions: make(map[string]*Subscription),
	}

	if err := cli.handshake(initOptions); err != nil {
		return nil, err
	}
	go cli.run()
	return cli, nil
}

func (c *SubscriptionClient) Close() error {
	for _, sub := range c.subscriptions {
		c.removeSubscription(sub)
	}
	if err := c.conn.SendJSONMessage(subscriptionMessage{
		Type: subscriptionMessageTerminate,
	}); err != nil {
		return err
	}
	return nil
}

func (c *SubscriptionClient) handshake(initOptions interface{}) error {
	initPayload, err := json.Marshal(initOptions)
	if err != nil {
		return err
	}

	msg := subscriptionMessage{
		Type:    subscriptionMessageTypeConnectionInit,
		Payload: initPayload,
	}
	if err := c.conn.SendJSONMessage(msg); err != nil {
		c.conn.Close()
		return err
	}

	rawMsg := <-c.conn.Stream()
	msg = subscriptionMessage{}
	if err := rawMsg.UnmarshalAsJSON(&msg); err != nil {
		c.conn.Close()
		return err
	}

	if msg.Type != subscriptionMessageTypeAck {
		c.conn.Close()

		if msg.Type == subscriptionMessageTypeConnectionError {
			return errors.New(string(msg.Payload))
		}
		return errors.New("failed acknowledge")
	}
	return nil
}

func (c *SubscriptionClient) run() {
	defer c.Close()

	for rawMsg := range c.conn.Stream() {
		var msg subscriptionMessage
		if err := rawMsg.UnmarshalAsJSON(&msg); err != nil {
			continue
		}

		if msg.ID == nil {
			continue
		}
		id := *msg.ID

		switch msg.Type {
		case subscriptionMessageTypeError:
			sub, ok := c.subscriptions[id]
			if !ok {
				continue
			}
			sub.Channel <- SubscriptionPayload{Error: msg.Payload}
		case subscriptionMessageTypeData:
			sub, ok := c.subscriptions[id]
			if !ok {
				continue
			}
			sub.Channel <- SubscriptionPayload{Data: msg.Payload}
		case subscriptionMessageTypeComplete:
			sub, ok := c.subscriptions[id]
			if !ok {
				continue
			}
			c.removeSubscription(sub)
		}
	}
}

func (c *SubscriptionClient) Subscribe(req *Request) (*Subscription, error) {
	var reqBody bytes.Buffer
	if err := json.NewEncoder(&reqBody).Encode(subscriptionRequestBody{
		Query:     req.q,
		Variables: req.vars,
	}); err != nil {
		return nil, errors.Wrap(err, "failed to encode request body")
	}

	id := strconv.Itoa(c.nextSubID)
	c.nextSubID++

	msg := subscriptionMessage{
		ID:      &id,
		Type:    subscriptionMessageTypeStart,
		Payload: reqBody.Bytes(),
	}
	if err := c.conn.SendJSONMessage(&msg); err != nil {
		return nil, err
	}

	sub := &Subscription{
		id:      id,
		Channel: make(chan SubscriptionPayload),
	}
	c.addSubscription(sub)
	return sub, nil
}

func (c *SubscriptionClient) Unsubscribe(sub *Subscription) error {
	c.removeSubscription(sub)

	msg := subscriptionMessage{
		ID:   &sub.id,
		Type: subscriptionMessageTypeStop,
	}
	return c.conn.SendJSONMessage(&msg)
}

func (c *SubscriptionClient) addSubscription(sub *Subscription) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.subscriptions[sub.id] = sub
}

func (c *SubscriptionClient) removeSubscription(sub *Subscription) {
	c.mut.Lock()
	defer c.mut.Unlock()
	close(sub.Channel)
	delete(c.subscriptions, sub.id)
}

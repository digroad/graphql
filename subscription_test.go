package graphql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/matryer/is"
	wsconn "github.com/shiwano/websocket-conn/v4"
)

func TestSub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	is := is.New(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsconn.UpgradeFromHTTP(ctx, wsconn.DefaultSettings(), w, r)
		is.NoErr(err)
		defer conn.Close()

		m := <-conn.Stream()
		initMsg := subscriptionMessage{}
		is.NoErr(m.UnmarshalAsJSON(&initMsg))
		is.Equal(initMsg.Type, subscriptionMessageTypeConnectionInit)

		ackMsg := subscriptionMessage{Type: subscriptionMessageTypeAck}
		is.NoErr(conn.SendJSONMessage(ackMsg))

		m = <-conn.Stream()
		startMsg := subscriptionMessage{}
		is.NoErr(m.UnmarshalAsJSON(&startMsg))
		is.Equal(startMsg.Type, subscriptionMessageTypeStart)
		subID := *startMsg.ID
		is.Equal(subID, "0")
		var startPayload struct {
			Query     string            `json:"query"`
			Variables map[string]string `json:"variables"`
		}
		is.NoErr(json.Unmarshal(startMsg.Payload, &startPayload))
		is.Equal(startPayload.Query, `subscription ($q: String) { cnt }`)
		is.Equal(len(startPayload.Variables), 1)
		is.Equal(startPayload.Variables["q"], "foo")

		is.NoErr(conn.SendJSONMessage(ackMsg))

		dataMsg := subscriptionMessage{
			ID:      &subID,
			Type:    subscriptionMessageTypeData,
			Payload: json.RawMessage(`{"data": "bar"}`),
		}
		is.NoErr(conn.SendJSONMessage(dataMsg))

		m = <-conn.Stream()
		stopMsg := subscriptionMessage{}
		is.NoErr(m.UnmarshalAsJSON(&stopMsg))
		is.Equal(stopMsg.Type, subscriptionMessageTypeStop)
		is.Equal(*stopMsg.ID, subID)

		completeMsg := subscriptionMessage{
			ID:   &subID,
			Type: subscriptionMessageTypeComplete,
		}
		is.NoErr(conn.SendJSONMessage(completeMsg))

		m = <-conn.Stream()
		terminateMsg := subscriptionMessage{}
		is.NoErr(m.UnmarshalAsJSON(&terminateMsg))
		is.Equal(terminateMsg.Type, subscriptionMessageTerminate)
	}))
	defer srv.Close()

	cli, err := NewSubscriptionClient(ctx, srv.URL, http.Header{}, nil)
	is.NoErr(err)
	defer cli.Close()

	sub, err := cli.Subscribe(&Request{
		q:    `subscription ($q: String) { cnt }`,
		vars: map[string]interface{}{"q": "foo"},
	})
	is.NoErr(err)

	res := <-sub.Channel
	is.Equal(string(res.Data), `{"data":"bar"}`)

	is.NoErr(cli.Unsubscribe(sub))
}

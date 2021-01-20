package graphql

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/stretchr/testify/assert"
)

func TestWithClientMpRS(t *testing.T) {
	is := is.New(t)
	var calls int
	testClient := &http.Client{
		Transport: roundTripperFuncMpRS(func(req *http.Request) (*http.Response, error) {
			calls++
			resp := &http.Response{
				Body: ioutil.NopCloser(strings.NewReader(`{"data":{"key":"value"}}`)),
			}
			return resp, nil
		}),
	}

	ctx := context.Background()
	client := NewClient("", WithHTTPClient(testClient), UseMultipartRequestSpec())

	req := NewRequest(``)
	client.Run(ctx, req, nil)

	is.Equal(calls, 1) // calls
}

func TestDoUseMultipartFormMpRS(t *testing.T) {
	is := is.New(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		is.Equal(r.Method, http.MethodPost)
		operations := r.FormValue("operations")
		is.Equal(operations, `{"query":"query {}","variables":{"file":null}}`)
		io.WriteString(w, `{
			"data": {
				"something": "yes"
			}
		}`)
	}))
	defer srv.Close()

	ctx := context.Background()
	client := NewClient(srv.URL, UseMultipartRequestSpec())

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	var responseData map[string]interface{}
	f := strings.NewReader(`This is a file`)
	req := NewRequest("query {}")
	req.File("file", "filename.txt", f)
	err := client.Run(ctx, req, &responseData)
	is.NoErr(err)
	is.Equal(calls, 1) // calls
	is.Equal(responseData["something"], "yes")
}

func TestImmediatelyCloseReqBodyMpRS(t *testing.T) {
	is := is.New(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		is.Equal(r.Method, http.MethodPost)
		operations := r.FormValue("operations")
		is.Equal(operations, `{"query":"query {}","variables":{"file":null}}`)
		io.WriteString(w, `{
			"data": {
				"something": "yes"
			}
		}`)
	}))
	defer srv.Close()

	ctx := context.Background()
	client := NewClient(srv.URL, ImmediatelyCloseReqBody(), UseMultipartRequestSpec())

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	var responseData map[string]interface{}
	f := strings.NewReader(`This is a file`)
	req := NewRequest("query {}")
	req.File("file", "filename.txt", f)
	err := client.Run(ctx, req, &responseData)
	is.NoErr(err)
	is.Equal(calls, 1) // calls
	is.Equal(responseData["something"], "yes")
}

func TestDoErrMpRS(t *testing.T) {
	is := is.New(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		is.Equal(r.Method, http.MethodPost)
		operations := r.FormValue("operations")
		is.Equal(operations, `{"query":"query {}","variables":{"file":null}}`)
		io.WriteString(w, `{
			"errors": [{
				"message": "Something went wrong"
			}]
		}`)
	}))
	defer srv.Close()

	ctx := context.Background()
	client := NewClient(srv.URL, UseMultipartRequestSpec())

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	var responseData map[string]interface{}
	f := strings.NewReader(`This is a file`)
	req := NewRequest("query {}")
	req.File("file", "filename.txt", f)
	err := client.Run(ctx, req, &responseData)
	is.True(err != nil)
	is.Equal(err.Error(), "graphql: Something went wrong")
}

func TestDoServerErrMpRS(t *testing.T) {
	is := is.New(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		is.Equal(r.Method, http.MethodPost)
		operations := r.FormValue("operations")
		is.Equal(operations, `{"query":"query {}","variables":{"file":null}}`)
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `Internal Server Error`)
	}))
	defer srv.Close()

	ctx := context.Background()
	client := NewClient(srv.URL, UseMultipartRequestSpec())

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	var responseData map[string]interface{}
	f := strings.NewReader(`This is a file`)
	req := NewRequest("query {}")
	req.File("file", "filename.txt", f)
	err := client.Run(ctx, req, &responseData)
	is.Equal(err.Error(), "graphql: server returned a non-200 status code: 500")
}

func TestDoBadRequestErrMpRS(t *testing.T) {
	is := is.New(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		is.Equal(r.Method, http.MethodPost)
		operations := r.FormValue("operations")
		is.Equal(operations, `{"query":"query {}","variables":{"file":null}}`)
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{
			"errors": [{
				"message": "miscellaneous message as to why the the request was bad"
			}]
		}`)
	}))
	defer srv.Close()

	ctx := context.Background()
	client := NewClient(srv.URL, UseMultipartRequestSpec())

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	var responseData map[string]interface{}
	f := strings.NewReader(`This is a file`)
	req := NewRequest("query {}")
	req.File("file", "filename.txt", f)
	err := client.Run(ctx, req, &responseData)
	is.Equal(err.Error(), "graphql: server returned a non-200 status code: 400")
}

func TestDoNoResponseMpRS(t *testing.T) {
	is := is.New(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		is.Equal(r.Method, http.MethodPost)
		operations := r.FormValue("operations")
		is.Equal(operations, `{"query":"query {}","variables":{"file":null}}`)
		io.WriteString(w, `{
			"data": {
				"something": "yes"
			}
		}`)
	}))
	defer srv.Close()

	ctx := context.Background()
	client := NewClient(srv.URL, UseMultipartRequestSpec())

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	f := strings.NewReader(`This is a file`)
	req := NewRequest("query {}")
	req.File("file", "filename.txt", f)
	err := client.Run(ctx, req, nil)
	is.NoErr(err)
	is.Equal(calls, 1) // calls
}

func TestQueryMpRS(t *testing.T) {
	is := is.New(t)

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		query := r.FormValue("query")
		is.Equal(query, "query {}")
		is.Equal(r.FormValue("variables"), `{"username":"matryer"}`+"\n")
		_, err := io.WriteString(w, `{"data":{"value":"some data"}}`)
		is.NoErr(err)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	client := NewClient(srv.URL, UseMultipartRequestSpec())

	f := strings.NewReader(`This is a file`)
	req := NewRequest("query {}")
	req.File("file", "filename.txt", f)
	req.Var("username", "matryer")

	// check variables
	is.True(req != nil)
	is.Equal(req.vars["username"], "matryer")

	var resp struct {
		Value string
	}
	err := client.Run(ctx, req, &resp)
	assert.Error(t, err, "variables doesn't supported due to the multipart request spec")
}

func TestFileMpRS(t *testing.T) {
	is := is.New(t)

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		file, header, err := r.FormFile("file")
		is.NoErr(err)
		defer file.Close()
		is.Equal(header.Filename, "filename.txt")

		b, err := ioutil.ReadAll(file)
		is.NoErr(err)
		is.Equal(string(b), `This is a file`)

		_, err = io.WriteString(w, `{"data":{"value":"some data"}}`)
		is.NoErr(err)

		operations := r.FormValue("operations")
		is.Equal(operations, `{"query":"query {}","variables":{"file":null}}`)

		maps := r.FormValue("map")
		is.Equal(maps, `{"file":["variables.file"]}`)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	client := NewClient(srv.URL, UseMultipartRequestSpec())
	f := strings.NewReader(`This is a file`)
	req := NewRequest("query {}")
	req.File("file", "filename.txt", f)
	err := client.Run(ctx, req, nil)
	is.NoErr(err)
}

type roundTripperFuncMpRS func(req *http.Request) (*http.Response, error)

func (fn roundTripperFuncMpRS) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

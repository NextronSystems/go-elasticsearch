package elasticsearch

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// Client is the api client for Elasticsearch.
type Client struct {
	baseURL *url.URL
}

// Open creates a new Client instance based on a baseURL.
// This function does not test the connection. Use Ping() for connection tests.
func Open(baseURL string) (*Client, error) {
	var (
		client = &Client{}
		err    error
	)
	if client.baseURL, err = url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("could not parse url: %s", err)
	}
	return client, nil
}

// Ping is the connection test for the Elasticsearch client.
func (c *Client) Ping() error {
	_, err := c.get("", nil)
	if err != nil {
		return fmt.Errorf("could not ping server: %s", err)
	}
	return nil
}

func (c *Client) do(r *http.Request) ([]byte, error) {
	if log.DebugMode() {
		b, err := httputil.DumpRequest(r, true)
		if err != nil {
			b = []byte(err.Error())
		}
		log.Debugf("Elasticsearch Request: %s", string(b))
	}
	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		return nil, fmt.Errorf("could not do request: %s", err)
	}
	defer resp.Body.Close()
	if log.DebugMode() {
		b, err := httputil.DumpResponse(resp, true)
		if err != nil {
			b = []byte(err.Error())
		}
		log.Debugf("Elasticsearch Response: %s", string(b))
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		if body, _ := ioutil.ReadAll(resp.Body); body != nil {
			return nil, fmt.Errorf("http status %d (%s)", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read response body: %s", err)
	}
	return body, nil
}

func (c *Client) post(apipath string, json []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s", c.baseURL.String(), apipath), bytes.NewReader(json))
	if err != nil {
		return nil, fmt.Errorf("could not prepare post request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req)
}

func (c *Client) get(apipath string, json []byte) ([]byte, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", c.baseURL.String(), apipath), bytes.NewReader(json))
	if err != nil {
		return nil, fmt.Errorf("could not prepare get request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req)
}

func (c *Client) put(apipath string, json []byte) ([]byte, error) {
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/%s", c.baseURL.String(), apipath), bytes.NewReader(json))
	if err != nil {
		return nil, fmt.Errorf("could not prepare put request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req)
}

func (c *Client) delete(apipath string, json []byte) ([]byte, error) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s", c.baseURL.String(), apipath), bytes.NewReader(json))
	if err != nil {
		return nil, fmt.Errorf("could not prepare delete request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req)
}

func refreshValue(refresh bool) string {
	if refresh {
		return "wait_for"
	}
	return "false"
}

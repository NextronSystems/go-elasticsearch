package elasticsearch

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

const sleepOnTooManyRequests = time.Second * 10

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

func (c *Client) do(r *http.Request) ([]byte, bool, error) {
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
		return nil, false, fmt.Errorf("could not do request: %s", err)
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
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, true, nil
		}
		if body, _ := ioutil.ReadAll(resp.Body); body != nil {
			return nil, false, fmt.Errorf("http status %d (%s)", resp.StatusCode, string(body))
		}
		return nil, false, fmt.Errorf("http status %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("could not read response body: %s", err)
	}
	return body, false, nil
}

func (c *Client) post(apipath string, json []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s", c.baseURL.String(), apipath), bytes.NewReader(json))
	if err != nil {
		return nil, fmt.Errorf("could not prepare post request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	b, retry, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if retry == true {
		time.Sleep(sleepOnTooManyRequests)
		return c.get(apipath, json)
	}
	return b, nil
}

func (c *Client) get(apipath string, json []byte) ([]byte, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", c.baseURL.String(), apipath), bytes.NewReader(json))
	if err != nil {
		return nil, fmt.Errorf("could not prepare get request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	b, retry, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if retry == true {
		time.Sleep(sleepOnTooManyRequests)
		return c.get(apipath, json)
	}
	return b, nil
}

func (c *Client) put(apipath string, json []byte) ([]byte, error) {
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/%s", c.baseURL.String(), apipath), bytes.NewReader(json))
	if err != nil {
		return nil, fmt.Errorf("could not prepare put request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	b, retry, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if retry == true {
		time.Sleep(sleepOnTooManyRequests)
		return c.get(apipath, json)
	}
	return b, nil
}

func (c *Client) delete_(apipath string, json []byte) ([]byte, error) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s", c.baseURL.String(), apipath), bytes.NewReader(json))
	if err != nil {
		return nil, fmt.Errorf("could not prepare delete request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	b, retry, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if retry == true {
		time.Sleep(sleepOnTooManyRequests)
		return c.get(apipath, json)
	}
	return b, nil
}

// Refresh parameter for most requests, default should be RefreshFalse,
// but if changes have to be done immediately, then you should use RefreshTrue
// or RefreshWaitFor, see: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
type Refresh string

const (
	// RefreshTrue refreshes the relevant primary and replica shards immediately
	RefreshTrue Refresh = "true"
	// RefreshWaitFor does not force a refresh, instead it waits for the next refresh specified by 'index.refresh_interval'
	RefreshWaitFor Refresh = "wait_for"
	// RefreshFalse is the default behaviour, does not refresh anything and is the fastest solution
	RefreshFalse Refresh = "false"
)

func getRefreshString(r Refresh) string {
	if r == RefreshTrue || r == RefreshWaitFor {
		return string(r)
	}
	return string(RefreshFalse)
}

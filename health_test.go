package elasticsearch

import "testing"


var healthClient *Client

func init() {
	var err error
	if healthClient, err = Open("http://localhost:9200"); err != nil {
		panic(err)
	}
	if err := healthClient.Ping(); err != nil {
		panic(err)
	}
}

func TestClient_Health(t *testing.T) {
	health, err := healthClient.Health()
	if err != nil {
		t.Fatalf("health error: %s", err)
	}
	if health == "green" || health == "yellow" {
		t.Logf("Elasticsearch Status: %s", health)
	} else {
		t.Fatalf("Elasticsearch Status: %s", health)
	}
}

package elasticsearch

import (
	"testing"
)

var bulkClient *Client

func init() {
	var err error
	bulkClient, err = Open("http://localhost:9200")
	if err != nil {
		panic(err)
	}
	if err := bulkClient.Ping(); err != nil {
		panic(err)
	}
	bulkClient.DeleteIndex("testclient_insertdocuments")
	bulkClient.DeleteIndex("testclient_insertdocuments2")
}

func TestClient_InsertDocuments(t *testing.T) {
	docs := map[string]map[string]interface{}{
		"1": {
			"field1": "value1",
		},
		"2": {
			"field1": "value2",
		},
		"3": {
			"field1": "value3",
		},
	}
	if _, err := bulkClient.InsertDocuments("testclient_insertdocuments", "doc", docs); err != nil {
		t.Fatalf("could not insert documents: %s", err)
	}
	if err := bulkClient.Refresh("testclient_insertdocuments"); err != nil {
		t.Fatalf("could not refresh index: %s", err)
	}
	count, err := bulkClient.CardinalityAggregate("testclient_insertdocuments", "doc", nil, "field1")
	if err != nil {
		t.Fatalf("could not cardinality aggregate: %s", err)
	}
	if count != 3 {
		t.Fatalf("expected count 3, got: %d", count)
	}
}

func TestClient_InsertDocuments2(t *testing.T) {
	docs := map[string]map[string]interface{}{
		"1": {
			"field1": "value1",
		},
		"2": nil, // the error
		"3": {
			"field1": "value3",
		},
	}
	bulkErrors, err := bulkClient.InsertDocuments("testclient_insertdocuments2", "doc", docs)
	if err != nil {
		t.Fatalf("could not insert documents: %s", err)
	}
	if bulkErrors == nil {
		t.Fatal("expected bulk errors")
	}
	if len(bulkErrors) == 1 && bulkErrors["2"] != nil {
		t.Logf("error as expected: %s", bulkErrors["2"])
	} else {
		t.Fatalf("expected bulk error for id: ' 2', got: %#v", bulkErrors)
	}
	if err := bulkClient.Refresh("testclient_insertdocuments2"); err != nil {
		t.Fatalf("could not refresh index: %s", err)
	}
	count, err := bulkClient.CardinalityAggregate("testclient_insertdocuments2", "doc", nil, "field1")
	if err != nil {
		t.Fatalf("could not cardinality aggregate: %s", err)
	}
	if count != 2 {
		t.Fatalf("expected count 2, got: %d", count)
	}
}

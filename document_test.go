package elasticsearch

import (
	"testing"
	"strings"
)

var documentClient *Client

func init() {
	var err error
	documentClient, err = Open("http://localhost:9200")
	if err != nil {
		panic(err)
	}
	if err := documentClient.Ping(); err != nil {
		panic(err)
	}
	documentClient.DeleteIndex("testclient_insertgetdeletedocument")
	documentClient.DeleteIndex("testclient_updatedocument")
}

func TestClient_InsertGetDeleteDocument(t *testing.T) {
	document := map[string]interface{}{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}
	if err := documentClient.InsertDocument("testclient_insertgetdeletedocument", "doc", "1", document, true); err != nil {
		t.Fatalf("could not insert document: %s", err)
	}
	result, err := documentClient.GetDocument("testclient_insertgetdeletedocument", "doc", "1")
	if err != nil {
		t.Fatalf("could not get document: %s", err)
	}
	if document, ok := result["_source"].(map[string]interface{}); ok {
		if document["field1"] != "value1" {
			t.Fatal("field1 not value1")
		}
	} else {
		t.Fatal("no _source")
	}
	if err := documentClient.DeleteDocument("testclient_insertgetdeletedocument", "doc", "1", true); err != nil {
		t.Fatalf("could not delete document: %s", err)
	}
	_, err = documentClient.GetDocument("testclient_insertgetdeletedocument", "doc", "1")
	if err != nil && strings.Contains(err.Error(), "http status 404") {
		t.Logf("error as expected: %s", err)
	} else {
		t.Fatalf("unknown error after deletion: %s", err)
	}
}

func TestClient_UpdateDocument(t *testing.T) {
	document := map[string]interface{}{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}
	if err := documentClient.InsertDocument("testclient_updatedocument", "doc", "1", document, true); err != nil {
		t.Fatalf("could not insert document: %s", err)
	}
	if err := documentClient.UpdateDocument("testclient_updatedocument", "doc", "1", "ctx._source.field1 = params.value", map[string]interface{}{"value": "valueX"}, true); err != nil {
		t.Fatalf("could not update document: %s", err)
	}
	result, err := documentClient.GetDocument("testclient_updatedocument", "doc", "1")
	if err != nil {
		t.Fatalf("could not get document: %s", err)
	}
	if document, ok := result["_source"].(map[string]interface{}); ok {
		if document["field1"] != "valueX" {
			t.Fatalf("field1 not valueX (%s)", document["field1"])
		}
	} else {
		t.Fatal("no _source")
	}
}
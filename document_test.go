package elasticsearch

import (
	"fmt"
	"strings"
	"sync"
	"testing"
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
	documentClient.DeleteIndex("testclient_scrolldocuments")
	documentClient.DeleteIndex("testclient_scrolldocuments2")
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

func TestClient_ScrollDocuments(t *testing.T) {
	for i := 0; i < 3456; i++ {
		if err := documentClient.InsertDocument("testclient_scrolldocuments", "doc", fmt.Sprint(i), map[string]interface{}{
			"field": "value",
		}, false); err != nil {
			t.Fatalf("could not insert document: %s", err)
		}
	}
	documentClient.Refresh("testclient_scrolldocuments")
	docs := make(chan map[string]interface{}, 1)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()
		if err := documentClient.ScrollDocuments("testclient_scrolldocuments", "doc", nil, docs); err != nil {
			t.Fatalf("could not scroll documents: %s", err)
		}
	}()
	var counter int
	for range docs {
		counter++
	}
	wg.Wait()
	if counter != 3456 {
		t.Fatalf("wrong count, expected 3456, got: %d", counter)
	}
}

func TestClient_ScrollDocuments2(t *testing.T) {
	if err := documentClient.InsertDocument("testclient_scrolldocuments2", "doc", "1", map[string]interface{}{
		"field": "value",
	}, false); err != nil {
		t.Fatalf("could not insert document: %s", err)
	}
	documentClient.Refresh("testclient_scrolldocuments2")
	docs := make(chan map[string]interface{}, 1)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()
		if err := documentClient.ScrollDocuments("testclient_scrolldocuments2", "doc", nil, docs); err != nil {
			t.Fatalf("could not scroll documents: %s", err)
		}
	}()
	var counter int
	for range docs {
		counter++
	}
	wg.Wait()
	if counter != 1 {
		t.Fatalf("wrong count, expected 0, got: %d", counter)
	}
}

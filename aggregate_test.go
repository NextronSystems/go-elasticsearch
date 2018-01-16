package elasticsearch

import (
	"testing"
	"encoding/json"
)

var aggregateClient *Client

func init() {
	var err error
	aggregateClient, err = Open("http://localhost:9200")
	if err != nil {
		panic(err)
	}
	if err := aggregateClient.Ping(); err != nil {
		panic(err)
	}
	aggregateClient.DeleteIndex("testclient_termaggregate")
	template, _ := json.Marshal(map[string]interface{}{
		"index_patterns": []string{"*"},
		"settings": map[string]interface{}{
			"number_of_shards": 1,
			"number_of_replicas": 0,
		},
		"mappings": map[string]interface{}{
			"doc": map[string]interface{}{
				"dynamic_templates": []interface{}{
					map[string]interface{}{
						"string_fields": map[string]interface{}{
							"mapping": map[string]interface{}{
								"type": "keyword",
								"index": true,
							},
							"match_mapping_type": "string",
							"match": "*",
						},
					},
				},
			},
		},
	})
	aggregateClient.put("_template/doc", template)
}

func TestClient_TermAggregate(t *testing.T) {
	aggregateClient.InsertDocument("testclient_termaggregate", "doc", "1", map[string]interface{}{
		"field1": "value1",
		"field2": "value2",
	}, true)
	aggregateClient.InsertDocument("testclient_termaggregate", "doc", "2", map[string]interface{}{
		"field1": "value1",
		"field2": "value3",
	}, true)
	aggregateClient.InsertDocument("testclient_termaggregate", "doc", "3", map[string]interface{}{
		"field1": "value1",
		"field2": "value4",
	}, true)
	result, err := aggregateClient.TermAggregate("testclient_termaggregate", "doc", nil, NewTermAggregations([]*TermAggregation{
		{Field: "field1", Size: 10},
		{Field: "field2", Size: 10},
	}))
	if err != nil {
		t.Fatalf("could not get aggregations: %s", err)
	}
	field1 := result["field1"].Buckets
	if len(field1) != 1 || field1[0].Key.(string) != "value1" || field1[0].Count != 3 {
		t.Fatalf("wrong field1 aggs: %#v", field1)
	}
	field2 := result["field2"].Buckets
	if len(field2) != 3 || field2[0].Count != 1 || field2[1].Count != 1 || field2[2].Count != 1 {
		t.Fatalf("wrong field2 aggs: %#v", field2)
	}
}

func TestClient_RangeAggregate(t *testing.T) {
	aggregateClient.InsertDocument("testclient_rangeaggregate", "doc", "1", map[string]interface{}{"field1": 10}, true)
	aggregateClient.InsertDocument("testclient_rangeaggregate", "doc", "2", map[string]interface{}{"field1": 100}, true)
	aggregateClient.InsertDocument("testclient_rangeaggregate", "doc", "3", map[string]interface{}{"field1": 1000}, true)
	aggregateClient.InsertDocument("testclient_rangeaggregate", "doc", "4", map[string]interface{}{"field1": 1}, true)
	minValue, maxValue, err := aggregateClient.RangeAggregate("testclient_rangeaggregate", "doc", nil, "field1")
	if err != nil {
		t.Fatalf("could not range aggregate: %s", err)
	}
	if minValue != 1.0 || maxValue != 1000.0 {
		t.Fatalf("wrong range, expected %f - %f, got: %f - %f", 1.0, 1000.0, minValue, maxValue)
	}
}
package elasticsearch

import (
	"encoding/json"
	"fmt"
	"path"
	"bytes"
)

type TermAggregations map[string]*TermAggregation

func NewTermAggregations(aggs []*TermAggregation) TermAggregations {
	result := TermAggregations{}
	for _, agg := range aggs {
		result[agg.Field] = agg
	}
	return result
}

type TermAggregation struct {
	Field string
	Size int
}

func (t *TermAggregation) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"terms": map[string]interface{}{
			"field": t.Field,
			"size": t.Size,
		},
	})
}

type TermAggregationResults map[string]TermAggregationResult

type TermAggregationResult struct {
	Buckets []Bucket `json:"buckets"`
}

type Bucket struct {
	Key interface{} `json:"key"`
	Count int `json:"doc_count"`
}

func (c *Client) TermAggregate(index, doctype string, query map[string]interface{}, aggregations TermAggregations) (TermAggregationResults, error) {
	request := map[string]interface{}{
		"size": 0,
		"aggs": aggregations,
	}
	if query != nil {
		request["query"] = query
	}
	b, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("could not marshal request: %s", err)
	}
	apipath := path.Join(index, doctype) + "/_search"
	res, err := c.get(apipath, b)
	if err != nil {
		return nil, fmt.Errorf("could not get aggregations: %s", err)
	}
	result := struct{
		Aggregations TermAggregationResults `json:"aggregations"`
	}{}
	decoder := json.NewDecoder(bytes.NewReader(res))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("could not decode result: %s", err)
	}
	return result.Aggregations, nil
}

package elasticsearch

import (
	"encoding/json"
	"fmt"
	"path"
	"bytes"
	"errors"
)

/*
 * TermAggregations is a list of TermAggregation, allowing
 * multiple aggregations with one request.
 */
type TermAggregations map[string]*TermAggregation

/*
 * NewTermAggregations takes a list of TermAggregation and
 * returns a TermAggregations
 */
func NewTermAggregations(aggs []*TermAggregation) TermAggregations {
	result := TermAggregations{}
	for _, agg := range aggs {
		result[agg.Field] = agg
	}
	return result
}

/*
 * TermAggregation term aggregates for the specified field. The higher
 * the size is, the more accurate are the results.
 */
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

/*
 * TermAggregationResults is a list of TermAggregationResult
 */
type TermAggregationResults map[string]TermAggregationResult

/*
 * TermAggregationResult contains the result of the TermAggregation.
 */
type TermAggregationResult struct {
	Buckets []Bucket `json:"buckets"`
}

/*
 * Bucket contains how often a specific key was found in a term aggregation.
 */
type Bucket struct {
	Key interface{} `json:"key"`
	Count int `json:"doc_count"`
}

/*
 * TermAggregate term aggregates in a specific index. A query is optional.
 */
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

/*
 * RangeAggregate returns the min- and max-value for a specific field in a specific index.
 * A query is optional.
 */
func (c *Client) RangeAggregate(index, doctype string, query map[string]interface{}, field string) (float64, float64, error) {
	request := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			"min_"+field: map[string]interface{}{
				"min": map[string]interface{}{
					"field": field,
				},
			},
			"max_"+field: map[string]interface{}{
				"max": map[string]interface{}{
					"field": field,
				},
			},
		},
	}
	if query != nil {
		request["query"] = query
	}
	b, err := json.Marshal(request)
	if err != nil {
		return 0, 0, fmt.Errorf("could not marshal request: %s", err)
	}
	apipath := path.Join(index, doctype) + "/_search"
	res, err := c.get(apipath, b)
	if err != nil {
		return 0, 0, fmt.Errorf("could not get aggregations: %s", err)
	}
	result := struct{
		Aggregations map[string]struct{
			Value float64 `json:"value"`
		} `json:"aggregations"`
	}{}
	decoder := json.NewDecoder(bytes.NewReader(res))
	if err := decoder.Decode(&result); err != nil {
		return 0, 0, fmt.Errorf("could not decode result: %s", err)
	}
	if result.Aggregations == nil {
		return 0, 0, fmt.Errorf("no aggregation result found: %s", err)
	}
	minValue, ok1 := result.Aggregations["min_"+field]
	maxValue, ok2 := result.Aggregations["max_"+field]
	if !ok1 || !ok2 {
		return 0, 0, errors.New("min or max value not a number")
	}
	return minValue.Value, maxValue.Value, nil
}

/*
 * CardinalityAggregate returns the unique count of a specific field in a specific index.
 * A query is optional.
 */
func (c *Client) CardinalityAggregate(index, doctype string, query map[string]interface{}, field string) (int64, error) {
	request := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			"count_"+field: map[string]interface{}{
				"cardinality": map[string]interface{}{
					"field": field,
				},
			},
		},
	}
	if query != nil {
		request["query"] = query
	}
	b, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("could not marshal request: %s", err)
	}
	apipath := path.Join(index, doctype) + "/_search"
	res, err := c.get(apipath, b)
	if err != nil {
		return 0, fmt.Errorf("could not get aggregations: %s", err)
	}
	result := struct{
		Aggregations map[string]struct{
			Value int64 `json:"value"`
		} `json:"aggregations"`
	}{}
	decoder := json.NewDecoder(bytes.NewReader(res))
	if err := decoder.Decode(&result); err != nil {
		return 0, fmt.Errorf("could not decode result: %s", err)
	}
	value, ok := result.Aggregations["count_"+field]
	if !ok {
		return 0, errors.New("could not find count of field")
	}
	return value.Value, nil
}

package elasticsearch

import (
	"encoding/json"
	"fmt"
	"path"
	"bytes"
)

type Order struct {
	Field string
	Order string // asc or desc
}

func (o *Order) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		o.Field: o.Order,
	})
}

func (c *Client) InsertDocument(index, doctype, id string, document map[string]interface{}, refresh bool) error {
	b, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("could not marshal the document: %s", err)
	}
	apipath := path.Join(index, doctype, id) + "?refresh=" + refreshValue(refresh)
	if _, err := c.put(apipath, b); err != nil {
		return fmt.Errorf("could not insert document: %s", err)
	}
	return nil
}

func (c *Client) GetDocument(index, doctype, id string) (map[string]interface{}, error) {
	apipath := path.Join(index, doctype, id)
	b, err := c.get(apipath, nil)
	if err != nil {
		return nil, fmt.Errorf("could not get document: %s", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	result := map[string]interface{}{}
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("could not decode document: %s", err)
	}
	return result, nil
}

func (c *Client) GetDocuments(index, doctype string, query map[string]interface{}, from int64, size int64, order *Order) ([]map[string]interface{}, int64, error) {
	request := map[string]interface{}{}
	if query != nil {
		request["query"] = query
	}
	if order != nil {
		request["sort"] = []*Order{order}
	}
	b, err := json.Marshal(request)
	if err != nil {
		return nil, 0, fmt.Errorf("could not marshal query: %s", err)
	}
	apipath := path.Join(index, doctype) + fmt.Sprintf("/_search?from=%d&size=%d", from, size)
	b, err = c.get(apipath, b)
	if err != nil {
		return nil, 0, fmt.Errorf("could not get documents: %s", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	result := struct {
		Hits struct{
			Total int64 `json:"total"`
			Hits []map[string]interface{} `json:"hits"`
		     } `json:"hits"`
	}{}
	if err := decoder.Decode(&result); err != nil {
		return nil, 0, fmt.Errorf("could not decode documents: %s", err)
	}
	return result.Hits.Hits, result.Hits.Total, nil
}

func (c * Client) UpdateDocument(index, doctype, id string, painlessScript string, params map[string]interface{}, refresh bool) error {
	script := map[string]interface{}{
		"source": painlessScript,
		"lang": "painless",
	}
	if params != nil {
		script["params"] = params
	}
	b, err := json.Marshal(map[string]interface{}{
		"script": script,
	})
	if err != nil {
		return fmt.Errorf("could not marshal the changes: %s", err)
	}
	apipath := path.Join(index, doctype, id) + "/_update?refresh=" + refreshValue(refresh)
	if _, err := c.post(apipath, b); err != nil {
		return fmt.Errorf("could not update document: %s", err)
	}
	return nil
}

func (c *Client) UpdateDocuments(index, doctype string, query map[string]interface{}, painlessScript string, params map[string]interface{}) error {
	script := map[string]interface{}{
		"source": painlessScript,
		"lang": "painless",
	}
	if params != nil {
		script["params"] = params
	}
	b, err := json.Marshal(map[string]interface{}{
		"query": query,
		"script": script,
	})
	if err != nil {
		return fmt.Errorf("could not marshal the query: %s", err)
	}
	apipath := path.Join(index, doctype) + "/_update_by_query?conflicts=proceed"
	if _, err := c.post(apipath, b); err != nil {
		return fmt.Errorf("could not update documents: %s", err)
	}
	return nil
}

func (c *Client) DeleteDocument(index, doctype, id string, refresh bool) error {
	apipath := path.Join(index, doctype, id) + "?refresh=" + refreshValue(refresh)
	if _, err := c.delete(apipath, nil); err != nil {
		return fmt.Errorf("could not update document: %s", err)
	}
	return nil
}

func (c *Client) DeleteDocuments(index, doctype string, query map[string]interface{}) error {
	b, err := json.Marshal(map[string]interface{}{
		"query": query,
	})
	if err != nil {
		return fmt.Errorf("could not marshal the query: %s", err)
	}
	apipath := path.Join(index, doctype) + "/_delete_by_query"
	if _, err := c.post(apipath, b); err != nil {
		return fmt.Errorf("could not delete by query: %s", err)
	}
	return nil
}
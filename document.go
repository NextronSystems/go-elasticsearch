package elasticsearch

import (
	"encoding/json"
	"fmt"
	"path"
	"bytes"
)

// Order can be used to define the order of the Elasticsearch result.
type Order struct {
	Field string
	Order string // asc or desc
}

func (o *Order) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		o.Field: o.Order,
	})
}

// InsertDocument inserts a document in a specific index.
// If the id already exists, the old document will be replaced.
// If refresh is set to false, the result will be returned immediately.
// If refresh is set to true, elasticsearch waits until all changes were done.
// If multiple inserts are done and all changes have to be done before continueing,
// set refresh to false and call Refresh() after.
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

// GetDocument returns the document in a specific index and a specific id.
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

// GetDocuments returns multiple documents in a specific index. Order and Query are optional.
// A offset and size have to be defined. The offset+size have to be lower than 10.000, otherwise
// Elasticsearch returns an error. If you want to get more than 10.000, use ScrollDocuments instead.
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

// UpdateDocument runs a update script on a specific index and a specific id.
// It's recommended to use parameterized update scripts and pass the parameters in 'params'.
// Then elasticsearch has to compile the script only once. Elasticsearch will also return
// an error, if to many different scripts are executed in a small time interval.
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

// UpdateDocuments runs an update script on multiple documents in a specific index. A query is optional.
// It's recommended to use parameterized update scripts and pass the parameters in 'params'.
// Then elasticsearch has to compile the script only once. Elasticsearch will also return
// an error, if to many different scripts are executed in a small time interval.
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

// DeleteDocument deletes a specific document in a specific index.
func (c *Client) DeleteDocument(index, doctype, id string, refresh bool) error {
	apipath := path.Join(index, doctype, id) + "?refresh=" + refreshValue(refresh)
	if _, err := c.delete(apipath, nil); err != nil {
		return fmt.Errorf("could not update document: %s", err)
	}
	return nil
}

// DeleteDocuments deletes multiple documents in a specific index. A query is optional.
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

// ScrollDocuments is the more performant solution to get lots of documents in a specific index. A query is optional.
// This function will return always all found documents without an order into the 'docs' channel. Ensure that this function
// is called as a go routine!
func (c *Client) ScrollDocuments(index, doctype string, query map[string]interface{}, docs chan map[string]interface{}) error {
	defer close(docs)
	apipath := path.Join(index, doctype) + "/_search?scroll=5m"
	req := map[string]interface{}{
		"size": 1000,
		"sort": []string{"_doc"},
	}
	if query != nil {
		req["query"] = query
	}
	return c.scrollDocuments(apipath, req, docs, "")
}

func (c *Client) scrollDocuments(apipath string, req map[string]interface{}, docs chan map[string]interface{}, scrollId string) error {
	scrollResult := struct {
		ScrollId string `json:"_scroll_id"`
		Hits struct{
			     Hits []map[string]interface{} `json:"hits"`
		     } `json:"hits"`
	}{}
	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("could not marshal scroll request: %s", err)
	}
	res, err := c.post(apipath, b)
	if err != nil {
		return fmt.Errorf("could not scroll documents: %s", err)
	}
	if err := json.Unmarshal(res, &scrollResult); err != nil {
		return fmt.Errorf("could not unmarshal scroll result: %s", err)
	}
	if scrollId != "" {
		if err := c.deleteScroll(scrollId); err != nil {
			return fmt.Errorf("could not delete scroll: %s", err)
		}
	}
	for _, hit := range scrollResult.Hits.Hits {
		docs <- hit
	}
	if scrollId == scrollResult.ScrollId {
		return nil
	}
	return c.scrollDocuments("_search/scroll", map[string]interface{}{
		"scroll": "5m",
		"scroll_id": scrollResult.ScrollId,
	}, docs, scrollResult.ScrollId)
}

func (c *Client) deleteScroll(scrollId string) error {
	b, err := json.Marshal(map[string]interface{}{
		"scroll_id": scrollId,
	})
	if err != nil {
		return fmt.Errorf("could not marshal the delete scroll query: %s", err)
	}
	if _, err := c.delete("_search/scroll", b); err != nil {
		return fmt.Errorf("could not delete the scroll: %s", err)
	}
	return nil
}

package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
)

// InsertDocuments bulk imports multiple documents into a specific index. Use the document id as
// key for the 'docs' map.
// If an error for a specific document occurs, the error will be returned in a map with the document id as key.
// If an error occurs that regards to all documents, this function will return an error.
func (c *Client) InsertDocuments(index string, doctype string, docs map[string]map[string]interface{}) (map[string]error, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for id, doc := range docs {
		if err := encoder.Encode(map[string]interface{}{
			"index": map[string]interface{}{
				"_id": id,
			},
		}); err != nil {
			return nil, fmt.Errorf("could not encode document id: %s", err)
		}
		if err := encoder.Encode(doc); err != nil {
			return nil, fmt.Errorf("could not encode document: %s", err)
		}
	}
	apipath := path.Join(index, doctype) + "/_bulk"
	res, err := c.put(apipath, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("could not bulk import: %s", err)
	}
	bulkResult := struct{
		Items []struct{
			Index struct{
			      Id string `json:"_id"`
			      Status int `json:"status"`
			      Error map[string]interface{} `json:"error"`
		      	} `json:"index"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(res, &bulkResult); err != nil {
		return nil, fmt.Errorf("could not unmarshal bulk result: %s", err)
	}
	bulkErrors := map[string]error{}
	for _, item := range bulkResult.Items {
		if item.Index.Error != nil {
			b, err := json.Marshal(item.Index.Error)
			if err != nil {
				b = []byte(err.Error())
			}
			bulkErrors[item.Index.Id] = fmt.Errorf("could not bulk import document: %s", string(b))
		}
	}
	if len(bulkErrors) > 0 {
		return bulkErrors, nil
	}
	return nil, nil
}

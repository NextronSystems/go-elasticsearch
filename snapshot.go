package elasticsearch

import (
	"encoding/json"
	"fmt"
)

// AddRepository adds a new repository for snapshots. Ensure that a location
// is configured in /etc/elasticsearch/elasticsearch.yml before calling
// this function.
func (c *Client) AddRepository(name string, location string) error {
	b, err := json.Marshal(map[string]interface{}{
		"type": "fs",
		"settings": map[string]interface{}{
			"location": location,
		},
	})
	if err != nil {
		return err
	}
	_, err = c.put(fmt.Sprintf("_snapshot/%s", name), b)
	return err
}

// AddSnapshot adds a new snapshot in a specified repository. Ensure that
// the repository exists before calling this function.
func (c *Client) AddSnapshot(repositoryName string, snapshotName string) error {
	_, err := c.put(fmt.Sprintf("/_snapshot/%s/%s?wait_for_completion=true", repositoryName, snapshotName), nil)
	return err
}

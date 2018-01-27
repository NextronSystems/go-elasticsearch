package elasticsearch

import (
	"encoding/json"
	"fmt"
	"path"
)

/*
 * AddTemplate adds a new template to Elasticsearch.
 */
func (c *Client) AddTemplate(id string, template map[string]interface{}) error {
	b, err := json.Marshal(template)
	if err != nil {
		return fmt.Errorf("could not marshal template: %s", err)
	}
	apipath := path.Join("_template", id)
	if _, err := c.put(apipath, b); err != nil {
		return fmt.Errorf("could not add template: %s", err)
	}
	return nil
}

/*
 * DeleteTemplate deletes a template.
 */
func (c *Client) DeleteTemplate(id string) error {
	apipath := path.Join("_template", id)
	if _, err := c.delete(apipath, nil); err != nil {
		return fmt.Errorf("could not delete template: %s", err)
	}
	return nil
}

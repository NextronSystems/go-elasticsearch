package elasticsearch

import (
	"encoding/json"
	"fmt"
	"strings"
)

func (c *Client) AddTemplate(id string, template map[string]interface{}) error {
	b, err := json.Marshal(template)
	if err != nil {
		return fmt.Errorf("could not marshal template: %s", err)
	}
	apipath := strings.Join("_template", id)
	if _, err := c.put(apipath, b); err != nil {
		return fmt.Errorf("could not add template: %s", err)
	}
	return nil
}

func (c *Client) DeleteTemplate(id string) error {
	apipath := strings.Join("_template", id)
	if _, err := c.delete(apipath, nil); err != nil {
		return fmt.Errorf("could not delete template: %s", err)
	}
	return nil
}

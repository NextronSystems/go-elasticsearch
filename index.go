package elasticsearch

import "fmt"

// DeleteIndex deletes a whole index.
func (c *Client) DeleteIndex(index string) error {
	_, err := c.delete(index, nil)
	if err != nil {
		return fmt.Errorf("could not delete index: %s", err)
	}
	return nil
}

// Refresh refreshs a index. Useful if multiple updates or inserts were done without refresh = true.
func (c *Client) Refresh(index string) error {
	_, err := c.post(index+"/_refresh", nil)
	if err != nil {
		return fmt.Errorf("could not refresh index: %s", err)
	}
	return nil
}
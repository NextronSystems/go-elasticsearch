package elasticsearch

import "fmt"

func (c *Client) DeleteIndex(index string) error {
	_, err := c.delete(index, nil)
	if err != nil {
		return fmt.Errorf("could not delete index: %s", err)
	}
	return nil
}

func (c *Client) Refresh(index string) error {
	_, err := c.post(index+"/_refresh", nil)
	if err != nil {
		return fmt.Errorf("could not refresh index: %s", err)
	}
	return nil
}
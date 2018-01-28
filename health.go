package elasticsearch

import "encoding/json"

// Status constants for Elasticsearch health
const (
	StatusGreen  = "green"
	StatusYellow = "yellow"
	StatusRed    = "red"
)

// Health returns the health status of Elasticsearch (green, yellow, red).
func (c *Client) Health() (string, error) {
	res, err := c.get("_cluster/health", nil)
	if err != nil {
		return StatusRed, err
	}
	health := map[string]interface{}{}
	if err := json.Unmarshal(res, &health); err != nil {
		return StatusRed, err
	}
	if status, ok := health["status"].(string); ok && (status == StatusGreen || status == StatusYellow) {
		return status, nil
	}
	return StatusRed, nil
}

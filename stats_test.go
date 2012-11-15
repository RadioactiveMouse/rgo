package main

import "testing"

func TestStats(t *testing.T) {
	c:= new(Client)
	c.address = "http://localhost:8098"
	err := c.Stats()
	if err != nil {
		t.Errorf("Error during Stats() endpoint test")
	}
}

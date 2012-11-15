package main

import (
	"testing"
)

func TestPing(t *testing.T) {
	c := new(Client)
	c.address = "http://localhost:8098"
	err := c.Ping()
	if err != nil {
		t.Errorf("Error During Ping: %s", err)
	}
}

package main

import (
	"testing"
)

func TestGet(t *testing.T) {
	c := new(Client)
	c.address = "http://localhost:8089"
	det, err := c.getBucketDetails("test")
	if err != nil {
		t.Errorf("Getting bucket details failed.)
	}
}

func TestSet(t *testing.T) {
	c := new(Client)
	c.address = "http://localhost:8089"
	err := c.SetBucketDetails("test")
	if err != nil {
		t.Errorf("Setting bucket details failed.")
	}
}

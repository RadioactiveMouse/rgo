package rgo

import (
	"testing"
)

var testAddress string = "127.0.0.1:8098"

func TestNodePing(t *testing.T) {
	c := NewConnection(testAddress)
	err := c.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNodeStatus(t *testing.T) {
	c := NewConnection(testAddress)
	_, err := c.Status()
	if err != nil {
		t.Error(err)
	}
}

func TestNodeListResources(t *testing.T) {
	c := NewConnection(testAddress)
	_, err := c.ListResources()
	if err != nil {
		t.Error(err)
	}
}

func TestNodeListBuckets(t *testing.T) {
	c := NewConnection(testAddress)
	_, err := c.ListBuckets()
	if err != nil {
		t.Error(err)
	}
}

func TestNodeListKeys(t *testing.T) {
	c := NewConnection(testAddress)
	_, err := c.ListKeys("test")
	if err != nil {
		t.Error(err)
	}
}

func TestNodeResetBucketProperties(t *testing.T) {
	c := NewConnection(testAddress)
	err := c.ResetBucketProperties("test")
	if err != nil {
		t.Error(err)
	}
}

func TestNodeGetBucketProperties(t *testing.T) {
	c := NewConnection(testAddress)
	_, err := c.GetBucketProperties("test")
	if err != nil {
		t.Error(err)
	}
}

func TestNodeSetBucketProperties(t *testing.T) {
	c := NewConnection(testAddress)
	bp := BucketProperties{NVal: 1, AllowMult: true}
	err := c.SetBucketProperties("test", bp)
	if err != nil {
		t.Error(err)
	}
	_, err = c.GetBucketProperties("test")
	if err != nil {
		t.Error(err)
	}
}

func TestNodeUpdateCounter(t *testing.T) {
	c := NewConnection(testAddress)
	err := c.UpdateCounter("test", "testcounter", 100)
	if err != nil {
		t.Error(err)
	}
}

func TestNodeGetCounter(t *testing.T) {
	c := NewConnection(testAddress)
	_, err := c.GetCounter("test", "testcounter")
	if err != nil {
		t.Error(err)
	}
}

func TestNodeDelete(t *testing.T) {
	c := NewConnection(testAddress)
	err := c.Delete("testOperations", "fetch")
	if err != nil {
		t.Error(err)
	}
}

func TestNodeStore(t *testing.T) {
	c := NewConnection(testAddress)
	o := Object{ Bucket : "testOperations", Key : "fetch", Value : []byte("fetchstoretestvalue"),}
	err := c.Store(o)
	if err != nil {
		t.Error(err)
	}
}

func TestNodeFetch(t *testing.T) {
	c := NewConnection(testAddress)
	_, err := c.Fetch("testOperations", "fetch")
	if err != nil {
		t.Error(err)
	}
}

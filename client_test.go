package rgo

import (
	"reflect"
	"testing"
)

func expect(t *testing.T, given interface{}, expected interface{}) {
	if given != expected {
		t.Errorf("Expected %v (type %v) - Got %v (type %v)", expected, reflect.TypeOf(expected), given, reflect.TypeOf(given))
	}
}

func client() *Client {
	return NewClient("213.5.183.10:49155")
}

func TestGetConnection(t *testing.T) {
	cl := client()
	conn := cl.GetConnection()
	expect(t, conn.address, "213.5.183.10:49155")
}

func TestClientConnectionPing(t *testing.T) {
	cl := client()
	conn := cl.GetConnection()
	err := conn.Ping()
	if err != nil {
		t.Error(err)
	}
}

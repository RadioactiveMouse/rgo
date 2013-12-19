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
	return NewClient("127.0.0.1:8098")
}

func TestGetConnection(t *testing.T) {
	cl := client()
	conn := cl.GetConnection()
	expect(t, conn.address, "127.0.0.1:8098")
}

func TestClientConnectionPing(t *testing.T) {
	cl := client()
	conn := cl.GetConnection()
	err := conn.Ping()
	if err != nil {
		t.Error(err)
	}
}

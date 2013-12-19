package rgo

import (
	"log"
	"time"
)

// Client struct maintains the pool of connections
type Client struct {
	pool     chan *Connection
	debug    bool
	pingRate time.Duration
}

// turn on/off debug messages
func (c *Client) Debug(flag bool) {
	c.debug = flag
}

// modify the ping rate
func (c *Client) ChangePingRate(rate time.Duration) {
	c.pingRate = rate
}

// create a new client with a variable number of nodes
func NewClient(addr ...string) *Client {
	client := &Client{
		debug: false,
		pool:  make(chan *Connection, len(addr)),
	}
	for _, addr := range addr {
		client.pool <- NewConnection(addr)
	}
	return client
}

// loop over the available nodes we hold references for to check they respond
func (c *Client) Up() {
	var failcount AtomicInt
	// for range of pool
	for {
		select {
		case <-time.After(c.pingRate):
			for conn := range c.pool {
				err := conn.Ping()
				if err != nil {
					failcount.Add(1)
				}
			}
			if int(failcount.Get()) == len(c.pool) {
				// all nodes unreachable
				log.Println("All nodes down. Cluster unreachable.")
			}
		}
	}
}

// allow accessing of a connection in the pool
func (c *Client) GetConnection() *Connection {
	for {
		conn := <-c.pool
		if conn.alive {
			return conn
		}
		c.pool <- conn
	}
	return nil
}

func (c *Client) Release(conn *Connection) {
	c.pool <- conn
}

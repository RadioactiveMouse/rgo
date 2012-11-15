package main

import (
	"net/http"
	"fmt"
	"errors"
)

type Client struct {
	http bool
	address string
}

func (c Client) Ping() error {
	res, err  := http.Get(c.address)
	defer res.Close()
	if err != nil {
		return errors.New("Error during Ping request")
	}
	return nil
}

func (c Client) Stats() error {
	res, err := http.Get(fmt.Sprintf("%s/stats",c.address)
	if err != nil {
		return errors.New("Error retrieving stats")
	}
	fmt.Println(res)
	return nil
}

func main() {
	c := new(Client)
	c.address = "192.168.0.1"
	err := c.Ping()
	if err != nil {
		log.Fatal(err)
	}
}

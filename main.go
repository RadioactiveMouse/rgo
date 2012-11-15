package main

import (
	"net/http"
	"fmt"
	"errors"
	"log"
)

// Client struct to instantiate a client to utilise Riak
type Client struct {
	http bool
	address string
}

/*
Function to Ping the Server over HTTP
Returns only an error value if it doesn't succeed as per convention
*/
func (c Client) Ping() error {
	res, err  := http.Get(fmt.Sprintf("%s/ping",c.address))
	if err != nil {
		return errors.New("Error during Ping request")
	}
	if res.StatusCode != http.StatusOK {
		return errors.New("Status code is not 200 OK")
	}
	return nil
}

/*
Function to retrieve a map of the configuration of Riak
Returns the map of data and an error value
*/
func (c Client) Stats() error {
	res, err := http.Get(fmt.Sprintf("%s/stats",c.address))
	defer res.Body.Close()
	if err != nil {
		return errors.New("Error retrieving stats")
	}
	return nil
}

/*
Function to return the location of the resources within the cluster
Returns a JSON map of the data and the customary error value
*/
func (c Client) ListResources() error {
	res, err := http.Get(fmt.Sprintf("%s/",c.address))
	defer res.Body.Close()
	if err != nil {
		return errors.New("Error retrieving list of resources")
	}
	fmt.Println(res.Body)
	return nil
}

func main() {
	c := new(Client)
	c.address = "http://localhost:8098"
	err := c.Stats()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Passed!")
}

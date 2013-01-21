package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
)

// Client struct to instantiate a client to utilise Riak
type Client struct {
	http    bool
	address string
}

// struct to hold bucket details
type BucketDetails struct {
	n_val           int32
	allow_mult      bool
	last_write_wins bool
	//precommit
	//postcommit
	r       string
	w       string
	dr      string
	dw      string
	backend string
}

/*
Function to Ping the Server over HTTP
Returns only an error value if it doesn't succeed as per convention
*/
func (c Client) Ping() error {
	res, err := http.Get(fmt.Sprintf("%s/ping", c.address))
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
func (c Client) Stats() (io.ReadCloser, error) {
	res, err := http.Get(fmt.Sprintf("%s/stats", c.address))
	defer res.Body.Close()
	if err != nil {
		return nil, errors.New("Error retrieving stats")
	}
	//fmt.Println(res.Body) // res is an io.ReadCloser so could return that?
	return res.Body, nil
}

/*
Function to return the location of the resources within the cluster
Returns a JSON map of the data and the customary error value
*/
func (c Client) ListResources() (io.ReadCloser, error) {
	res, err := http.Get(fmt.Sprintf("%s/", c.address))
	defer res.Body.Close()
	if err != nil {
		return nil, errors.New("Error retrieving list of resources")
	}
	//fmt.Println(res.Body)
	return res.Body, nil
}

/*
Function to list the buckets that can be queried against
Returns a slice of the buckets in Riak
*/
func (c Client) ListBuckets() error {
	res, err := http.Get(fmt.Sprintf("%s/buckets?buckets=true", c.address))
	defer res.Body.Close()
	if err != nil {
		return errors.New("Error listing the buckets")
	}
	fmt.Println(res.Body)
	return nil
}

/*
Function to list the keys that can be queried against
Returns a slice of the keys in a specific bucket
*/
func (c Client) ListKeys(bucketname string, stream bool) error {
	if stream {
		res, err := http.Get(fmt.Sprintf("%s/buckets/%s/keys?keys=stream", c.address, bucketname))
		defer res.Body.Close()
		if err != nil {
			return errors.New("Error streaming keys")
		}
		fmt.Println(res.Body)
		return nil
	} else {
		res, err := http.Get(fmt.Sprintf("%s/buckets/%s/keys?keys=true", c.address, bucketname))
		defer res.Body.Close()
		if err != nil {
			return errors.New("Error listing the keys")
		}
		fmt.Println(res.Body)
		return nil
	}
	return nil
}

/*
Function to list the properties of a given bucket
Returns a slice of bucket properties
*/
func (c Client) GetBucketDetails(bucketname string) (BucketDetails, error) {
	res, err := http.Get(fmt.Sprintf("%s/buckets/%s/props", c.address, bucketname))
	defer res.Body.Close()
	if err != nil {
		return BucketDetails{}, errors.New("Error getting bucket details")
	}
	// json decode
	details := new(BucketDetails)
	det := json.NewDecoder(res.Body).Decode(details)
	if det != nil {
		return BucketDetails{}, errors.New("Error during details parse.")
	}
	return *details, nil
}

/*
Function to set the Bucket Properties
Returns an error if not successful otherwise returns nil
*/
func (c Client) SetBucketDetails(bucketname string, dets BucketDetails) error {
	// create the body before piping it into the req (use io.Reader in strings)
	body, e := json.Marshal(dets)
	if e != nil {
		return errors.New("Error converting struct to json")
	}
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/buckets/%s/props", c.address, bucketname), bytes.NewBuffer(body))
	if err != nil {
		return errors.New("Error during request creation. Please check the data you passed to the function.")
	}
	res, er := http.DefaultClient.Do(req)
	if er != nil {
		return errors.New("Error during PUT request")
	}
	if res.StatusCode == http.StatusBadRequest {
		return errors.New("Error in submitted JSON")
	}
	if res.StatusCode == http.StatusUnsupportedMediaType {
		return errors.New("Unsupported mediatype")
	}
	return nil
}

/*
Function to set a key value pair in the selected bucket
Returns an error should something fail
*/
func (c Client) Store(bucketname string, key string, payload string) error {
	if bucketname == "" {
		// return error as we don't know where to store the value
		return errors.New("You must specify a bucket to store the value into.")
	}
	if key != "" {
		// do PUT with bucketname and fetch vectorclock
	} else {
		// assume post without user specified key
	}
	return errors.New("An error occured during the storage request.")
}

/*
Function to return return the value stored at a specific key
Returns a string representation of the value
*/
func (c Client) Fetch(bucketname string, key string) (string, error) {
	if bucketname == "" || key == "" {
		return "", errors.New("Please check you have specified a bucket and a key to fetch from")
	}
	res, err := http.Get(fmt.Sprintf("%s/buckets/%s/keys/%s",c.address,bucketname,key))
	if err != nil {
		return "", errors.New("Error during HTTP request.")
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return "", errors.New("Item could not be found")
	}
	// ioutil.ReadAll(res.Body)
	return "", nil
}

/*
Function to delete the value associated a with a given key
Returns an error if the key could not be found
*/
func (c Client) Delete(bucketname string, key string) error {
	// buckets/bucket/keys/key
	if bucketname == "" || key == "" {
		return errors.New("Missing bucket or string for delete to be completed.")
	}	
	request, er := http.NewRequest("DELETE", fmt.Sprintf("%s/buckets/%s/keys/%s", c.address, bucketname, key), nil)
	if er != nil {
		return errors.New("Error during HTTP request formulation.")
	}
	res, err := http.DefaultClient.Do(request)
	if err != nil {
		errors.New("Error during application of request on the server.")
	}
	if res.StatusCode  == http.StatusNoContent {
		return nil
	}
	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.StatusCode == http.StatusBadRequest {
		return errors.New("400 Bad Request")	
	}
	return nil
}

func main() {
	c := new(Client)
	c.address = "http://localhost:8098"
	res, err := c.ListResources()
	fmt.Println(res)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Passed!")
}

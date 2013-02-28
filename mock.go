package rgo

import (
	"log"
	"net/http"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

type Data struct {
	Key	string
	Value	interface{}
	ContentType	string `Content-Type`
	Vclock	bytes.Buffer `X-Riak-VClock`
	Meta	interface{} `X-Riak-Meta-*`
	ETag	string
	LastModified	string `Last-Modified`
	Link	string
}

type Client struct {
	Address	string
	Port	int
	Type	string
}

type HTTPError struct {
	code int
}

func (h *HTTPError) Error() string {
	return "rgo: Error during the operation with HTTP code : " + h.code.String()
}

var (
	requestError = errors.New("Request could not be created.")
	responseError = errors.New("There was a problem when sending the response.")
	parseError = errors.New("There was a problem parsing the JSON request.")
	wrongStatusError = errors.New("The status code received did not match the specifcation.")
)

// Call this function to instantiate a new Client
// Returns a Client
func NewClient(address string, port int, connType string) (*Client,error) {
	c := new(Client)
	c.Address = address
	c.Port = port
	if connType == "http" || connType == "protobuf" {
		c.Type = connType
	} else {
		log.Fatalf("Please specify a correct connection type instead of : " + connType)
	}
	return c, nil
}

// ### Object/Key Operations

// Retrieve the data from the riak database
// takes a string and a key and returns a Data struct
func (c * Client) Fetch(bucket string, key string) (Data, error) {
	var data Data
	if c.Type == "http" {
		response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s/keys/%s",c.Address,c.Port,bucket,key))
		// set appropriate header values
		if error != nil {
			return data, requestError
		}
		switch response.StatusCode {
			case http.StatusOK :
				// marshal the json into a Data struct
				er := json.Unmarshal(response.Body, &data)
				if er != nil {
					return data, parseError
				}
				return data, nil
			case http.MultipleChoices :
				// siblings present
			case http.NotModified :
				// use with http conditionals
			default :
				return data, new(HTTPError){response.StatusCode}
		}
	}
}

// Stores the given data in the database
// Returns the data if return is true
func (c * Client) Store(d Data, bucketName string, toReturn bool)(Data,error) {
	var data Data
	if c.Type == "http" {
		if d.Key == "" {
			// POST request
			encoded, prob := json.Marshal(d)
			if prob != nil {
				return nil, errors.New("Problem encoding the data into JSON.")
			}
			request,error := http.NewRequest("POST",fmt.Sprintf("%s:%i/buckets/%s/keys",c.Address,c.Port,bucketName),encoded)
			if error != nil {
				return nil, requestError
			}
			// set appropriate headers
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, responseError
			}
			if response.StatusCode == http.StatusOK && toReturn == true {
				// marshal the return into a data struct
				er := json.Unmarshal(response.Body, &data)
				if er != nil {
					return nil, parseError
				}
				return data, nil
			} else {
				return data, nil
			}
		} else {
			// PUT request
			encoded, prob := json.Marshal(d)
			if prob != nil {
				return nil, errors.New("Error encoding data into JSON.")
			}
			request, error := http.NewRequest("PUT",fmt.Sprintf("%s:%i/buckets/%s/keys/%s",c.Address,c.Port,bucketName,d.Key),encoded)
			// set appropriate headers
			if error != nil {
				return nil, requestError
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, responseError
			}
			if response.StatusCode == http.StatusOK && toReturn == true {
				er := json.Unmarshal(response.Body,&data)
				if er := nil {
					return nil, parseError
				}
				return data, nil
			} else {
				return data, nil
			}
		}
	}
}

// Deletes the value in a bucket at the given key
// returns an error on failure else assume delete complete
func (c * Client) Delete(bucket string,key string) (error) {
	if c.Type == "http" {
		request, error := http.NewRequest("DELETE", fmt.Sprintf("%s:%i/buckets/%s/keys/%s",c.Address,c.Port,bucket,key))
		//request, error := http.NewRequest()
		// request.Method = "DELETE"
		if error != nil {
			return requestError
		}
		// set appropriate header values
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return responseError
		}
		if response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotFound {
			// success
			return nil
		} else {
			// fail
			return errors.New(err)
		}
	}
}

// ### Server operations

// Ping the server to ensure it's contactable
// Returns an error only if the method was unsuccessful
func (c * Client) Ping() (error) {
	if c.Type == "http" {
		response, error := http.Get("%s:%i/ping",c.Address,c.Port)
		if error != nil {
			return responseError
		}
		if response.StatusCode == http.StatusOK {
			return nil
		} else {
			return wrongStatusError
		}
	}
}

// Get the full status of the Riak cluster
// Returns an arbitrary interface representation of the data
func (c * Client) Status() (interface{}, error) {
	if c.Type == "http" {
		response, error := http.Get("%s:%i/status",c.Address,c.Port)
		if error != nil {
			return nil, responseError
		}
		if response.StatusCode == http.StatusOK {
			var body interface{}
			err := json.Unmarshal(response.body, &body)
			if err != nil {
				return nil, parseError
			}
			return body, nil
		} else {
			return nil, wrongStatusError
		}
	}
}

// Lists the resources in the cluster
// Returns an arbitrary interface representation of the resources
func (c * Client) ListResources() (interface{}, error) {
	if c.Type == "http" {
		response, err := http.Get(fmt.Sprintf("%s:%i/",c.Address,c.Port),nil)
		if err != nil {
			return nil, responseError
		}
		var data interface{}
		err = json.Unmarshal(response.Body,&data)
		if er != nil {
			return nil, parseError
		}
		return data, nil
	}
}

// ### Bucket Operations

// Lists the buckets in a DB (SHOULD NOT BE USED IN PRODUCTION)
// returns an interface listing the buckets in the database
func (c * Client) ListBuckets() (interface{},error) {
	response, error := http.Get(fmt.Sprintf("%s:%i/buckets?buckets=true",c.Address,c.Port))
	if error != nil {
		return nil, responseError
	}
	var d interface{}
	err := json.Unmarshal(response.Body,&d)
	if err != nil {
		return nil, parseError
	}
	return d, nil
}

// Lists the keys for a given bucket (SHOULD NOT BE USED IN PRODUCTION)
// returns an interface listing the keys in a given bucket
func (c * Client) ListKeys(bucket string, stream bool) (interface{},error) {
	if stream {
		response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s/keys?keys=stream",c.Address,c.Port,bucket))
		if error != nil {
			return nil, responseError
		}
		if response.StatusCode == http.StatusOK {
			var data interface{}
			if err := json.Unmarshal(response.Body,&data); err {
				return nil, parseError
			}
			return data, nil
			
		} else {
			return nil, wrongStatusError
		}
	} else {
		response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s/keys?keys=true",c.Address,c.Port,bucket))
		if error != nil {
			return nil, responseError
		}
		if response.StatusCode == http.StatusOK {
			var data interface{}
			if err := json.Unmarshal(response.Body,&data); err {
				return nil, parseError
			}
			return data, nil
		}
	}
}

// Get the listing of the bucket properties
// returns the bucket properties in a list interface
func (c * Client) GetBucketProperties(bucket string) (interface{}, error) {
	response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s",c.Address,c.Port,bucket))
	if error != nil {
		return nil, responseError
	}
	if response.StatusCode == http.StatusOK {
		var data interface{}
		if err := json.UnMarshal(response.Body,&data); err {
			return nil, parseError
		}
		return data, nil
	}
	return nil, wrongStatusError
}

// Set the bucket properties
// returns an error if unsuccessful
func (c * Client) SetBucketProperties(bucket string) (error) {
	response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s",c.Address,c.Port,bucket))
	if error != nil {
		return responseError
	}
	if response.StatusCode == http.StatusOK {
		return nil
	}
	return wrongStatusError
}

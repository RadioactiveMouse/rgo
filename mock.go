package rgo

import (
	"log"
	"net/http"
	"bytes"
	"encoding/json"
)

type Data struct {
	Key	string
	Value	interface{}
	ContentType	string
	Vclock	bytes.Buffer
	Meta	interface{}
	Link	string
}

type Client struct {
	Address	string
	Port	int
	Type	string
}

var (
	requestError = errors.New("Request could not be created.")
	responseError = errors.New("There was a problem when sending the response.")
	parseError = errors.New("There was a problem parsing the JSON request.")
	wrongStatusError = errors.New("The status code received did not match the specifcation.")
)

// Call this function to instantiate a new Client
// Returns a Client
func NewClient(address string, port int, type string) (Client,error) {
	c = new(Client)
	c.Address = address
	c.Port = port
	if type == "http" || type == "protobuf" {
		c.Type = type
	} else {
		return nil, errors.New("Unrecognised type entered. Types are http or protobuf.")
	}
	return c, nil
}

// ### Object/Key Operations

// Retrieve the data from the riak database
// takes a string and a key and returns a Data struct
func (c * Client) Fetch(bucket string, key string) (Data, error) {
	if c.Type == "http" {
		response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s/keys/%s",c.Address,c.Port,bucket,key))
		// set appropriate header values
		if error != nil {
			return nil, requestError
		}
		if response.StatusCode == http.StatusOK {
			// marshal the json into a Data struct
			var data Data
			er := json.Unmarshal(response.Body, &data)
			if er != nil {
				return nil, parseError
			{
			return data, nil
		} else {
			return nil, wrongStatusError
		}
	}
}

// Stores the given data in the database
// Returns the data if return is true
func (c * Client) Store(d Data, return bool)(Data,error) {
	if c.Type == "http" {
		if d.Key == "" {
			// POST request
			encoded, prob := json.Marshal(d)
			if prob != nil {
				return nil, errors.New("Problem encoding the data into JSON.")
			}
			request,error := http.NewRequest("POST",fmt.Sprintf("%s:%i/buckets/%s/keys",c.Address,c.Port,d.BucketName),encoded)
			if error != nil {
				return nil, requestError
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, responseError
			}
			if response.StatusCode == http.StatusOK && return == true {
				// marshal the return into a data struct
				var data Data
				er := json.Unmarshal(response.Body, &data)
				if er != nil {
					return nil, parseError
				}
				return data, nil
			} else {
				return nil, nil
			}
		} else {
			// PUT request
			encoded, prob := json.Marshal(d)
			if prob := nil {
				return nil, errors.New("Error encoding data into JSON.")
			}
			request, error := http.NewRequest("PUT",fmt.Sprintf("%s:%i/buckets/%s/keys/%s",c.Address,c.Port,d.BucketName,d.Key),encoded)
			if error != nil {
				return nil, requestError
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, responseError
			}
			if response.StatusCode == http.StatusOK && return == true {
				var data Data
				er := json.Unmarshal(response.Body,&data)
				if er := nil {
					return nil, parseError
				}
				return data, nil
			} else {
				return nil, nil
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

// #### Server operations

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
		var data inteface{}
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

}

// Set the bucket properties
// returns an error if unsuccessful
func (c * Client) SetBucketProperties(bucket string) (error) {

}

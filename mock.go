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
	parseError = errors.New("There was a problem parsing the request.")

)

func NewClient(address string, port int, type string) (Client,error) {
	c = new(Client)
	c.Address = address
	c.Port = port
	if type == "http" || type == "protobuf" {
		c.Type = type
	} else {
		return nil, errors.New("Unrecognised type entered.")
	}
	return c, nil
}

// ### Object/Key Operations

func (c * Client) Fetch(bucket string, key string) (Data, error) {
	if c.Type == "http" {
		response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s/keys/%s",c.Address,c.Port,bucket,key))
		// set appropriate header values
		if error != nil {
			return nil, errors.New("Error creating HTTP request.")
		}
		if response.StatusCode == http.StatusOK {
			// marshal the json into a Data struct
			var data Data
			er := json.Unmarshal(response.Body, &data)
			if er != nil {
				return nil, errors.New("Error decoding response from server.")
			{
			return data, nil
		} else {
			return nil, errors.New("Non-OK error response returned.")
		}
	}
}

func (c * Client) Store(d Data, return bool)(Data,error) {
	if c.Type == "http" {
		if d.Key == "" {
			// POST request
			encoded, prob := json.Marshal(d)
			if prob != nil {
				return nil, errors.New("Problem encoding the data into JSON.")
			}
			request,error := http.NewRequest("POST",fmt.Sprintf("%s:%i/buckets/%s/keys",c.Address,c.Port,d.BucketName),encoded)
			// request, error := http.NewRequest()
			if error != nil {
				return nil, errors.New("Error creating HTTP request from Data.")
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, errors.New("Error during HTTP request.")
			}
			if response.StatusCode == http.StatusOK && return == true {
				// marshal the return into a data struct
				var data Data
				er := json.Unmarshal(response.Body, &data)
				if er != nil {
					return nil, errors.New("Error decoding response from the server.")
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
				return nil, errors.New("Error creating HTTP request from Data.")
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, errors.New("Error during HTTP request.")
			}
			if response.StatusCode == http.StatusOK && return == true {
				var data Data
				er := json.Unmarshal(response.Body,&data)
				if er := nil {
					return nil, errors.New("Error decoding response from the server.")
				}
				return data, nil
			} else {
				return nil, nil
			}
		}
	}
}

func (c * Client) Delete(bucket string,key string) (error) {
	if c.Type == "http" {
		request, error := http.NewRequest("DELETE", fmt.Sprintf("%s:%i/buckets/%s/keys/%s",c.Address,c.Port,bucket,key))
		//request, error := http.NewRequest()
		// request.Method = "DELETE"
		if error != nil {
			return errors.New("Error during request creation. Please check the data you are inputting.")
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return errors.New("Error during request to the server.")
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

func (c * Client) Ping() (error) {
	if c.Type == "http" {
		response, error := http.Get("%s:%i/ping",c.Address,c.Port)
		if error != nil {
			return errors.New("Error during HTTP response.")
		}
		if response.StatusCode == http.StatusOK {
			return nil
		} else {
			return errors.New("Non 200 OK response returned.")
		}
	}
}

func (c * Client) Status() (interface{}, error) {
	if c.Type == "http" {
		response, error := http.Get("%s:%i/status",c.Address,c.Port)
		if error != nil {
			return nil, errors.New("Error during HTTP response.")
		}
		if response.StatusCode == http.StatusOK {
			var body interface{}
			err := json.Unmarshal(response.body, &body)
			if err != nil {
				return nil, errors.New("Error reading body of the HTTP response.")
			}
			return body, nil
		}
	}
}

func (c * Client) ListResources() (interface{}, error) {
	if c.Type == "http" {
		response, err := http.Get(fmt.Sprintf("%s:%i/",c.Address,c.Port),nil)
		if err != nil {
			return nil, errors.New("Error during HTTP response.")
		}
		var data inteface{}
		err = json.Unmarshal(response.Body,&data)
		if er != nil {
			return nil, errors.New("Error during reading of HTTP response.")
		}
		return data, nil
	}
}

// ### Bucket Operations

func (c * Client) ListBuckets() (interface{},error) {
	response, error := http.Get(fmt.Sprintf("%s:%i/buckets?buckets=true",c.Address,c.Port))
	if error != nil {
		return nil, errors.New("Error during HTTP response.")
	}
	var d interface{}
	err := json.Unmarshal(response.Body,&d)
	if err != nil {
		return nil, errors.New("Error during parsing of HTTP response.")
	}
	return d, nil
}

func (c * Client) ListKeys(bucket string, stream bool) (interface{},error) {
	if stream {
		response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s/keys?keys=stream",c.Address,c.Port,bucket))
		if error != nil {
			return nil, errors.New("Error during HTTP request.")
		}
		if response.StatusCode == http.StatusOK {
			var data interface{}
			if err := json.Unmarshal(response.Body,&data); err {
				return nil, errors.New("Error during JSON parsing.")
			}
			return data, nil
			
		} else {
			return nil, errors.New("Non OK status code.")
		}
	} else {
		response, error := http.Get(fmt.Sprintf("%s:%i/buckets/%s/keys?keys=true",c.Address,c.Port,bucket))
		if error != nil {
			return nil, errors.New("Error during HTTP request.")
		}
		if response.StatusCode == http.StatusOK {
			var data interface{}
			if err := json.Unmarshal(response.Body,&data); err {
				return nil, errors.New("Error during JSON parsing.")
			}
			return data, nil
		}
	}
}

func (c * Client) GetBucketProperties(bucket string) (interface{}, error) {

}

func (c * Client) SetBucketProperties(bucket string) (error) {

}

package rgo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// Encapsulate the client to provide struct to attach methods to
type Client struct {
	Address string
	Port    int
	Type    string
	Log     bool
}

// Fetch a single piece of  data from the Riak cluster
func (self *Client) Fetch(bucket string, key string) ([]byte, error) {
	r := http.Response{}
	if bucket == "" || key == "" {
		return nil, errors.New(fmt.Sprintf("Please specify both a bucket [%v] and a key [%v]", bucket, key))
	}
	path := fmt.Sprintf("/buckets/%s/keys/%s", bucket, key)
	err := self.query("GET", path, nil, nil, &r)
	defer r.Body.Close()
	// check the status codes
	if r.StatusCode > 304 {
		return nil, errors.New(fmt.Sprintf("Error during fetch operation, code: %d", r.StatusCode))
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Delete a single item in a given bucket with a given key from the Riak cluster
func (self *Client) Delete(bucket string, key string) error {
	r := http.Response{}
	if bucket == "" || key == "" {
		return errors.New(fmt.Sprintf("Please specify both a bucket [%v] and a key [%v]", bucket, key))
	}
	path := fmt.Sprintf("/buckets/%s/keys/%s", bucket, key)
	err := self.query("DELETE", path, nil, nil, &r)
	if r.StatusCode == 400 {
		return errors.New(fmt.Sprintf("Error during delete operation, code: %d", r.StatusCode))
	}
	if self.Log {
		fmt.Println("Status code for Delete operation is : ", r.StatusCode)
	}
	return err
}

// Store a single piece of data in the Riak cluster
func (self *Client) Store(bucket string, data *Data) ([]byte, error) {
	path := ""
	resp := http.Response{}
	// check if the key exists
	if data.Value == "" {
		return "", errors.New("RGO: no value defined for the key")
	}
	if data.Key != "" {
		// put
		path = fmt.Sprintf("/buckets/%s/keys/%s", bucket, data.Key)
		//values := url.Values{{data.value}}
		body := strings.NewReader(data.Value)
		err := self.query("PUT", path, nil, body, &resp)
		if err != nil {
			return "", err
		}
	} else {
		//post
		path = fmt.Sprintf("/buckets/%s/keys", bucket)
		//values := url.Values{{data.Value}}
		body := strings.NewReader(data.Value)
		err := self.query("POST", path, nil, body, &resp)
		if err != nil {
			return "", err
		}
	}

	// catch errors 400 404
	if resp.StatusCode > 300 {
		return "", errors.New(fmt.Sprintf("Error during store request code : %d", resp.StatusCode))
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return "", err
	}
	return body, nil
}

// Check to see if the Riak node is responding to the Client
func (self *Client) Ping() error {
	path := "/ping"
	r := http.Response{}
	err := self.query("GET", path, nil, nil, &r)
	if r.StatusCode != 200 {
		return err
	}
	return nil
}

// Check the status of the Riak Cluster
func (self *Client) Status() (Status, error) {
	path := "/stats"
	data := Status{}
	r := http.Response{}
	err := self.query("GET", path, nil, nil, &r)
	if r.StatusCode == 404 {
		return nil, errors.New(fmt.Sprintf("Error during status operation, status code : %d", r.StatusCode))
	} else if err != nil {
		return nil, err
	}
	body, error := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if error != nil {
		return nil, error
	}
	parseError := json.Unmarshal(body, &data)
	if parseError != nil {
		return nil, parseError
	}
	return data, parseError
}

// List the different endpoints exposed by the Riak Cluster
func (self *Client) ListResources() (Resources{}, error) {
	path := "/"
	data := Resources{}
	r := http.Response{}
	err := self.query("GET", path, nil,nil, &r)
	if r.StatusCode != 200 {
		// error
	} else if err != nil {
		return nil, err
	}
	body, error := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if error != nil {
		return nil, error
	}
	parseError := json.Unmarshal(body, &data)
	if parseError != nil {
		return nil, parseError
	}
	return data, parseError
}

// function to list the keys in a given bucket
func (self * Client) listKeys(name string) ([]string,error) {
	
}

// function to list the buckets in a cluster
func (self * Client) listBuckets() ([]string,error) {

}

// get the properties of a given bucket
func (self *Client) getBucketProperties(name string) error {
	return nil
}

// set the bucket properties
func (self *Client) setBucketProperties(b Bucket) error {
	type props struct {
		n_val	int
		allow_mult	bool
		last_write_wins	bool
		r	string
		w	string
		dw	string
		rw	string
	}
	props.n_val = b.nval
	props.allow_mult = b.allowMult
	props.last_write_wins = b.lastWriteWins
	props.r = b.r
	props.w = b.w
	props.dw = b.dw
	props.rw = b.rw
	// package as single JSON
	encoded, jsonErr := json.Marshal(props)
	if jsonErr != nil {
		return jsonErr
	} else {
		// send the properties that were just encoded to the server
	}
}

// reset bucket properties
func (self *Client) resetBucketProperties(name string) error {
	return nil
}

func (self *Client) Bucket(name string) (*Bucket) {
	b := new(Bucket)
	b.Name = name
	b.Client = self
	return b
}

type RiakResponse struct {
	StatusCode	int
	Body	[]byte
	ContentType	string
}

// query
func (self *Client) httpQuery(method string, path string, bucket Bucket, caller string, data Data) (RiakResponse,error) {
	// construct base url
	riakurl := fmt.Sprintf("%s:%d",self.Address,self.Port)
	endpoint, urlErr := url.Endpoint(riakurl)
	if urlErr != nil {
		return nil, urlErr
	}
	endpoint.Path = path
	// create the request so we can add and modify it before sending
	request, err := http.NewRequest(method,endpoint.String(),nil)
	switch caller
		case "store":
			// make sure content-type is set here
			request.Header.Set("Content-Type", data.ContentType)
			// ensure that the body is always returned
			returnBod := url.Values{"returnbody": {"true"}}
			endpoint.RawQuery = returnBod.Encode()
			if method == "PUT" {
				// in place update
			} else {
				// new value
			}
		case "bucketprops":
			// content-type = application/json
			request.Header.Set("Content-Type", "application/json")
			if method == "PUT" {
				// bucket update
			} else if method == "DELETE" {
				// bucket props reset
			} else {
				// assume GET
			}
		case "delete":
			// delete using the bucket name
		case "fetch":
			// fetch has specific key
			// accept header *
		case default:
			// generic GET for PING, LISTRESOURCES etc
	// actually execute the prepackaged query
	response, respError := http.DefaultClient.Do(query)
	if respError != nil {
		return nil, respError
	}
	// parse the required values into the RiakResponse for passing back to the caller function
	rr := RiakResponse{}
	bod, ioErr := ioutil.ReadAll(response.Body)
	if ioErr != nil {
		// error with body
		return rr, ioErr
	}
	rr.Body = bod
	return rr, nil
}

// function to create the request
func (self * Client) createRequest(b Bucket, d Data) http.Request {
	// generate the request object
	// attach the content type as stored in d
	
}

// function encapsulates the query logic of connecting to the database
func (self *Client) query(method string, path string, values url.Values, body io.Reader, r *http.Response) error {
	// construct the base URL
	riakurl := fmt.Sprintf("%s:%d", self.Address, self.Port)
	endpoint, err := url.Parse(riakurl)
	if err != nil {
		return err
	}
	endpoint.Path = path
	if method == "GET" {
		endpoint.RawQuery = values.Encode()
	}
	
	// assume PUT or POST
	if method != "GET" {
		returnBod := url.Values{"returnbody": {"true"}}
		endpoint.RawQuery = returnBod.Encode()
	}

	if self.Log {
		fmt.Println("RGO :", method, endpoint.String())
	}

	request, err := http.NewRequest(method, endpoint.String(), body)
	if err != nil {
		return err
	}

	// set the correct headers for the data TODO: needs some sort of ability to change the content-type
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	
	response, err := http.DefaultClient.Do(request)
	*r = *response

	return err
}

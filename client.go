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

type Client struct {
	Address string
	Port    int
	Type    string
	Log     bool
}

func (self *Client) Fetch(bucket string, key string) (interface{}, error) {
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
	return string(body), nil
}

func (self *Client) Delete(bucket string, key string) error {
	r := http.Response{}
	if bucket == "" || key == "" {
		return errors.New(fmt.Sprintf("Please specify both a bucket [%v] and a key [%v]", bucket, key))
	}
	path := fmt.Sprintf("/buckets/%s/keys/%s", bucket, key)
	//headers := http.Header{}
	err := self.query("DELETE", path, nil, nil, &r)
	if r.StatusCode == 400 {
		return errors.New(fmt.Sprintf("Error during delete operation, code: %d", r.StatusCode))
	}
	if self.Log {
		fmt.Println("Status code for Delete operation is : ", r.StatusCode)
	}
	return err
}

func (self *Client) Store(bucket string, data *Data) (string, error) {
	// check if the key exists for conditional put/post
	path := ""
	resp := http.Response{}
	// check if the key exists
	if data.value == "" {
		return "", errors.New("RGO: no value defined for the key")
	}
	if data.key != "" {
		// put
		path = fmt.Sprintf("/buckets/%s/keys/%s", bucket, data.key)
		//values := url.Values{{data.value}}
		body := strings.NewReader(data.value)
		err := self.query("PUT", path, nil, body, &resp)
		if err != nil {
			return "", err
		}
	} else {
		//post
		path = fmt.Sprintf("/buckets/%s/keys", bucket)
		//values := url.Values{{data.value}}
		body := strings.NewReader(data.value)
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
	return string(body), nil
}

func (self *Client) Ping() error {
	path := "/ping"
	r := http.Response{}
	err := self.query("GET", path, nil, nil, &r)
	if r.StatusCode != 200 {
		return err
	}
	return nil
}

func (self *Client) Status() (interface{}, error) {
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

func (self *Client) ListResources() (interface{}, error) {
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
	//	fmt.Println(values.Encode())
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

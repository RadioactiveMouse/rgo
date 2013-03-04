package rgo

import (
	"net/url"
	"net/http"
	"fmt"
	"errors"
	"io"
	"io/ioutil"
	"encoding/json"
	"strings"
)

type Client struct {
	Address string
	Port	int
	Type	string
	Log	bool
}

func (self *Client) Fetch(bucket string, key string) (*Data, error) {
	data := Data{}
	r := http.Response{}
	if bucket == "" || key == "" {	
		return &data, errors.New(fmt.Sprintf("Please specify both a bucket [%v] and a key [%v]",bucket,key))
	}
	path := fmt.Sprintf("/buckets/%s/keys/%s",bucket,key)
	err := self.query("GET",path,nil,&r)
	defer r.Body.Close()
	// check the status codes
	if r.StatusCode > 304 {
		return &data, errors.New(fmt.Sprintf("Error during fetch operation, code: %d",r.StatusCode))
	}
	if r.ContentLength <= 0 {
		return &data, errors.New(fmt.Sprintf("Error during fetch operation, Content-Length : %d",r.ContentLength))
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return &data, err
	}
	error := json.Unmarshal(body,&data) 
	return &data, error
}

func (self *Client) Delete(bucket string, key string) (error) {
	r := http.Response{}
	if bucket == "" || key == "" {
		return errors.New(fmt.Sprintf("Please specify both a bucket [%v] and a key [%v]",bucket,key))
	}
	path := fmt.Sprintf("/buckets/%s/keys/%s",bucket,key)
	err := self.query("DELETE",path,nil,&r)
	if r.StatusCode == 400 {
		return errors.New(fmt.Sprintf("Error during delete operation, code: %d",r.StatusCode))
	}
	if self.Log {
		fmt.Println("Status code for Delete operation is : ",r.StatusCode)
	}
	return err
}

func (self *Client) Store(bucket string, returnBody bool, data *Data) (*Data, error) {
	// check if the key exists for conditional put/post
	path :=  "" 
	returnData := Data{}
	resp := http.Response{}
	// check if the key exists
	if data.value == "" {
		return &returnData, errors.New("RGO: no value defined for the key")
	}
	if data.key != "" {
		// put
		path = fmt.Sprintf("/buckets/%s/keys/%s",bucket,data.key)
		values := url.Values{data.key:{data.value}}
		err := self.query("PUT",path,values,&resp)
		if err != nil {
			return &returnData, err
		}
	} else {
		//post
		path = fmt.Sprintf("/buckets/%s/keys",bucket)
		values := url.Values{"":{data.value}}
		err := self.query("POST",path,values,&resp)
		if err != nil {
			return &returnData, err
		}
	}

	if self.Log {
		fmt.Println("Returned r : ", resp.Body)
	}
	
	// catch errors 400 404
	if resp.StatusCode > 300 {
		return &returnData, errors.New(fmt.Sprintf("Error during store request code : %d",resp.StatusCode))
	}

	// content-length should always be > 0 as we specify returnbody=true
	if resp.ContentLength <= 0 {
		return &returnData, errors.New(fmt.Sprintf("Error during store operation, content-length : %d",resp.ContentLength))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if self.Log {
		fmt.Println("Body : ", body)
	}
	defer resp.Body.Close()
	if err != nil {
		return &returnData, err
	}	
	error := json.Unmarshal(body, &returnData)
	
	return &returnData, error
}

func (self *Client) Ping() (error) {
	path := "/ping"
	r := http.Response{}
	err := self.query("GET",path,nil,&r)
	if r.StatusCode != 200 {
		return err
	}
	return nil
}

func (self *Client) Status() (interface{},error) {
	path := "/stats"
	data := Data{}
	r := http.Response{}
	err := self.query("GET",path,nil,&r)
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
	fmt.Println("Before unmarshal : ",&body)
	parseError := json.Unmarshal(body,&data)
	if parseError != nil {
		return nil, parseError
	}
	if self.Log {
		fmt.Println("Status : ", data)
	}
	return data, parseError
}

func (self *Client) query(method string, path string, values url.Values, r *http.Response) (error) {
	// construct the base URL
	riakurl := fmt.Sprintf("%s:%d",self.Address,self.Port)
	endpoint, err := url.Parse(riakurl)
	if err != nil {
		return err
	}
	endpoint.Path = path
	if method == "GET" {
		endpoint.RawQuery = values.Encode()
	}

	var body io.Reader
	if method != "GET" && values != nil {
		returnBod := url.Values{"returnbody" : {"true"}}
		endpoint.RawQuery = returnBod.Encode()
		body = strings.NewReader(values.Encode())
	}

	if self.Log {
		fmt.Println("RGO :", method, endpoint.String())
		fmt.Println(values.Encode()) 
	}

	request, err := http.NewRequest(method, endpoint.String(), body)
	if err != nil {
		return err
	}

	// set the correct headers for the data TODO: needs some sort of ability to change the content-type
	request.Header.Set("Content-Type","application/json")

	response, err := http.DefaultClient.Do(request)
	*r = *response

	return err
}


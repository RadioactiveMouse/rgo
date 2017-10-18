package rgo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Connection struct encapsulates all the query logic
type Connection struct {
	address   string
	port      int
	transport http.RoundTripper
	alive     bool
	debug     bool
	Stats     Stats
}

// NewConnection returns a connection with the specified address populated.
// By default it uses http.DefaultTransport
func NewConnection(addr string) *Connection {
	return &Connection{
		address:   addr,
		alive:     true,
		transport: http.DefaultTransport,
	}
}

// Ping the server to ensure it is up.
// For examples of usage look at Client.IsUp()
func (c *Connection) Ping() error {
	if c.debug {
		log.Printf("connection address : %s \nalive : %s", c.address, c.alive)
	}
	if c.transport == nil {
		log.Printf("dialling: %s", c.address)
		c.transport = http.DefaultTransport
	}
	c.Stats.Pings.Add(1)
	path := fmt.Sprintf("http://%s/ping", c.address)
	request, err := http.NewRequest("GET", path, nil)
	if err != nil {
		c.Stats.PingErrors.Add(1)
		return err
	} else {
		c.Stats.Pings.Add(1)
		_, requestErr := c.transport.RoundTrip(request)
		if requestErr != nil {
			c.Stats.PingErrors.Add(1)
			log.Print(requestErr)
		}
		return requestErr
	}

}

// Status() returns an struct of type Status enumerating the full set of stats held for the node this connection exists for.
func (c *Connection) Status() (Status, error) {
	var s Status
	path := fmt.Sprintf("http://%s/stats", c.address)
	request, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return s, err
	}
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return s, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return s, errors.New("riak_kv_stat is not enabled")
	}
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return s, err
	}
	err = json.Unmarshal(b, &s)
	return s, err
}

// Returns a Resource struct enumerating the resources available on the node this connection exists for.
func (c *Connection) ListResources() (Resources, error) {
	var r Resources
	path := fmt.Sprintf("http://%s/", c.address)
	request, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return r, err
	}
	request.Header.Set("Accept", "application/json")
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return r, err
	}
	defer response.Body.Close()
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

// CAUTION : Not recommended for production usage
// Traverses the whole cluster to retrieve the names of the buckets
func (c *Connection) ListBuckets() ([]string, error) {
	path := fmt.Sprintf("http://%s/buckets", c.address)
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	query.Set("buckets", "true")
	u.RawQuery = query.Encode()
	request, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	type Buckets struct {
		buckets []string
	}
	var data Buckets
	data.buckets = make([]string, 10)
	// TODO make the slice or designate as zero and append (probably expensive)
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return data.buckets, err
	}
	defer response.Body.Close()
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return data.buckets, err
	}
	err = json.Unmarshal(b, &data)
	return data.buckets, err
}

// CAUTION : Not for production usage
// Returns all the keys for a given bucket.
func (c *Connection) ListKeys(bucket string) ([]string, error) {
	path := fmt.Sprintf("http://%s/buckets/%s/keys", c.address, bucket)
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	query.Set("keys", "true")
	u.RawQuery = query.Encode()
	request, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	type Keys struct {
		keys []string
	}
	var data Keys
	data.keys = make([]string, 10)
	// TODO make the slice or designate as zero and append (probably expensive)
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return data.keys, err
	}
	defer response.Body.Close()
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return data.keys, err
	}
	err = json.Unmarshal(b, &data)
	return data.keys, err
}

// Resets the bucket properties to their default values
func (c *Connection) ResetBucketProperties(bucket string) error {
	path := fmt.Sprintf("http://%s/buckets/%s/props", c.address, bucket)
	request, err := http.NewRequest("DELETE", path, nil)
	if err != nil {
		return err
	}
	_, err = c.transport.RoundTrip(request)
	return err
}

// Gets the bucket properties from the request bucket
func (c *Connection) GetBucketProperties(bucket string) (BucketProperties, error) {
	path := fmt.Sprintf("http://%s/buckets/%s/props", c.address, bucket)
	type BucketProps struct {
		Props BucketProperties `json:"props"`
	}
	var bp BucketProps
	request, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return bp.Props, err
	}
	request.Header.Set("Accept", "application/json")
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return bp.Props, err
	}
	defer response.Body.Close()
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return bp.Props, err
	}
	err = json.Unmarshal(b, &bp)
	return bp.Props, err
}

// Set the properties for a given bucket. Currently only 3 options are enabled and must all be specified.
func (c *Connection) SetBucketProperties(bucket string, bp BucketProperties) error {
	path := fmt.Sprintf("http://%s/buckets/%s/props", c.address, bucket)
	type BucketProps struct {
		Props BucketProperties `json:"props"`
	}
	props := BucketProps{Props: bp}
	js, err := json.Marshal(props)
	if err != nil {
		return err
	}
	body := strings.NewReader(string(js))
	request, err := http.NewRequest("PUT", path, body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusBadRequest {
		return errors.New("There is an invalid field in the Bucket Properties")
	}
	return nil
}

func (c *Connection) UpdateCounter(bucket string, key string, value int64) error {
	path := fmt.Sprintf("http://%s/buckets/%s/counters/%s", c.address, bucket, key)
	count := strconv.FormatInt(value, 10)
	body := strings.NewReader(count)
	request, err := http.NewRequest("POST", path, body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	_, err = c.transport.RoundTrip(request)
	return err
}

func (c *Connection) GetCounter(bucket string, key string) (*Counter, error) {
	counter := new(Counter)
	// check to make sure the bucket is enabled for counters
	bp, err := c.GetBucketProperties(bucket)
	if err != nil {
		return counter, err
	}
	if bp.AllowMult == false {
		return counter, errors.New("Please modify the bucket properties and set allow_mult to true")
	}
	path := fmt.Sprintf("http://%s/buckets/%s/counters/%s", c.address, bucket, key)
	request, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return counter, err
	}
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return counter, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return counter, errors.New("Counter with that key not found")
	}
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return counter, err
	}
	var i int64
	err = json.Unmarshal(b, &i)
	if err != nil {
		return counter, err
	}
	counter.Count = i
	return counter, nil
}

func (c *Connection) Delete(bucket string, key string) error {
	path := fmt.Sprintf("http://%s/buckets/%s/keys/%s", c.address, bucket, key)
	request, err := http.NewRequest("DELETE", path, nil)
	if err != nil {
		return err
	}
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusBadRequest {
		return errors.New("rw parameter is likely invalid")
	}
	return nil
}

func (c *Connection) Fetch(bucket string, key string) (*Object, error) {
	path := fmt.Sprintf("http://%s/buckets/%s/keys/%s", c.address, bucket, key)
	request, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusBadRequest {
		return nil, errors.New("r parameter may be invalid")
	} else if response.StatusCode == http.StatusNotFound {
		return nil, errors.New("object not found")
	} else if response.StatusCode == http.StatusServiceUnavailable {
		return nil, errors.New("request timed out")
	}
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return &Object{
		Bucket:       bucket,
		Key:          key,
		ContentType:  response.Header.Get("Content-Type"),
		vClock:       response.Header.Get("X-Riak-Vclock"),
		lastModified: response.Header.Get("Last-Modified"),
		eTag:         response.Header.Get("Etag"),
		link:         response.Header.Get("Link"),
		Value:        b,
	}, nil
}

func (c *Connection) Store(o Object) error {
	var path string
	var action string
	if o.Key == "" {
		path = fmt.Sprintf("http://%s/buckets/%s/keys", c.address, o.Bucket)
		action = "POST"
	} else {
		path = fmt.Sprintf("http://%s/buckets/%s/keys/%s", c.address, o.Bucket, o.Key)
		action = "PUT"
	}
	body := strings.NewReader(string(o.Value))
	request, err := http.NewRequest(action, path, body)
	if err != nil {
		return err
	}
	var contentType string
	// do the header manipulation here
	if o.ContentType == "" {
		contentType = http.DetectContentType(o.Value)
	} else {
		contentType = o.ContentType
	}
	request.Header.Set("Content-Type", contentType)
	response, err := c.transport.RoundTrip(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusBadRequest {
		return errors.New("r, w or dw are likely invalid")
	} else if response.StatusCode == http.StatusPreconditionFailed {
		return errors.New("one of the conditional headers failed to match")
	}
	return nil
}

// gracefully shuts down the connection to the node and marks it as not alive
func (c *Connection) Close() {
	c.alive = false
	if c.transport != nil {
		//	c.transport.CloseIdleConnections()
	}
}

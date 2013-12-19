package rgo

type Object struct {
	vClock       string   `json:"X-Riak-Vclock"`
	link         string   `json:"Link"`
	lastModified string   `json:"Last-Modified"`
	eTag         string   `json:"Etag"`
	meta         string   `json:"X-Riak-Meta-*"`
	Indices      []string `json:"X-Riak-Index-*"`
	ContentType  string   `json:"Content-Type"`
	client       Client
	Bucket       string
	Key          string
	Value        []byte
}

func (o *Object) Store() error {
	return nil
}

func (o *Object) Fetch() ([]byte, error) {
	return nil, nil
}

func (o *Object) Delete() error {
	return nil
}

func NewObject(bucket string) *Object {
	obj := new(Object)
	obj.Bucket = bucket
	return obj
}

package rgo

import (
	"errors"
)

// Bucket structure to encapsulate bucket operations
type Bucket struct {
	Name	string
	Client	Client
	nval	int // number of replicas for objects in this bucket
	allowMult	bool // sibling creation
	lastWriteWins	bool // ignore object history(vector clocks) when writing
	precommit	interface{} // NOT SUPPORTED
	postcommit	interface{} // NOT SUPPORTED
	r	interface{}
	d	interface{}
	dw	interface{}
	rw	interface{}
	backend	interface{}
}

/*
	Function to store data in the bucket
	Returns : Error if store could not be completed
*/
func (b*Bucket) Store(key string, data interface{}) ([]byte,error) {
	d := Data{}
	d.key = key
	d.value = data
	return b.Client.Store(b.Name,&d)
}

/*
	Function to delete the value associated with a given key
	Returns : Error if key not found
*/
func (b*Bucket) Delete(key string) error {
	return b.Client.Delete(b.Name,key)
}

/*
	Function to Fetch a value given it's key from the bucket
	Returns : value and error if cannot be found
*/
func (b*Bucket) Fetch(key string) ([]byte,error) {
	return b.Client.Fetch(b.Name,key)
}

/*
	Function to List all the keys in the bucket
	NOTE: not suitable for production usage
	Returns : Slice of key names and an error
*/
func (b*Bucket) ListKeys() ([]string,error) {
	return b.Client.listKeys(b.Name)
}

/*
	Function to List all the buckets in the cluster
	NOTE: not suitable for production usage
	Returns : Slice of bucket names and an error
*/
func (b*Bucket) ListBuckets() ([]string,error) {
	return b.Client.listBuckets()
}

/*
	Function to get the current bucket properties
	Returns : error if communication fails
*/
func (b*Bucket) GetBucketProperties() error {
	return b.Client.getBucketProperties(b.Name)
}

/*
	Function to set the bucket specfic properties
	Returns : Error if set operation fails
*/
func (b*Bucket) UpdateBucketProperties(prop interface{}) error {
	if !b.isQuorumType(prop) {
		return errors.New("Bucket property p is not a valid quorum value")
	}
	return b.Client.setBucketProperties(*b)
}

/*
	Function to reset the bucket back to cluster default settings
	Returns : Error should the operation not succeed.
*/
func (b*Bucket) ResetBucketProperties() error {
	return b.Client.resetBucketProperties(b.Name)
}

/*
	Function to ensure quorum type is one of the values allowed
	Returns : true if quorum is valid otherwise false
*/
func (b*Bucket) isQuorumType(value interface{}) bool {
	if str, tick := value.(string); tick {
		if str == "all" || str == "one" || str == "quorum" {
			return true
		}
	} else if num, ok := value.(int); ok {
		if num > 0 && num <= b.nval {
			return true
		}
	}
	return false
}

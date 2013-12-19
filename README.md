#rgo

Riak Go binding for HTTP support.

Current Version : 0.1

##Installation

Recommended only for development of the library in the current state.

Install using :

<code>go get http://github.com/RadioactiveMouse/rgo</code>

##Current Status

Not stable enough for production.

### Implemented
Bucket Operations
* ListKeys
* ListBuckets
* GetBucketProperties
* SetBucketProperties
* ResetBucketProperties

Object/Key Operations
* Fetch
* Store
* Delete

DataTypes
* Counters

Server Operations
* Ping
* Status
* ListResources

### Still to be implemented
Query Operations
* Link Walking
* MapReduce
* Secondary Indexes

Object/Key Operations
* Siblings are not currently handled
* Conditional queries are not supported in this version

## Features
Rgo uses the very familiar idea within Riak of being centered around Buckets. So creating/getting a bucket and then storing a value to the bucket can be done like this.
<code>
	client := rgo.NewClient("127.0.0.1:8098")
	newBucket := client.Bucket("test")
	obj := newBucket.Object()
	obj.Key = "testvalue"
	obj.Value = []byte("storedValue")
	obj.Store()
</code>
## License

MIT

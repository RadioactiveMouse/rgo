#rgo

Riak Go binding based around interoperability with both HTTP and protocol buffer support. The aim is to provide a relatively unified interface for both interfaces.

##Installation
Recommended only for development of the library in the current state.
Once completed installation will be by:

<code>go get http://github.com/RadioactiveMouse/rgo</code>

##Current Status:
Incomplete with many things broken/not implemented.
Not suitable for use in development or production.

## Features
Rgo uses the very familiar idea within Riak of being centered around Buckets. So creating/getting a bucket and then storing a value to the bucket can be done like this.

	newBucket := rgo.Bucket("test")
	newBucket.Store("key","value")

## License

MIT

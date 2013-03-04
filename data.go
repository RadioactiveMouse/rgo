package rgo

import (
	

)
/*
type Response struct {
	Vclock	string	`json:X-Riak-Vclock`
	Vary	string	`json:Vary`
	ContentType	string	`json:Content-Type`
	Link	string	`json:Link`
	Last-Modified	string	`json:Last-Modified`
	Etag	string	`json:ETag`
	Body	Data
}
*/
type Data struct {
	key	string
	value	string
}

package rgo

import (
	"testing"
	"fmt"
)

type Datum struct {
	data	Data
	pass	bool
}

var(
	storeTests =[]*Datum{
		&Datum{Data{"test","value"},true}, // data with key and value should pass
		&Datum{Data{"","onlyvalue"},true}, // data with no key should pass
		&Datum{Data{"",""},false}, // data with no key or value set should fail
		&Datum{Data{"key",""},false}, // key but no value should fail
	}
	deleteTests =[]*Datum{
		&Datum{Data{"test",""},true}, // should pass as test is previously declared
		&Datum{Data{"",""},false}, // should fail as no key is defined
	}
	fetchTests = []*Datum{
		&Datum{Data{"",""}, false}, // should fail due to no key
		&Datum{Data{"test",""}, true}, // should succeed as test is a valid key
	}
	client = Client{Address :"http://127.0.0.1",Port : 8098,Type : "http",Log :false}
)

/*
	Testing for the exposed client methods.
	Requires a working Riak installation on localhost:8098
*/
func TestPing(t * testing.T) {
	err := client.Ping()
	if err != nil {
		t.Errorf("Please check your riak installation is configured correctly as it is returning this error : ", err)
	}
	fmt.Println("Riak installation is reachable.")
}

func TestStatus(t * testing.T) {
	_, err := client.Status()
	if err != nil {
		t.Errorf("Error during Status command, err : ",err)
	}

}

func TestListResources(t * testing.T) {
	_, err := client.ListResources()
	if err != nil {
		t.Errorf("Error during List Resources command, err : ",err)
	}
}

func TestStoreData(t *testing.T) {
	for _, datum := range storeTests {
		store, err := client.Store("test",&datum.data)
		if err != nil {
			if datum.pass != false {
				t.Errorf("Unexpected failure observed : %v",err)
			}
		} else if datum.pass == false {
			// something got through
			t.Errorf("Expected an error but didn't observe one")
		} else if store != datum.data.Value {
			t.Errorf("Data returned from the store request did not match that sent. Sent [%v] and received [%v]",datum.data.Value,store)
		}
	}
}

func TestFetchData(t *testing.T) {
	for _, datum := range fetchTests {
		fetch, err := client.Fetch("test",datum.data.Key)
		if err != nil {
			// fetch failed
			if datum.pass != false {
				t.Errorf("Fetch was supposed to fail but didn't using [%v], err : %v",datum.data.Key,err)
			}
		} else if fetch == "" {
			t.Errorf("Fetch returned a value of empty. Value [%v]",fetch)
		}
	}
}

func TestDeleteData(t *testing.T) {
	for _, datum := range deleteTests {
		err := client.Delete("test",datum.data.Key)
		if err != nil {
			if datum.pass != false {
				t.Errorf("Delete was supposed to fail but didnt on key [%v], err : %v",datum.data.Key,err)
			}
		}
	}
}

/*
	Benchmarks for Riak Client interface
	Requires an active Riak installation like Testing but preferably with at least 5 nodes
*/

// bench POSTS as no items in bucket bench
func BenchmarkStore(b *testing.B) {
	for i:=0;i<b.N;i++ {
		d := Data{fmt.Sprintf("%d",i),"benched"}
		result, err := client.Store("bench",&d)
		if err != nil || result == "" {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

// bench GET as items are now in the DB
func BenchmarkFetch(b *testing.B) {
	for i:=0;i<b.N;i++ {
		_, err := client.Fetch("bench",fmt.Sprintf("%d",i))
		if err != nil {
			b.FailNow()
		}
	}
}

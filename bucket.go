package rgo

type Bucket struct {
	Name   string
	client Client
}

func (b *Bucket) Object() *Object {
	return &Object{Bucket: b.Name, client: b.client}
}

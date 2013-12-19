package rgo

type Counter struct {
	bucket string
	key    string
	conn   Connection
	Count  int64
}

func (c *Counter) Add(i int64) {
	c.Count = c.Count + i
	c.conn.UpdateCounter(c.bucket, c.key, c.Count)
}

func (c *Counter) Inc() {
	c.Add(1)
}

func (c *Counter) Dec() {
	c.Add(-1)
}

package rgo

import (
//"fmt"
//"strconv"
)

type BucketProperties struct {
	NVal          int64 `json:"n_val"`
	AllowMult     bool  `json:"allow_mult"`
	LastWriteWins bool  `json:"last_write_wins"`
	//PreCommit     interface{}
	//HasPrecommit  bool
	//PostCommit    interface{}
	//HasPostCommit bool
	// r             Quorum
	// w             Quorum
	// dw            Quorum
	// rw            Quorum
	//backend       string
}

/*
type Quorum string

func (q *Quorum) Validate(nval int64) bool {
	if q.String() == "all" || q.String() == "one" || q.String() == "quorum" {
		return true
	} else {
		// check if it is a number
		val, err := strconv.ParseInt(q.String(), 10, 64)
		if err != nil {
			return false
		} else {
			if val > 0 && val <= nval {
				return true
			} else {
				return false
			}
		}
	}
}

func (q *Quorum) String() string {
	return fmt.Sprintf("%v", q)
}
*/

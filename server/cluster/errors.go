package cluster

import "errors"

var (
	ErrIDRemoved     = errors.New("cluster: ID removed")
	ErrIDExists      = errors.New("cluster: ID exists")
	ErrIDNotExists   = errors.New("cluster: ID not exists")
	ErrNotLearner    = errors.New("cluster: not learner")
	ErrAddressClash  = errors.New("cluster: address clash")
	ErrNoLocalID     = errors.New("cluster: local member ID not set")
	ErrAlreayApplied = errors.New("cluster: already applied")
)

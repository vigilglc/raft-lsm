package server

import "errors"

var (
	ErrStopped        = errors.New("raft-lsm: server stopped")
	ErrRemoveSelf     = errors.New("raft-lsm: node should remove itself")
	ErrTimeout        = errors.New("raft-lsm: request timed out")
	ErrLeaderChanged  = errors.New("raft-lsm: raft leader changed")
	ErrInternalServer = errors.New("raft-lsm: internal server error")
	ErrBadRequest     = errors.New("raft-lsm: bad request")
	ErrInvalidArgs    = errors.New("raft-lsm: invalid arguments")
)

package server

import "errors"

var (
	ErrStopped        = errors.New("raft-lsm: server stopped")
	ErrTimeout        = errors.New("raft-lsm: request timed out")
	ErrLeaderChanged  = errors.New("raft-lsm: raft leader changed")
	ErrInternalServer = errors.New("raft-lsm: internal server error")
	ErrBadRequest     = errors.New("raft-lsm: bad request")
)

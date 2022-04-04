package app

import "errors"

var (
	ErrUnknownCommand      = errors.New("client: unknown command")
	ErrPutArgumentsInvalid = errors.New("client: invalid Put args")
	ErrClientStateInvalid  = errors.New("client: invalid client state, you cannot execute this cmd now")
)

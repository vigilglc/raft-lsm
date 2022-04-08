package app

import "errors"

var (
	ErrUnknownCommand            = errors.New("client: unknown command")
	ErrPutArgumentsInvalid       = errors.New("client: invalid Put args")
	ErrAddMemberArgumentsInvalid = errors.New("client: invalid AddMember args")
	ErrClientStateInvalid        = errors.New("client: invalid client state, you cannot execute this cmd now")
)

package store

import (
	"errors"
	"fmt"
)

func IsVersionNotFound(err error) bool {
	target := versionNotFoundError{}
	return errors.As(err, &target)
}

func IsVersionAlreadyExists(err error) bool {
	_, ok := err.(versionAlreadyExistsError)
	return ok
}

func NewVersionNotFoundError(msg string) error {
	return versionNotFoundError{msg: msg}
}

func NewVersionNotFoundErrorWithCause(msg string, cause error) error {
	return versionNotFoundError{msg: msg, cause: cause}
}

type versionNotFoundError struct {
	msg   string
	cause error
}

func (e versionNotFoundError) Error() string {
	if e.cause == nil {
		return e.msg
	}
	return fmt.Sprintf("%s: %s", e.msg, e.cause)
}

type versionAlreadyExistsError struct {
	msg string
}

func (v versionAlreadyExistsError) Error() string {
	return v.msg
}

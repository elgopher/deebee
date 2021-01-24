package deebee

type deebeeError struct {
	message string
}

func newClientError(message string) *deebeeError {
	return &deebeeError{
		message: message,
	}
}

func (e *deebeeError) IsClientError() bool {
	return true
}

func (e *deebeeError) Error() string {
	return e.message
}

func IsClientError(err error) bool {
	if err == nil {
		return false
	}
	type clientError interface {
		IsClientError() bool
	}
	e, ok := err.(clientError)
	if !ok {
		return false
	}
	return e.IsClientError()
}

type dataNotFoundError struct{}

func (e *dataNotFoundError) Error() string {
	return "data not found"
}

func IsDataNotFound(err error) bool {
	_, ok := err.(*dataNotFoundError)
	return ok
}

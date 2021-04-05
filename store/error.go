package store

func IsVersionNotFound(err error) bool {
	_, ok := err.(versionNotFoundError)
	return ok
}

func IsVersionAlreadyExists(err error) bool {
	_, ok := err.(versionAlreadyExistsError)
	return ok
}

type versionNotFoundError struct {
	msg string
}

func (v versionNotFoundError) Error() string {
	return v.msg
}

type versionAlreadyExistsError struct {
	msg string
}

func (v versionAlreadyExistsError) Error() string {
	return v.msg
}

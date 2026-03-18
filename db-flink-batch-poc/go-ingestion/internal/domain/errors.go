package domain

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound      = errors.New("resource not found")
	ErrAlreadyExists = errors.New("resource already exists")
	ErrInvalidInput  = errors.New("invalid input")
	ErrParsingFailed = errors.New("file parsing failed")
	ErrStorageFailed = errors.New("storage operation failed")
)

// FileProcessingError wraps errors with file context.
type FileProcessingError struct {
	Filename string
	Line     int
	Err      error
}

func (e *FileProcessingError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("file %s, line %d: %v", e.Filename, e.Line, e.Err)
	}
	return fmt.Sprintf("file %s: %v", e.Filename, e.Err)
}

func (e *FileProcessingError) Unwrap() error {
	return e.Err
}

package proxyerror

import (
	"fmt"
	"strings"
	"time"
)

// ProxyError is error(s) occuring during communication with Kafka
// Use Code for error reference
// Try to use predefined errors (such as ConnectionError)
// Add new if required
type ProxyError struct {
	Code    int
	Time    time.Time
	Message string
}

func (e *ProxyError) Error() string {
	const layout = "02/Jan/2006-15:04:05"
	return fmt.Sprintf("%s: Error %d: %s", e.Time.Format(layout), e.Code, e.Message)
}

func createError(code int, messages []string) *ProxyError {
	proxyError := &ProxyError{
		Code:    code,
		Time:    time.Now(),
		Message: strings.Join(messages, ": "),
	}

	return proxyError
}

// BrokersNotSetError represents error when brokers are not specified
func BrokersNotSetError(messages ...string) *ProxyError {
	proxyError := createError(1, messages)
	return proxyError
}

// ConnectionError represents error while connecting to some resource
func ConnectionError(messages ...string) *ProxyError {
	proxyError := createError(2, messages)
	return proxyError
}

// ResourceCloseError represents error while closing some resource
func ResourceCloseError(messages ...string) *ProxyError {
	proxyError := createError(3, messages)
	return proxyError
}

// ResourceClosedError represents error while trying to access a closed resource
func ResourceClosedError(messages ...string) *ProxyError {
	proxyError := createError(4, messages)
	return proxyError
}

// ResourceWriteError represents error while writing to a resource
func ResourceWriteError(messages ...string) *ProxyError {
	proxyError := createError(5, messages)
	return proxyError
}

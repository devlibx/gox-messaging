package kafka

// GlobalRateLimiterFunc is a func to apply global limit
type GlobalRateLimiterFunc func(producerOrConsumer bool, name string) error

// GlobalRateLimiter is a global rate limiter
var GlobalRateLimiter = func(producerOrConsumer bool, name string) error { return nil }

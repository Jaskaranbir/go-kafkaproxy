package producer

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Jaskaranbir/go-kafkaproxy/pkg/proxyerror"

	"github.com/Shopify/sarama"
)

// Adapter is the Kafka-Producer interface
type Adapter interface {
	AsyncClose()
	Close() error
	Input() chan<- *sarama.ProducerMessage
	Successes() <-chan *sarama.ProducerMessage
	Errors() <-chan *sarama.ProducerError
}

// Config wraps configuration for producer
type Config struct {
	ErrHandler   func(*sarama.ProducerError)
	KafkaBrokers []string
	// Allow overwriting default sarama-config
	SaramaConfig *sarama.Config
}

// Producer wraps sarama's producer
type Producer struct {
	producer         Adapter
	isClosed         bool
	isLoggingEnabled bool
}

// New returns a configured sarama Kafka-AsyncProducer instance
func New(initConfig *Config) (*Producer, error) {
	if initConfig.KafkaBrokers == nil || len(initConfig.KafkaBrokers) == 0 {
		return nil, proxyerror.BrokersNotSetError("No Kafka Brokers set.")
	}

	var config *sarama.Config
	if initConfig.SaramaConfig != nil {
		config = initConfig.SaramaConfig
	} else {
		config = sarama.NewConfig()
		config.Producer.Return.Errors = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Compression = sarama.CompressionNone
	}

	producer, err := sarama.NewAsyncProducer(initConfig.KafkaBrokers, config)
	if err != nil {
		return nil, proxyerror.ConnectionError(err.Error())
	}

	proxyProducer := Producer{
		producer:         producer,
		isClosed:         false,
		isLoggingEnabled: false,
	}
	proxyProducer.handleKeyInterrupt()
	proxyProducer.handleErrors(initConfig.ErrHandler)
	return &proxyProducer, nil
}

// EnableLogging logs events to console
func (p *Producer) EnableLogging() {
	p.isLoggingEnabled = true
}

// CreateKeyMessage creates producer-formatted message with key
func (p *Producer) CreateKeyMessage(topic string, key string, value string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	return msg
}

// CreateMessage creates keyless producer-formatted message
func (p *Producer) CreateMessage(topic string, value string) *sarama.ProducerMessage {
	return p.CreateKeyMessage(topic, "", value)
}

// IsClosed returns a bool specifying if Kafka producer is closed
func (p *Producer) IsClosed() bool {
	return p.isClosed
}

// Get returns the original Sarama Kafka producer
func (p *Producer) Get() *Adapter {
	return &p.producer
}

// Input takes Kafka messages to be produced
func (p *Producer) Input() (chan<- *sarama.ProducerMessage, error) {
	if !p.IsClosed() {
		return p.producer.Input(), nil
	}

	err := proxyerror.ResourceClosedError("Producer already closed.")
	return nil, err
}

func (p *Producer) handleKeyInterrupt() {
	// Capture the Ctrl+C signal (interrupt or kill)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// Elegant exit
	go func() {
		<-sigChan
		// We always log here, special situation
		log.Println("Keyboard-Interrupt signal received.")
		closeError := <-p.Close()
		log.Fatalln(closeError.Error())
	}()
}

func (p *Producer) handleErrors(errHandler func(*sarama.ProducerError)) {
	producer := *p.Get()
	go func() {
		for err := range producer.Errors() {
			if p.isLoggingEnabled {
				log.Fatalln("Failed to produce message", err)
			}
			errHandler(err)
		}
	}()
}

// Close attempts to close the producer,
// and returns any occurring errors over channel
func (p *Producer) Close() chan error {
	// The error-channel only contains errors occurred
	// while closing producer. Ignore if producer already
	// closed.
	if p.IsClosed() {
		return nil
	}

	closeErrorChan := make(chan error, 1)
	go func() {
		producer := *p.Get()
		err := producer.Close()
		if err != nil {
			if p.isLoggingEnabled {
				log.Fatal("Error closing async producer.", err)
			}
			closeErrorChan <- err
		}
		if p.isLoggingEnabled {
			log.Println("Async Producer closed.")
		}
		p.isClosed = true
	}()

	return closeErrorChan
}

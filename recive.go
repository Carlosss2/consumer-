package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Notification struct {
	Message string `json:"message"`

}

type Server struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue amqp.Queue
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func NewServer() (*Server, error) {
	conn, err := amqp.Dial("amqp://carlos:carlos@100.27.181.40:5672/")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}

	queue, err := ch.QueueDeclare(
		"platillos", 
		true,    
		false,   
		false,   
		false,   
		nil,     
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %v", err)
	}

	return &Server{conn: conn, ch: ch, queue: queue}, nil
}

func (s *Server) StartConsumer(apiURL string) {
	msgs, err := s.ch.Consume(
		s.queue.Name,
		"",
		false, 
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	for msg := range msgs {
		log.Printf("Received a message: %s", msg.Body)

		err := sendNotificationToAPI(apiURL, string(msg.Body))
		if err != nil {
			log.Printf("Error sending notification to API: %v", err)
			msg.Nack(false, true)
			msg.Reject(false)
			continue
		}

		msg.Ack(false)
	}
}

func sendNotificationToAPI(apiURL, message string) error {
	notification := Notification{Message: message}
	jsonData, err := json.Marshal(notification)
	if err != nil {
		return err
	}

	resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	log.Printf("API Response: %s", body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("API returned non-200/201 status: %d", resp.StatusCode)
	}

	return nil
}


func main() {
	server, err := NewServer()
	if err != nil {
		log.Fatal(err)
	}

	apiURL := "http://localhost:8082/payments" 
	log.Println("Listening for messages...")

	server.StartConsumer(apiURL)
}
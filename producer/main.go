package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

func producerHandler(kafkaWriter *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: body,
		}
		err = kafkaWriter.WriteMessages(req.Context(), msg)

		if err != nil {
			wrt.Write([]byte(err.Error()))
			log.Fatalln(err)
		}
	})
}

func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func InitializeViper(configPath string) {
	viper.SetEnvPrefix("config")
	viper.AutomaticEnv()

	replacer := strings.NewReplacer(".", "_")
	viper.EnvKeyReplacer(replacer)
	viper.SetConfigType("json")
	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
}

func main() {
	// get kafka writer using environment variables.
	InitializeViper("config.json")

	kafkaURL := viper.GetString("kafka.url")
	topic := viper.GetString("kafka.topic")

	kafkaWriter := getKafkaWriter(kafkaURL, topic)

	defer kafkaWriter.Close()

	// Add handle func for producer.
	http.HandleFunc("/", producerHandler(kafkaWriter))

	// Run the web server.
	fmt.Println("start producer-api ... !!")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

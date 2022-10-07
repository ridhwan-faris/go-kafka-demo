package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
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
	// initialize env
	InitializeViper("config.json")

	// get kafka reader using environment variables from json.
	kafkaURL := viper.GetString("kafka.url")
	topic := viper.GetString("kafka.topic")
	groupID := viper.GetString("kafka.group_id")
	reader := getKafkaReader(kafkaURL, topic, groupID)

	log.Println(kafkaURL)
	log.Println(topic)
	log.Println(groupID)

	defer reader.Close()

	fmt.Println("start consuming ... !!")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		log.Println("message: ", string(msg.Value))
	}
}

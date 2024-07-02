package main

import (
	"context"
	"fmt"
	kafkarita "go-mongo/kafka"
	cmd "go-mongo/ritaCmd"
	"log"
	"sync"
	"time"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
    uri       = "mongodb://localhost:27017"
	duration  = 10* time.Second
	primaryDB = "mydatabase"
	blDbname  = "rita-bl"	
	config = "config.yaml"
	zeekPath= "/opt/zeek/logs/current/"
	totalchunk = "24"
	brokerAddress   = "kafka.cosgrid.com:9092"
	
)


func main() {
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())

	// Ping the MongoDB server to ensure connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")

	ticker := time.NewTicker(duration)

	func() {
		for range ticker.C {
			cmd.RunCommands(primaryDB, config, zeekPath, totalchunk)
			var wg sync.WaitGroup
			wg.Add(10) // We have two goroutines to wait for

			go func() {
				defer wg.Done()
				getData(client, primaryDB, "beacon", "beacon_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, primaryDB, "useragent", "user_agent_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, primaryDB, "beaconSNI", "beacon_SNI_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, primaryDB, "explodedDns", "beacon_DNS_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, primaryDB, "uconn", "uconn_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, primaryDB, "SNIconn", "SNIconn_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, "rita-bl", "ip", "bl_ip_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, "rita-bl", "lists", "bl_lists_rita")
			}()

            go func() {
				defer wg.Done()
				getData(client, primaryDB, "host", "host_rita")
			}()

            go func() {
				defer wg.Done()
				getData(client, primaryDB, "hostnames", "bl_lists_rita")
			}()

            // go func(){
            //     defer wg.Done()
                
            // }

			wg.Wait()

		}
	}()

	time.Sleep(5 * time.Second)
	ticker.Stop()

}

func getData(client *mongo.Client, db string, colName string, topic string) {
	database := client.Database(db)
	collection := database.Collection(colName)
	cursor, err := collection.Find(context.TODO(), bson.D{})
	// var c1 mongo.Cursor
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()) {
		var elem bson.Raw
		err := cursor.Decode(&elem)
		// fmt.Println(cursor1)

		if err != nil {
			log.Fatal(err)
		}
		jsondata, _ := bson.MarshalExtJSON(elem, false, false)
		kafkarita.PubToKafka(topic, jsondata, brokerAddress)
	}
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}
}

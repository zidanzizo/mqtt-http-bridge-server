package main

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/gin-gonic/gin"
	mqttServer "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/events"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/urfave/cli"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

// Configuration Structure to Hold the Config Values
type Configuration struct {
	MqttServerURL string
	MqttUsername  string
	MqttPassword  string
	BasicAuthUser string
	BasicAuthPass string
	GinPort       string
}

// Container Structure to Hold both Config and MQTTClient
type Container struct {
	Config     Configuration
	MqttClient mqtt.Client
}

// PublishDTO is the sample data transfer object for publish http route
type PublishDTO struct {
	Topic   string `binding:"required" json:"topic"`
	Message string `binding:"required" json:"message"`
}

type SubsDTO struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

func main() {

	app := cli.NewApp()
	app.Name = "HTTP-MQTT Bridge"
	app.Usage = "Makes a bridge from HTTP to MQTT"
	app.Action = func(c *cli.Context) error {
		cli.ShowAppHelp(c)
		return nil
	}

	app.Commands = []cli.Command{
		{
			Name:    "start",
			Aliases: []string{"s"},
			Usage:   "starts the server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "mqtt-host, mh",
					Value:  "tcp://localhost:1883",
					Usage:  "MQTT Host Address",
					EnvVar: "MQTT_HOST",
				},
				cli.StringFlag{
					Name:   "mqtt-user,mu",
					Value:  "user",
					Usage:  "MQTT Host Username",
					EnvVar: "MQTT_USER",
				},
				cli.StringFlag{
					Name:   "mqtt-pass,mp",
					Value:  "password",
					Usage:  "MQTT Host Password",
					EnvVar: "MQTT_PASS",
				},
				cli.StringFlag{
					Name:   "username,u",
					Value:  "try",
					Usage:  "Basic Authentication Username",
					EnvVar: "AUTH_USERNAME",
				},
				cli.StringFlag{
					Name:   "password,p",
					Value:  "catch",
					Usage:  "Basic Authentication Password",
					EnvVar: "AUTH_PASSWORD",
				},
				cli.StringFlag{
					Name:   "port",
					Value:  "8090",
					Usage:  "GIN Router Port",
					EnvVar: "PORT",
				},
			},
			Action: func(c *cli.Context) error {
				// Fills the Configuration
				container := InitializeContainer(c)
				// Setup Gin Server + MQTT
				// Setup(container)

				/**
				INITIALISASI VARIABLE
				*/
				var subFromMQTT = new(SubsDTO)

				/**
				MQTT SERVER
				*/
				// Create the new MQTT Server.
				server := mqttServer.NewServer(nil)

				// Create a TCP listener on a standard port.
				tcp := listeners.NewTCP("t1", ":1883")

				// Add the listener to the server with default options (nil).
				err := server.AddListener(tcp, nil)
				if err != nil {
					log.Fatal(err)
				}

				// Start the broker. Serve() is blocking - see examples folder
				// for usage ideas.
				go func() {
					err = server.Serve()
					if err != nil {
						log.Fatal(err)
					}
				}()

				// Add OnMessage Event Hook
				go func() {
					server.Events.OnMessage = func(cl events.Client, pk events.Packet) (pkx events.Packet, err error) {
						// Log time MQTT packet arrived in broker
						fmt.Println()
						fmt.Printf("** OnMessage received message from client %s: %s **\n", cl.ID, string(pk.Payload))
						fmt.Printf("MQTT PACKET RECEIVE TIMESTAMP : %d\n", time.Now().UnixMicro())
						fmt.Println(strings.Repeat("=", 30))
						fmt.Println()

						// parsing topic and payload from mqtt to struct HTTP
						subFromMQTT.Topic = pk.TopicName
						subFromMQTT.Message = string(pk.Payload)

						// Re-publish to subscribe packet from subscribe with same topic
						err = server.Publish(pk.TopicName, []byte(subFromMQTT.Message), false)
						if err != nil {
							log.Fatal(err)
						}

						// Log time when packet HTTP send to client
						fmt.Println()
						fmt.Printf("MQTT PACKET SEND TIMESTAMP : %d \n", time.Now().UnixMicro())
						fmt.Println(strings.Repeat("=", 30))
						fmt.Println()
						return pkx, nil
					}
				}()

				/**
				MQTT CLIENT
				*/
				opts := mqtt.NewClientOptions().AddBroker(container.Config.MqttServerURL)
				if container.Config.MqttUsername != "" {
					opts.Username = container.Config.MqttUsername
					opts.Password = container.Config.MqttPassword
				}
				client := mqtt.NewClient(opts)
				if token := client.Connect(); token.Wait() && token.Error() != nil {
					log.Fatal("MQTT Error: ", token.Error())
				}
				container.MqttClient = client

				/**
				SETUP GIN
				*/
				r := gin.Default()

				// Ping Test
				r.GET("/ping", func(c *gin.Context) {
					c.String(http.StatusOK, "pong")
				})

				// Ping Test
				r.GET("/", func(c *gin.Context) {
					c.String(http.StatusOK, "i am ready !")
				})

				// Endpoint to get new value using HTPP
				subcribe := r.Group("/subscribe")
				subcribe.GET("/sensor/suhu", func(c *gin.Context) {
					c.JSON(http.StatusOK, subFromMQTT)
				})

				// Conf to set authorization HTTP
				authorized := r.Group("/", gin.BasicAuth(gin.Accounts{
					container.Config.BasicAuthUser: container.Config.BasicAuthPass, // user:foo password:bar
				}))

				// Post To Topic
				authorized.POST("publish", func(c *gin.Context) {

					// Log time when packet HTTP arrived in broker
					fmt.Println()
					fmt.Printf("HTTP PACKET RECEIVE TIMESTAMP : %d \n", time.Now().UnixMicro())
					fmt.Println(strings.Repeat("=", 30))
					fmt.Println()

					var publishDTO PublishDTO
					// Validate Payloadd
					if err := c.ShouldBindJSON(&publishDTO); err != nil {
						c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": err.Error()})
						return
					}

					// Set new value for HTTP GET from HTTP POST
					subFromMQTT.Topic = publishDTO.Topic
					subFromMQTT.Message = publishDTO.Message

					// Go For MQTT Publish
					client := container.MqttClient
					if token := client.Publish(publishDTO.Topic, 0, false, publishDTO.Message); token.Error() != nil {
						// Return Error
						log.Println("Error:", token.Error())
						c.JSON(http.StatusFailedDependency, gin.H{"status": "error", "error": token.Error()})
						return
					}
					c.JSON(http.StatusCreated, gin.H{"status": "ok"})

					// Log time when packet HTTP send to client
					fmt.Println()
					fmt.Printf("HTTP PACKET SEND TIMESTAMP : %d \n", time.Now().UnixMicro())
					fmt.Println(strings.Repeat("=", 30))
					fmt.Println()
				})

				r.Run(":" + container.Config.GinPort)

				return nil
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}

// InitializeContainer Create the container with provided cli config values
func InitializeContainer(c *cli.Context) *Container {
	container := new(Container)
	container.Config = Configuration{
		MqttServerURL: c.String("mqtt-host"),
		MqttUsername:  c.String("mqtt-user"),
		MqttPassword:  c.String("mqtt-pass"),
		BasicAuthUser: c.String("username"),
		BasicAuthPass: c.String("password"),
		GinPort:       c.String("port"),
	}
	return container
}

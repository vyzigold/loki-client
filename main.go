package main

import (
    "fmt"
    "net/http"
    "errors"
    "strings"
    "time"
    "strconv"
    "io/ioutil"
    "encoding/json"
)

type Label struct {
    key   string
    value string
}

type LokiClient struct {
    url       string
    endpoints Endpoints
}

type Message struct {
    message string
    time    string
}

type Endpoints struct {
    push  string
    query string
    ready string
}

func (client *LokiClient) initEndpoints() {
    client.endpoints.push = "/loki/api/v1/push"
    client.endpoints.query = "/loki/api/v1/query"
    client.endpoints.ready = "/ready"
}

// Checks if the loki is ready
func (client *LokiClient) IsReady() bool {
    response, err := http.Get(client.url + client.endpoints.ready)
    return err == nil && response.StatusCode == 200
}

// Creates a new loki client
func CreateClient(url string) (*LokiClient, error) {
    var client LokiClient
    client.url = url
    client.initEndpoints()
    if !client.IsReady() {
        return &client, errors.New("The server on: " + url + "isn't ready.")
    }
    return &client, nil
}

// The template for the message sent to Loki is:
//{
//  "streams": [
//    {
//      "stream": {
//        "label": "value"
//      },
//      "values": [
//          [ "<unix epoch in nanoseconds>", "<log line>" ],
//          [ "<unix epoch in nanoseconds>", "<log line>" ]
//      ]
//    }
//  ]
//}

// Sends the messages to loki with the labels assigned to them
func (client *LokiClient) Send(labels []Label, messages []Message) error {
    var body strings.Builder
    body.WriteString("{\"streams\": [{\"stream\": {")
    for i := range labels {
        body.WriteString("\"")
        body.WriteString(labels[i].key)
        body.WriteString("\": \"")
        body.WriteString(labels[i].value)
        if i == len(labels) - 1 {
            // don't write ',' to the last label
            body.WriteString("\"\n")
        } else {
            body.WriteString("\",\n")
        }
    }
    body.WriteString("},\"values\": [")

    for i := range messages {
        body.WriteString("[ \"")
        body.WriteString(messages[i].time)
        body.WriteString("\", \"")
        body.WriteString(messages[i].message)
        if i == len(messages) - 1 {
            // don't write ',' to the last message
            body.WriteString("\" ]\n")
        } else {
            body.WriteString("\" ],\n")
        }
    }
    body.WriteString("]}]}")

    response, err := http.Post(client.url + client.endpoints.push, "application/json", strings.NewReader(body.String()))
    if response.StatusCode != 204 {
        return errors.New(response.Status)
    } else {
        return err
    }
}

type returnedJSON struct {
    Status interface{}
    Data struct {
        ResultType string
        Result []struct {
            Stream interface{}
            Values [][]string
        }
        Stats interface{}
    }
}

// Queries the server. The queryString is expected to be in the
// LogQL format described here:
// https://github.com/grafana/loki/blob/master/docs/logql.md
func (client *LokiClient) Query(queryString string) ([]Message, error) {
    response, err := http.Get(client.url + client.endpoints.query + "?query=" + queryString)

    body, err := ioutil.ReadAll(response.Body)
    if err != nil {
        return []Message{}, err
    }
    var answer returnedJSON
    json.Unmarshal(body, &answer)
    var values []Message
    for i := range answer.Data.Result {
        for j := range answer.Data.Result[i].Values {
            msg := Message{
                time: answer.Data.Result[i].Values[j][0],
                message: answer.Data.Result[i].Values[j][1],
            }
            values = append(values, msg)
        }
    }
    return values, nil
}

func main () {
    client, err := CreateClient("http://localhost:3100")
    if err == nil {
        fmt.Println("Client successfuly created")
        fmt.Println(client)
    }
    label1 := Label {
        key: "testkey2",
        value: "testvalue5",
    }
    message1 := Message {
        time: strconv.FormatInt(time.Now().UnixNano(), 10),
        message: "test-mesageotnauh",
    }
    labels := []Label{label1}
    messages := []Message{message1}
    err = client.Send(labels, messages)
    response, err := client.Query("{testkey2=~\"test.*\"}")
    fmt.Println(response)
    if err == nil {
        fmt.Println("Message successfuly sent")
    }
}

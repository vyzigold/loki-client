package loki

import (
    "fmt"
    "net/http"
    "errors"
    "strings"
    "io/ioutil"
    "encoding/json"
)

type Label struct {
    Key   string
    Value string
}

type LokiClient struct {
    url       string
    endpoints Endpoints
}

type Message struct {
    Message string
    Time    string
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
        body.WriteString(labels[i].Key)
        body.WriteString("\": \"")
        body.WriteString(labels[i].Value)
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
        body.WriteString(messages[i].Time)
        body.WriteString("\", ")
        escapedMessage, err := json.Marshal(messages[i].Message)
        if err != nil {
            return err
        }
        body.Write(escapedMessage)
        if i == len(messages) - 1 {
            // don't write ',' to the last message
            body.WriteString(" ]\n")
        } else {
            body.WriteString(" ],\n")
        }
    }
    body.WriteString("]}]}")

    fmt.Println(body.String())
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
                Time: answer.Data.Result[i].Values[j][0],
                Message: answer.Data.Result[i].Values[j][1],
            }
            values = append(values, msg)
        }
    }
    return values, nil
}


package loki

import (
    "net/http"
    "errors"
    "io/ioutil"
    "encoding/json"
    "bytes"
)

type jsonValue [2]string

type jsonStream struct {
    Stream map[string]string `json:"stream"`
    Values []jsonValue `json:"values"`
}

type jsonMessage struct {
    Streams []jsonStream `json:"streams"`
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
func (client *LokiClient) Send(labels map[string]string, messages []Message) error {
    // build the structure of the json
    var vals []jsonValue
    for i := range messages {
        var val jsonValue
        val[0] = messages[i].Time
        val[1] = messages[i].Message
        vals = append(vals, val)
    }
    var stream []jsonStream
    stream = append(stream, jsonStream {
        Stream: labels,
        Values: vals,
    })
    msg := jsonMessage {
        Streams: stream,
    }

    // encode it and send to loki

    str, err := json.Marshal(msg)

    response, err := http.Post(client.url + client.endpoints.push, "application/json", bytes.NewReader(str))
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


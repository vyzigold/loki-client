package main

import (
    "time"
    "strconv"
    "fmt"
    "github.com/vyzigold/loki-client/pkg/loki"
)

func sendSomething(client *loki.LokiClient) {
    labels := make(map[string]string)
    labels["labelkey"]="labelvalue"
    message1 := loki.Message {
        Time: strconv.FormatInt(time.Now().UnixNano(), 10),
        Message: "{\"simple\": [\"json\", \"string\"]}",
    }
    message2 := loki.Message {
        Time: strconv.FormatInt(time.Now().UnixNano(), 10),
        Message: "not json message",
    }
    messages := []loki.Message{message1, message2}
    client.AddStream(labels, messages)
}

func main () {
    client, err := loki.CreateClient("http://localhost:3100", 4, 1000000000)
    if err == nil {
        fmt.Println("Client successfuly created")
    }
    for i := 10; i > 0; i-- {
        sendSomething(client)
        time.Sleep(100000000)
    }
    response, err := client.Query("{labelkey=~\"lab.*\"}")
    fmt.Println("\nthe response of the query currently is:")
    fmt.Println(response)
    client.Shutdown()
}

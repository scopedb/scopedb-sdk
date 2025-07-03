package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"log"
	"os"
	"time"
)

type Event struct {
	Id   string `json:"id"`
	Type string `json:"type"`
	Repo struct {
		Id   int    `json:"id"`
		Name string `json:"name"`
	} `json:"repo"`
	Actor struct {
		Id    int    `json:"id"`
		Login string `json:"login"`
	} `json:"actor"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

type NormalizedEvent struct {
	Id         string    `json:"id"`
	Type       string    `json:"type"`
	RepoId     int       `json:"repo_id"`
	RepoName   string    `json:"repo_name"`
	ActorId    int       `json:"actor_id"`
	ActorLogin string    `json:"actor_login"`
	CreatedAt  time.Time `json:"created_at"`
	Payload    string    `json:"payload"`
}

func main() {
	ctx := context.Background()

	c := scopedb.NewClient(&scopedb.Config{
		Endpoint: "http://127.0.0.1:6543",
	})

	tbl := c.Table("gharchive")
	_, err := c.Statement(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			created_at timestamp,
			id string,
			type string,
			repo_id int,
			repo_name string,
			actor_id int,
			actor_login string,
			payload object,
		)
	`, tbl.Identifier())).Execute(ctx)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}

	cable := c.RawDataBatchCable(fmt.Sprintf(`
		SELECT
			$0["created_at"]::timestamp,
			$0["id"]::string,
			$0["type"]::string,
			$0["repo_id"]::int,
			$0["repo_name"]::string,
			$0["actor_id"]::int,
			$0["actor_login"]::string,
			parse_json($0["payload"]::string)::object
		INSERT INTO %s (
			created_at, id, type, repo_id, repo_name, actor_id, actor_login, payload
		)
	`, tbl.Identifier()))
	cable.Start(ctx)
	defer cable.Close()

	file, err := os.Open("examples/2025-07-02-18.json.gz")
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		log.Fatalf("Error creating gzip reader: %v", err)
	}
	defer gzReader.Close()

	var data Event
	events := make([]<-chan error, 0)
	decoder := json.NewDecoder(gzReader)
	lines := 0
	for {
		if err := decoder.Decode(&data); err != nil {
			log.Printf("Error decoding JSON: %v", err)
			break
		}

		lines += 1
		normalized := NormalizedEvent{
			Id:         data.Id,
			Type:       data.Type,
			RepoId:     data.Repo.Id,
			RepoName:   data.Repo.Name,
			ActorId:    data.Actor.Id,
			ActorLogin: data.Actor.Login,
			CreatedAt:  data.CreatedAt,
			Payload:    string(data.Payload),
		}

		//log.Printf("Normalized event: %+v\n", normalized)

		events = append(events, cable.Send(normalized))
	}

	log.Printf("Total lines processed: %d\n", lines)
	for _, event := range events {
		err = <-event
		if err != nil {
			log.Fatalf("Error sending last event: %v", err)
		}
	}
}

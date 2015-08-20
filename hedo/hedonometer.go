package main

import (
	"encoding/json"
	"log"
	"net/http"
)

// http://hedonometer.org/api/v1/words/?format=json&happs__gt=5&rank__lt=1500&stdDev__lt=2
const url = "http://hedonometer.org/api/v1/words/?format=json&happs__gt=5&rank__lt=1500&stdDev__lt=2"

type meta struct {
	Limit       int
	Next        string
	Offset      int
	Previous    string
	Total_count int
}
type object struct {
	GooogleBooksRank int
	Happs            float32
	LyricsRank       int
	NewYorkTimesRank int
	Rank             int
	StdDev           float32
	Text             string
	TwitterRank      int
	Word             string
}

type hedonometer struct {
	Meta    meta
	Objects []object
}

func GetHedonometer() hedonometer {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatalln(err)
	}
	req.Header.Add("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	v := hedonometer{}
	err = decoder.Decode(&v)
	if err != nil {
		log.Fatalln(err)
	}
	return v
}

//func main() {
//	hedo := GetHedonometer()
//	log.Println(hedo)
//}

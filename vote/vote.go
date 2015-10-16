package notification

import (
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/urlfetch"
	"net/http"
	"strconv"
	"time"
)

var redirectMap = make(map[string]string)

func init() {
	http.HandleFunc("/redirect", redirect)
	http.HandleFunc("/", handler)
	initRedirectMap()
}

func initRedirectMap() {
	redirectMap["a1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_136"
	redirectMap["a2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_181"
	redirectMap["b1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.gcb1cc01c5_0_5"
	redirectMap["b2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_10"
	redirectMap["c1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_166"
	redirectMap["c2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_20"
	redirectMap["d1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_171"
	redirectMap["d2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_30"
	redirectMap["e1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_40"
	redirectMap["e2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_176"
	redirectMap["f1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_186"
	redirectMap["f2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_50"
	redirectMap["g1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_191"
	redirectMap["g2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_60"
	redirectMap["h1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge357d4ecc_3_43"
	redirectMap["h2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge357d4ecc_3_43"
	redirectMap["i1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_212"
	redirectMap["i2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge4474d053_0_84"
	redirectMap["j1"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge357d4ecc_1_0"
	redirectMap["j2"] = "https://docs.google.com/presentation/d/1-qn9igzD9QUCo9QtkRdLMtT-p88OYc2R7hzVxNcdrqU/pub?start=true&delayms=60000&slide=id.ge357d4ecc_1_5"
}

func bodyUnmarshal(body []byte) map[string]interface{} {
	var f interface{}
	json.Unmarshal(body, &f)
	m := f.(map[string]interface{})
	return m
}

func BigQueryService(ctx *context.Context) (*bigquery.Service, error) {
	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(*ctx, bigquery.BigqueryScope),
			Base: &urlfetch.Transport{
				Context: *ctx,
			},
		},
	}

	service, err := bigquery.New(client)

	return service, err

}

func redirect(w http.ResponseWriter, r *http.Request) {
	bq := getBQ(r)
	c := appengine.NewContext(r)
	projectID := appengine.AppID(c)
	queryResponse, _ := queryVote(projectID, bq)

	rows := make(map[string]int)

	for _, row := range queryResponse.Rows {
		vote := row.F[0].V.(string)
		nb := row.F[1].V.(string)
		rows[vote], _ = strconv.Atoi(nb)
	}

	choice1 := r.URL.Query()["choice1"][0]
	choice2 := r.URL.Query()["choice2"][0]

	nbChoice1 := rows[choice1]
	nbChoice2 := rows[choice2]
	url := ""

	if nbChoice1 > nbChoice2 {
		url = redirectMap[choice1]
	} else {
		url = redirectMap[choice2]
	}

	// https://github.com/chischaschos/contributron-golang-api/blob/master/get_current_year_stats.go
	//	http.Redirect(w, r, "https://docs.google.com/presentation/d/1am-qXNiCxl2Xlr-0Wpa0f0IkqeQTdTwxKbxEsgzKBqw/edit#slide=id.ge20507c66_0_1", http.StatusFound)
	http.Redirect(w, r, url, http.StatusFound)
}

func queryVote(projectID string, bigQueryService *bigquery.Service) (*bigquery.QueryResponse, error) {

	query := `
SELECT
  message,
  COUNT(message) AS nb
FROM (
  SELECT
    REPLACE(REPLACE(LOWER(message), "bdxio", ""), " ", "") AS message
  FROM
    [smsgateway.smsmessages]
  WHERE
    LOWER(message) CONTAINS LOWER("bdxio"))
GROUP BY
  message`

	queryRequest := &bigquery.QueryRequest{
		Query: query,
		Kind:  "igquery#queryRequest",
	}

	jobsService := bigquery.NewJobsService(bigQueryService)
	jobsQueryCall := jobsService.Query(projectID, queryRequest)
	queryResponse, err := jobsQueryCall.Do()

	if err != nil {
		return nil, err
	}

	return queryResponse, nil
}

func getBQ(r *http.Request) *bigquery.Service {
	c := appengine.NewContext(r)
	bq, _ := BigQueryService(&c)
	return bq
}

func handler(w http.ResponseWriter, r *http.Request) {

	datasetID := "smsgateway"
	tableID := "smsmessages"
	c := appengine.NewContext(r)
	projectID := appengine.AppID(c)

	message := r.URL.Query()["message"][0]
	telnumber := r.URL.Query()["telnumber"][0]
	bq := getBQ(r)

	rows := make([]*bigquery.TableDataInsertAllRequestRows, 0)

	row := &bigquery.TableDataInsertAllRequestRows{
		Json: make(map[string]bigquery.JsonValue, 0),
	}
	row.Json["date"] = time.Now().Unix()
	row.Json["telnumber"] = telnumber
	row.Json["message"] = message

	rows = append(rows, row)

	req := &bigquery.TableDataInsertAllRequest{
		Rows: rows,
	}

	call := bq.Tabledata.InsertAll(projectID, datasetID, tableID, req)
	resp, err := call.Do()
	buff, _ := json.Marshal(resp)

	/*
		rows := make([]*bigquery.TableDataInsertAllRequestRows, 1)
		jsonRow := make(map[string]bigquery.JsonValue)
		jsonRow["date"] = bigquery.JsonValue(0)
		jsonRow["telnumber"] = bigquery.JsonValue("0101010101")
		jsonRow["message"] = bigquery.JsonValue("unmessage")
		rows[0] = new(bigquery.TableDataInsertAllRequestRows)
		rows[0].Json = jsonRow

		insertRequest := &bigquery.TableDataInsertAllRequest{Rows: rows}

		_, err := bq.Tabledata.InsertAll(projectID, datasetID, tableID, insertRequest).Do()
	*/

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	/*

		if body, err := ioutil.ReadAll(r.Body); err != nil {
			fmt.Fprintf(w, "Couldn't read request body: %s", err)
		} else {
			if len(body) > 0 {
				bodyMap := bodyUnmarshal(body)
				for k, v := range bodyMap {
					switch vv := v.(type) {
					case string:
						c.Infof(k, vv)
					default:
					}
				}
			}
		}
	*/
	fmt.Fprintln(w, "Hello, world toto", err, string(buff))

}

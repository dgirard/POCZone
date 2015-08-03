package main

import (
	"fmt"
	"google.golang.org/appengine"
	"google.golang.org/appengine/urlfetch"
	"io/ioutil"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func init() {
	http.HandleFunc("/", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	transport := &oauth2.Transport{
		Source: google.AppEngineTokenSource(c, "https://spreadsheets.google.com/feeds"),
		Base:   &urlfetch.Transport{Context: c},
	}

	client := &http.Client{Transport: transport}

	spreadsheetsID := "1QwybKPlmYFhFa5CSZqfwsmQDIPTfagMtvQ0C3CA5xwY"
	sheetID := "1560292823" // gid=sheetID

	resp, err := client.Get("https://docs.google.com/spreadsheets/d/" + spreadsheetsID + "/gviz/tq?gid=" + sheetID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer resp.Body.Close()
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%s\n", string(contents))

}

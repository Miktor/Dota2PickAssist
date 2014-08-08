// Parser
package parser

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const streamApi = "https://api.steampowered.com/IDOTA2Match_570/"

type Player struct {
	account_id  float64
	player_slot float64
	hero_id     float64
}

type Match struct {
	match_id      float64
	match_seq_num float64
	start_time    float64
	lobby_type    float64
	players       []Player
}

type GetMatchHistoryResult struct {
	result            float64
	status            float64
	statusDetail      float64
	num_results       float64
	total_results     float64
	results_remaining float64
	matches           []Match
}

func Start(apiKey string) {
	resp, err := http.Get(streamApi + "GetMatchHistory/v1/?key=" + apiKey)
	if err != nil {
		// handle error
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	//bodyString := string(bodyBytes)

	var result GetMatchHistoryResult
	err = json.Unmarshal(bodyBytes, &result)
	if err != nil || result.result != 1 {
		fmt.Println("error:", err)
	}

	//fmt.Println(bodyString)
}

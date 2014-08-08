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
	AccountId  float64 `json:"account_id"`
	PlayerSlot float64 `json:"player_slot"`
	HeroId     float64 `json:"hero_id"`
}

type Match struct {
	MatchId       float64  `json:"match_id"`
	MatchSeqNum   float64  `json:"match_seq_num"`
	StartTime     float64  `json:"start_time"`
	LobbyType     float64  `json:"lobby_type"`
	RadiantTeamId float64  `json:"radiant_team_id"`
	DireTeamId    float64  `json:"dire_team_id"`
	Players       []Player `json:"players"`
}

type MatchHistoryResult struct {
	Result struct {
		Status           float64 `json:"status"`
		NumResults       float64 `json:"num_results"`
		TotalResults     float64 `json:"total_results"`
		ResultsRemaining float64 `json:"results_remaining"`
		Matches          []Match `json:"matches"`
	}
}

func Start(apiKey string) {
	resp, err := http.Get(streamApi + "GetMatchHistory/v1/?key=" + apiKey)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var res MatchHistoryResult
	err = json.Unmarshal(bodyBytes, &res)
	if err != nil && res.Result.Status != 1 {
		fmt.Println("result", res)
		fmt.Println("JSON", string(bodyBytes))
		panic(err)
	}

	fmt.Println("Matches fetched:", len(res.Result.Matches))
	//fmt.Println(bodyString)
}

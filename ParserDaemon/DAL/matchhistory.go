// matchhistory
package dal

import (
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"net/http"
)

type Player struct {
	AccountId  uint32 `json:"account_id"`
	PlayerSlot uint8  `json:"player_slot"`
	HeroId     uint8  `json:"hero_id"`
}

type Match struct {
	MatchId       uint64   `json:"match_id"`
	MatchSeqNum   uint64   `json:"match_seq_num"`
	StartTime     uint32   `json:"start_time"`
	LobbyType     uint8    `json:"lobby_type"`
	RadiantTeamId uint32   `json:"radiant_team_id"`
	DireTeamId    uint32   `json:"dire_team_id"`
	Players       []Player `json:"players"`
}

type MatchHistoryResult struct {
	Result struct {
		Status           uint32  `json:"status"`
		NumResults       uint32  `json:"num_results"`
		TotalResults     uint32  `json:"total_results"`
		ResultsRemaining uint32  `json:"results_remaining"`
		Matches          []Match `json:"matches"`
	}
}

func GetMatchHistory(apiKey string, account_id uint32, startMatchId uint64, count uint16, result *MatchHistoryResult) error {
	request := fmt.Sprintf("%sGetMatchHistory/v1/?key=%s", streamApi, apiKey)
	if account_id != 0 {
		request = fmt.Sprintf("%s&account_id=%d", request, account_id)
	}
	if startMatchId != 0 {
		request = fmt.Sprintf("%s&start_at_match_id=%d", request, startMatchId)
	}
	if count != 0 {
		request = fmt.Sprintf("%s&matches_requested=%d", request, count)
	}

	log.Tracef("Request: " + request)
	resp, err := http.Get(request)
	if err != nil {
		return err
	}

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	//log.Tracef("GetMatchHistory, JSON ", string(bodyBytes))

	err = json.Unmarshal(bodyBytes, result)
	if err != nil && result.Result.Status != 1 {
		return err
	}
	return nil
}

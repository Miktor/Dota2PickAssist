// matchhistorybyseqnum
package dal

import (
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"net/http"
)

type MatchHistorySeqNumResult struct {
	Result struct {
		Status  uint8         `json:"status"`
		Matches []MatchResult `json:"matches"`
	} `json:"result"`
}

func GetMatchHistoryBySeqNum(apiKey string, startSeqNum uint64, count uint16, result *MatchHistorySeqNumResult) error {
	request := fmt.Sprintf("%sGetMatchHistoryBySequenceNum/v1/?key=%s", streamApi, apiKey)
	if startSeqNum != 0 {
		request = fmt.Sprintf("%s&start_at_match_seq_num=%d", request, startSeqNum)
	}
	if count != 0 {
		request = fmt.Sprintf("%s&matches_requested=%d", request, count)
	}

	//log.Tracef("Request: " + request)
	resp, err := http.Get(request)
	if err != nil {
		return err
	}

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	//log.Tracef("GetMatchHistory, JSON ", string(bodyBytes))

	err = json.Unmarshal(bodyBytes, result)
	if err != nil || result.Result.Status != 1 {
		log.Error("Unmarshal err=", err)
		return err
	}
	return nil
}

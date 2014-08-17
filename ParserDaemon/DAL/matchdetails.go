// MatchDetails
package dal

import (
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"net/http"
)

type AbilityUpgrades struct {
	Ability uint32 `json:"ability"`
	Time    uint64 `json:"time"`
	Level   uint8  `json:"level"`
}

type AdditionalUnits struct {
	Unitname string `json:"unitname"`
	Item_0   uint16 `json:"item_0"`
	Item_1   uint16 `json:"item_1"`
	Item_2   uint16 `json:"item_2"`
	Item_3   uint16 `json:"item_3"`
	Item_4   uint16 `json:"item_4"`
	Item_5   uint16 `json:"item_5"`
}

type PlayerEx struct {
	AccountId       uint32            `json:"account_id"`
	Player_slot     uint8             `json:"player_slot"`
	Hero_id         uint8             `json:"hero_id"`
	Item_0          uint16            `json:"item_0"`
	Item_1          uint16            `json:"item_1"`
	Item_2          uint16            `json:"item_2"`
	Item_3          uint16            `json:"item_3"`
	Item_4          uint16            `json:"item_4"`
	Item_5          uint16            `json:"item_5"`
	Kills           uint8             `json:"kills"`
	Deaths          uint8             `json:"deaths"`
	Assists         uint8             `json:"assists"`
	Leaver_status   uint8             `json:"leaver_status"`
	Gold            uint32            `json:"gold"`
	Last_hits       uint16            `json:"last_hits"`
	Denies          uint8             `json:"denies"`
	Gold_per_min    uint16            `json:"gold_per_min"`
	Xp_per_min      uint16            `json:"xp_per_min"`
	Gold_spent      uint32            `json:"gold_spent"`
	Hero_damage     uint32            `json:"hero_damage"`
	Tower_damage    uint32            `json:"tower_damage"`
	Hero_healing    uint32            `json:"hero_healing"`
	Level           uint8             `json:"level"`
	AbilityUpgrades []AbilityUpgrades `json:"ability_upgrades"`
	AdditionalUnits []AdditionalUnits `json:"additional_units"`
}

type PicksBans struct {
	IsPick bool  `json:"is_pick"`
	HeroId uint8 `json:"hero_id"`
	Team   uint8 `json:"team"`
	Order  uint8 `json:"order"`
}

type MatchResult struct {
	Players               []PlayerEx  `json:"players"`
	Season                uint64      `json:"season"`
	RadiantWin            bool        `json:"radiant_win"`
	Durration             uint32      `json:"duration"`
	StartTime             uint32      `json:"start_time"`
	MatchID               uint64      `json:"match_id"`
	MatchSeq              uint64      `json:"match_seq_num"`
	TowerStatusRadiant    uint16      `json:"tower_status_radiant"`
	TowerStatusDire       uint16      `json:"tower_status_dire"`
	BarracksStatusRadiant uint8       `json:"barracks_status_radiant"`
	BarracksStatusDire    uint8       `json:"barracks_status_dire"`
	Cluster               uint32      `json:"cluster"`
	FirstBloodTime        uint32      `json:"first_blood_time"`
	LobbyType             uint8       `json:"lobby_type"`
	HumanPlayers          uint8       `json:"human_players"`
	LeagueId              uint32      `json:"leagueid"`
	PositiveVotes         uint32      `json:"positive_votes"`
	NegativeVotes         uint32      `json:"negative_votes"`
	GameMode              uint8       `json:"game_mode"`
	PicksBans             []PicksBans `json:"picks_bans"`
	RadiantCaptain        uint64      `json:"radiant_captain"`
	DireCaptain           uint64      `json:"dire_captain"`
	RadiantTeamId         uint64      `json:"radiant_team_id"`
	RadiantName           string      `json:"radiant_name"`
	RadiantLogo           uint64      `json:"radiant_logo"`
	RadiantTeamComplete   uint8       `json:"radiant_team_complete"`
	DireTeamId            uint64      `json:"dire_team_id"`
	DireName              string      `json:"dire_name"`
	DireLogo              uint64      `json:"dire_logo"`
	DireTeamComplete      uint8       `json:"dire_team_complete"`
}

type MatchDetailsResult struct {
	Match MatchResult `json:"result"`
}

func (ctx DALContext) GetMatchDetails(apiKey string, matchId uint64, result *MatchDetailsResult) error {
	request := streamApi + "GetMatchDetails/v1/?key=" + apiKey + "&match_id=" + fmt.Sprintf("%d", matchId)
	log.Trace("Request: " + request)

	resp, err := http.Get(request)
	if err != nil {
		return err
	}

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	//log.Trace("GetMatchDetails, JSON", string(bodyBytes))

	err = json.Unmarshal(bodyBytes, result)
	if err != nil {
		return err
	}
	return nil
}

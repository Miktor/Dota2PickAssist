// dal
package dal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"net/http"
)

type DbConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Login    string `json:"login"`
	Password string `json:"password"`
	DbName   string `json:"db_name"`
}

const queryAddMatch string = "INSERT INTO matches (season, radiant_win, duration, start_time, match_id, match_seq_num, cluster, first_blood_time, lobby_type, human_players, leagueid, positive_votes, negative_votes, game_mode, tower_status_radiant, tower_status_dire, barracks_status_radiant, barracks_status_dire) " +
	"VALUES( ?, ?, ?, FROM_UNIXTIME( ? ), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
const queryAddPlayer string = "INSERT INTO match_players (match_id, account_id, hero_id, item_0, item_1, item_2, item_3, item_4, item_5, kills, deaths, assists, leaver_status, gold, last_hits, denies, gold_per_min, xp_per_min, gold_spent, hero_damage, tower_damage, hero_healing, level, skill_build, player_slot) " +
	"VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
const queryAddTeam string = "INSERT INTO match_teams (match_id, team_id, name, logo, team_complete, radiant) " +
	"VALUES( ?, ?, ?, ?, ?, ?)"
const queryAddSkillBuilds string = "INSERT INTO player_skill_builds (build_id, level, ability, time) VALUES( ?, ?, ?, FROM_UNIXTIME( ? ) )"
const queryAddCaptain string = "INSERT INTO match_captains (match_id, captain, radiant) VALUES( ?, ?, ?)"
const queryAddPickBans string = "INSERT INTO match_picks_bans (match_id, `order`, is_pick, hero_id, team) VALUES( ?, ?, ?, ?, ? )"
const queryAddUnits string = "INSERT INTO match_additional_units (match_id, account_id, unitname, item_0, item_1, item_2, item_3, item_4, item_5, player_slot) " +
	"VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"

type dbData struct {
	db *sql.DB
}

type DALContext struct {
	transaction *sql.Tx

	stmtAddSkillBuilds *sql.Stmt
	stmtAddMatch       *sql.Stmt
	stmtAddTeam        *sql.Stmt
	stmtAddCaptain     *sql.Stmt
	stmtAddPlayer      *sql.Stmt
	stmtAddPickBans    *sql.Stmt
	stmtAddUnits       *sql.Stmt
}

var dbData_ dbData

const streamApi = "https://api.steampowered.com/IDOTA2Match_570/"

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
type AbilityUpgrades struct {
	Ability uint32 `json:"ability"`
	Time    uint64 `json:"time"`
	Level   uint8  `json:"level"`
}
type AdditionalUnits struct {
	unitname string `json:"unitname"`
	Item_0   uint16 `json:"item_0"`
	Item_1   uint16 `json:"item_1"`
	Item_2   uint16 `json:"item_2"`
	Item_3   uint16 `json:"item_3"`
	Item_4   uint16 `json:"item_4"`
	Item_5   uint16 `json:"item_5"`
}
type PlayerEx struct {
	AccountId        uint32            `json:"account_id"`
	Player_slot      uint8             `json:"player_slot"`
	Hero_id          uint8             `json:"hero_id"`
	Item_0           uint16            `json:"item_0"`
	Item_1           uint16            `json:"item_1"`
	Item_2           uint16            `json:"item_2"`
	Item_3           uint16            `json:"item_3"`
	Item_4           uint16            `json:"item_4"`
	Item_5           uint16            `json:"item_5"`
	Kills            uint8             `json:"kills"`
	Deaths           uint8             `json:"deaths"`
	Assists          uint8             `json:"assists"`
	Leaver_status    uint8             `json:"leaver_status"`
	Gold             uint32            `json:"gold"`
	Last_hits        uint16            `json:"last_hits"`
	Denies           uint8             `json:"denies"`
	Gold_per_min     uint16            `json:"gold_per_min"`
	Xp_per_min       uint16            `json:"xp_per_min"`
	Gold_spent       uint32            `json:"gold_spent"`
	Hero_damage      uint32            `json:"hero_damage"`
	Tower_damage     uint32            `json:"tower_damage"`
	Hero_healing     uint32            `json:"hero_healing"`
	Level            uint8             `json:"level"`
	Ability_upgrades []AbilityUpgrades `json:"ability_upgrades"`
	Additional_units []AdditionalUnits `json:"additional_units"`
}
type PicksBans struct {
	Is_pick bool  `json:"is_pick"`
	Hero_id uint8 `json:"hero_id"`
	Team    uint8 `json:"team"`
	Order   uint8 `json:"order"`
}
type MatchDetailsResult struct {
	Result struct {
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
}

func GetMatchHistory(apiKey string, startMatchId uint64, count uint16, result *MatchHistoryResult) error {
	request := fmt.Sprintf("%sGetMatchHistory/v1/?key=%s", streamApi, apiKey)
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

	log.Tracef("GetMatchHistory, JSON ", string(bodyBytes))

	err = json.Unmarshal(bodyBytes, result)
	if err != nil && result.Result.Status != 1 {
		return err
	}
	return nil
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

	log.Trace("GetMatchDetails, JSON", string(bodyBytes))

	err = json.Unmarshal(bodyBytes, result)
	if err != nil {
		return err
	}
	return nil
}

func Connect(config DbConfig) error {
	var err error

	connectString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config.Login, config.Password, config.Host, config.Port, config.DbName)
	log.Trace("Connection string = " + connectString)
	dbData_.db, err = sql.Open("mysql", connectString)
	if err != nil {
		panic(err)
	}

	err = dbData_.db.Ping()
	if err != nil {
		panic(err)
	}

	return nil
}
func Close() {
	if dbData_.db != nil {
		dbData_.db.Close()
	}
}

func Begin() (ctx DALContext, err error) {

	ctx.transaction, err = dbData_.db.Begin()
	if err != nil {
		return
	}
	ctx.stmtAddSkillBuilds, err = ctx.transaction.Prepare(queryAddSkillBuilds)
	if err != nil {
		return
	}
	ctx.stmtAddMatch, err = ctx.transaction.Prepare(queryAddMatch)
	if err != nil {
		return
	}
	ctx.stmtAddTeam, err = ctx.transaction.Prepare(queryAddTeam)
	if err != nil {
		return
	}
	ctx.stmtAddCaptain, err = ctx.transaction.Prepare(queryAddCaptain)
	if err != nil {
		return
	}
	ctx.stmtAddPlayer, err = ctx.transaction.Prepare(queryAddPlayer)
	if err != nil {
		return
	}
	ctx.stmtAddPickBans, err = ctx.transaction.Prepare(queryAddPickBans)
	if err != nil {
		return
	}
	ctx.stmtAddUnits, err = ctx.transaction.Prepare(queryAddUnits)
	if err != nil {
		return
	}
	return
}

func (ctx DALContext) Close() (err error) {
	err = ctx.transaction.Commit()
	return
}

func (ctx DALContext) addSkillBuild(player PlayerEx, skillBuildId *int64) {
	var buildId int64

	for _, ability := range player.Ability_upgrades {
		res, err := ctx.stmtAddSkillBuilds.Exec(buildId, ability.Level, ability.Ability, ability.Time)
		if err != nil {
			panic("Failed to add ability: " + err.Error())
		}
		buildId, err = res.LastInsertId()
		if err != nil {
			panic("Failed to add ability: " + err.Error())
		}
	}
	*skillBuildId = buildId
}

func (ctx DALContext) addPlayer(matchId uint64, player PlayerEx) {
	log.Tracef("Add player, matchId = %d, player_id = %d, player_slot = %d", matchId, player.AccountId, player.Player_slot)
	var skillBuildId int64

	ctx.addSkillBuild(player, &skillBuildId)
	_, err := ctx.stmtAddPlayer.Exec(
		matchId,
		player.AccountId,
		player.Hero_id,
		player.Item_0,
		player.Item_1,
		player.Item_2,
		player.Item_3,
		player.Item_4,
		player.Item_5,
		player.Kills,
		player.Deaths,
		player.Assists,
		player.Leaver_status,
		player.Gold,
		player.Last_hits,
		player.Denies,
		player.Gold_per_min,
		player.Xp_per_min,
		player.Gold_spent,
		player.Hero_damage,
		player.Tower_damage,
		player.Hero_healing,
		player.Level,
		skillBuildId,
		player.Player_slot)

	if err != nil {
		panic("Failed to add match: " + err.Error())
	}
}

func (ctx DALContext) addTeam(match MatchDetailsResult) {
	if match.Result.RadiantCaptain != 0 {
		ctx.stmtAddCaptain.Exec(match.Result.MatchID, match.Result.RadiantCaptain, true)
	}

	if match.Result.DireCaptain != 0 {
		ctx.stmtAddCaptain.Exec(match.Result.MatchID, match.Result.DireCaptain, false)
	}

	if match.Result.RadiantTeamId != 0 {
		ctx.stmtAddTeam.Exec(match.Result.MatchID, match.Result.RadiantTeamId, match.Result.RadiantName, match.Result.RadiantLogo, match.Result.RadiantTeamComplete, true)
	}

	if match.Result.DireTeamId != 0 {
		ctx.stmtAddTeam.Exec(match.Result.MatchID, match.Result.DireTeamId, match.Result.DireName, match.Result.DireLogo, match.Result.DireTeamComplete, false)
	}
}

func (ctx DALContext) addPicks(match PlayerEx) {

}

func (ctx DALContext) addMatchData(match MatchDetailsResult) {

	log.Tracef("Add matchData, matchId = %d\n", match.Result.MatchID)

	_, err := ctx.stmtAddMatch.Exec(
		match.Result.Season,
		match.Result.RadiantWin,
		match.Result.Durration,
		match.Result.StartTime,
		match.Result.MatchID,
		match.Result.MatchSeq,
		match.Result.Cluster,
		match.Result.FirstBloodTime,
		match.Result.LobbyType,
		match.Result.HumanPlayers,
		match.Result.LeagueId,
		match.Result.PositiveVotes,
		match.Result.NegativeVotes,
		match.Result.GameMode,
		match.Result.TowerStatusRadiant,
		match.Result.TowerStatusDire,
		match.Result.BarracksStatusRadiant,
		match.Result.BarracksStatusDire)

	if err != nil {
		panic("Failed to get id: " + err.Error())
	}
}

func (ctx DALContext) AddMatch(match *MatchDetailsResult) {
	log.Tracef("Add match, matchId = %d\n", match.Result.MatchID)

	ctx.addMatchData(*match)
	for _, player := range match.Result.Players {
		ctx.addPlayer(match.Result.MatchID, player)
	}
}

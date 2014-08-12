// dal
package dal

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"net/http"
)

type DbConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Login    string `json:"login"`
	Password string `json:"password"`
	DbName   string `json:"db_name"`
}

type dbData struct {
	db                 *sql.DB
	stmtAddSkillBuilds *sql.Stmt
	stmtAddMatch       *sql.Stmt
	stmtAddTeam        *sql.Stmt
	stmtAddPlayer      *sql.Stmt
	stmtAddPickBans    *sql.Stmt
	stmtAddUnits       *sql.Stmt
}

var dbData_ dbData

const streamApi = "https://api.steampowered.com/IDOTA2Match_570/"

type Player struct {
	AccountId  uint64 `json:"account_id"`
	PlayerSlot uint8  `json:"player_slot"`
	HeroId     uint8  `json:"hero_id"`
}
type Match struct {
	MatchId       uint64   `json:"match_id"`
	MatchSeqNum   uint64   `json:"match_seq_num"`
	StartTime     uint64   `json:"start_time"`
	LobbyType     uint64   `json:"lobby_type"`
	RadiantTeamId uint64   `json:"radiant_team_id"`
	DireTeamId    uint64   `json:"dire_team_id"`
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
	AccountId        uint64            `json:"accountId"`
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
		Durration             uint64      `json:"results_remaining"`
		StartTime             uint64      `json:"start_time"`
		MatchID               uint64      `json:"match_id"`
		MatchSeq              uint64      `json:"match_seq_num"`
		TowerStatusRadiant    uint16      `json:"tower_status_radiant"`
		TowerStatusDire       uint16      `json:"tower_status_dire"`
		BarracksStatusRadiant uint8       `json:"barracks_status_radiant"`
		BarracksStatusDire    uint8       `json:"barracks_status_dire"`
		Cluster               uint8       `json:"cluster"`
		FirstBloodTime        uint64      `json:"first_blood_time"`
		LobbyType             uint8       `json:"lobby_type"`
		HumanPlayers          uint64      `json:"human_players"`
		LeagueId              uint64      `json:"leagueid"`
		PositiveVotes         uint64      `json:"positive_votes"`
		NegativeVotes         uint64      `json:"negative_votes"`
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

func GetMatchHistory(apiKey string, result *MatchHistoryResult) error {
	request := streamApi + "GetMatchHistory/v1/?key=" + apiKey
	log.Println("Request: " + request)
	resp, err := http.Get(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	err = json.Unmarshal(bodyBytes, &result)
	if err != nil && result.Result.Status != 1 {
		log.Fatalln("result", result)
		log.Fatalln("JSON", string(bodyBytes))
		return err
	}
	return nil
}
func GetMatchDetails(apiKey string, matchId uint64, result *MatchDetailsResult) error {
	request := streamApi + "GetMatchDetails/v1/?key=" + apiKey + "&match_id=" + fmt.Sprintf("%d", matchId)
	log.Println("Request: " + request)

	resp, err := http.Get(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	reader := bytes.NewReader(bodyBytes)

	d := json.NewDecoder(reader)
	d.UseNumber()
	err = d.Decode(result)
	if err != nil {
		log.Fatalln("result", result)
		log.Fatalln("JSON", string(bodyBytes))
		return err
	}
	return nil
}

func Connect(config DbConfig) error {
	var err error

	connectString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config.Login, config.Password, config.Host, config.Port, config.DbName)
	log.Println("Connection string = " + connectString)
	dbData_.db, err = sql.Open("mysql", connectString)
	if err != nil {
		panic(err)
	}

	err = dbData_.db.Ping()
	if err != nil {
		panic(err)
	}

	dbData_.stmtAddMatch, err = dbData_.db.Prepare("INSERT INTO matches (season, radiant_win, duration, start_time, match_id, match_seq_num, cluster, first_blood_time, lobby_type, human_players, leagueid, positive_votes, negative_votes, game_mode, tower_status_radiant, tower_status_dire, barracks_status_radiant, barracks_status_dire) " +
		"VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )")
	if err != nil {
		panic(err.Error())
	}

	dbData_.stmtAddPlayer, err = dbData_.db.Prepare("INSERT INTO match_players (match_id, account_id, hero_id, item_0, item_1, item_2, item_3, item_4, item_5, kills, deaths, assists, leaver_status, gold, last_hits, denies, gold_per_min, xp_per_min, gold_spent, hero_damage, tower_damage, hero_healing, level, skill_build, player_slot) " +
		"VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		panic(err.Error())
	}

	dbData_.stmtAddTeam, err = dbData_.db.Prepare("INSERT INTO match_teams (match_id, radiant_team_id, radiant_name, radiant_logo, radiant_team_complete, dire_team_id, dire_name, dire_logo, dire_team_complete, radiant_captain, dire_captain) " +
		"VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		panic(err.Error())
	}

	dbData_.stmtAddSkillBuilds, err = dbData_.db.Prepare("INSERT INTO player_skill_builds (build_id, `order`, ability) VALUES( ?, ?, ?)")
	if err != nil {
		panic(err.Error())
	}

	dbData_.stmtAddPickBans, err = dbData_.db.Prepare("INSERT INTO match_picks_bans (match_id, `order`, is_pick, hero_id, team) " +
		" VALUES( ?, ?, ?, ?, ? )")
	if err != nil {
		panic(err.Error())
	}

	dbData_.stmtAddUnits, err = dbData_.db.Prepare("INSERT INTO match_additional_units (match_id, account_id, unitname, item_0, item_1, item_2, item_3, item_4, item_5, player_slot) " +
		"VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )")
	if err != nil {
		panic(err.Error())
	}

	return nil
}

func Close() {
	if dbData_.db != nil {
		dbData_.db.Close()
		dbData_.stmtAddSkillBuilds.Close()
		dbData_.stmtAddMatch.Close()
		dbData_.stmtAddTeam.Close()
		dbData_.stmtAddPlayer.Close()
		dbData_.stmtAddPickBans.Close()
		dbData_.stmtAddUnits.Close()
	}
}

type DbContext struct {
	transaction *sql.Tx

	stmtAddSkillBuilds *sql.Stmt
	stmtAddMatch       *sql.Stmt
	stmtAddTeam        *sql.Stmt
	stmtAddPlayer      *sql.Stmt
	stmtAddPickBans    *sql.Stmt
	stmtAddUnits       *sql.Stmt
}

func Begin() (ctx DbContext, err error) {
	ctx = DbContext{}
	ctx.transaction, err = dbData_.db.Begin()
	if err != nil {
		log.Fatalln("Can't create transactions, error = " + err.Error())
		return
	}
	ctx.stmtAddSkillBuilds = ctx.transaction.Stmt(dbData_.stmtAddSkillBuilds)
	ctx.stmtAddMatch = ctx.transaction.Stmt(dbData_.stmtAddMatch)
	ctx.stmtAddTeam = ctx.transaction.Stmt(dbData_.stmtAddTeam)
	ctx.stmtAddPlayer = ctx.transaction.Stmt(dbData_.stmtAddPlayer)
	ctx.stmtAddPickBans = ctx.transaction.Stmt(dbData_.stmtAddPickBans)
	ctx.stmtAddUnits = ctx.transaction.Stmt(dbData_.stmtAddUnits)
	return
}

func (ctx DbContext) Close() error {
	return ctx.transaction.Commit()
}

func (ctx DbContext) addSkillBuild(player PlayerEx, skillBuildId *uint64) {

}

func (ctx DbContext) addPlayer(matchId uint64, player PlayerEx) {
	log.Println(fmt.Sprintf("Add player, matchId = %d, player = %d", matchId, player.Player_slot))
	var skillBuildId uint64

	ctx.addSkillBuild(player, &skillBuildId)
	_, err := dbData_.stmtAddPlayer.Exec(
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

func (ctx DbContext) addTeam(match MatchDetailsResult) {
	if match.Result.RadiantTeamId == 0 || match.Result.DireTeamId == 0 {
		return
	}

}

func (ctx DbContext) addPicks(match PlayerEx) {

}

func (ctx DbContext) addMatchData(match MatchDetailsResult) {

	log.Println(fmt.Sprintf("Add matchData, matchId = %d, %p", match.Result.MatchID, dbData_.stmtAddMatch))

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

func (ctx DbContext) AddMatch(match *MatchDetailsResult) {
	log.Println(fmt.Sprintf("Add match, matchId = %d", match.Result.MatchID))

	ctx.addMatchData(*match)
	for _, player := range match.Result.Players {
		ctx.addPlayer(match.Result.MatchID, player)
	}
}

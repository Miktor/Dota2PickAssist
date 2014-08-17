// dal
package dal

import (
	"container/list"
	"database/sql"
	"fmt"
	log "github.com/cihub/seelog"
	_ "github.com/go-sql-driver/mysql"
)

type DbConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Login    string `json:"login"`
	Password string `json:"password"`
	DbName   string `json:"db_name"`
}
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

	stmtGetLastMatchSeqNum    *sql.Stmt
	stmtGetNeedUpdateAccounts *sql.Stmt
	stmtNeedMatch             *sql.Stmt
}

var dbData_ dbData

const queryAddMatch string = "INSERT INTO matches (season, radiant_win, duration, start_time, match_id, match_seq_num, cluster, first_blood_time, lobby_type, human_players, leagueid, positive_votes, negative_votes, game_mode, tower_status_radiant, tower_status_dire, barracks_status_radiant, barracks_status_dire) " +
	"VALUES( ?, ?, ?, FROM_UNIXTIME( ? ), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
const queryAddPlayer string = "INSERT INTO match_players (match_id, account_id, hero_id, item_0, item_1, item_2, item_3, item_4, item_5, kills, deaths, assists, leaver_status, gold, last_hits, denies, gold_per_min, xp_per_min, gold_spent, hero_damage, tower_damage, hero_healing, level, skill_build, player_slot) " +
	"VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
const queryAddTeam string = "INSERT INTO match_teams (match_id, team_id, name, logo, team_complete, radiant) " +
	"VALUES( ?, ?, ?, ?, ?, ?)"
const queryAddSkillBuilds string = "INSERT INTO player_skill_builds (build_id, level, ability, time) VALUES( ?, ?, ?, FROM_UNIXTIME( ? ) )"
const queryAddCaptain string = "INSERT INTO match_captains (match_id, captain, radiant) VALUES( ?, ?, ?)"
const queryAddPickBans string = "INSERT INTO match_picks_bans (match_id, `order`, is_pick, hero_id, team) VALUES( ?, ?, ?, ?, ? )"
const queryAddUnits string = "INSERT INTO match_additional_units (match_id, account_id, unitname, item_0, item_1, item_2, item_3, item_4, item_5) " +
	"VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ? )"
const queryNeedUpdateAccounts string = "SELECT account_id FROM registred_users"
const queryNeedMatch string = "SELECT match_id FROM matches WHERE match_id = ?"
const queryLastMatchSeqNum string = "SELECT match_seq_num FROM matches ORDER BY match_seq_num DESC LIMIT 1"

const streamApi = "https://api.steampowered.com/IDOTA2Match_570/"

func (ctx DALContext) GetNeedUpdateAccounts() (error, *list.List) {
	rows, err := ctx.stmtGetNeedUpdateAccounts.Query()
	if err != nil {
		return err, nil
	}
	defer rows.Close()

	ids := list.New()

	var id uint32
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			log.Error(err)
			continue
		}
		log.Tracef("Account %d is need update\n", id)
		ids.PushBack(id)
	}

	return err, ids
}
func (ctx DALContext) NeedMatch(id uint64) error {
	rows, err := ctx.stmtNeedMatch.Query(id)
	if err != nil {
		return err
	}
	defer rows.Close()

	var resId uint64
	for rows.Next() {
		if err := rows.Scan(&resId); err != nil {
			log.Error(err)
			continue
		}
		return log.Errorf("Match %d is already exists\n", id)
	}

	return nil
}
func (ctx DALContext) GetLastMatchSeqNum() (err error, seqNum uint64) {
	rows, err := ctx.stmtGetLastMatchSeqNum.Query()
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&seqNum); err != nil {
			log.Error(err)
			continue
		}
		return
	}

	return
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

	log.Trace("Open transaction!")
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
	ctx.stmtGetNeedUpdateAccounts, err = ctx.transaction.Prepare(queryNeedUpdateAccounts)
	if err != nil {
		return
	}
	ctx.stmtNeedMatch, err = ctx.transaction.Prepare(queryNeedMatch)
	if err != nil {
		return
	}
	ctx.stmtGetLastMatchSeqNum, err = ctx.transaction.Prepare(queryLastMatchSeqNum)
	if err != nil {
		return
	}

	return
}
func (ctx DALContext) Close() (err error) {
	log.Trace("Close transaction!")
	err = ctx.transaction.Commit()
	return
}

func (ctx DALContext) addUnits(matchId uint64, player PlayerEx) {
	for _, unit := range player.AdditionalUnits {
		_, err := ctx.stmtAddUnits.Exec(matchId, player.AccountId, unit.Unitname, unit.Item_0, unit.Item_1, unit.Item_2, unit.Item_3, unit.Item_4, unit.Item_5)
		if err != nil {
			panic("Failed to add unit: " + err.Error())
		}
	}
}
func (ctx DALContext) addSkillBuild(player PlayerEx, skillBuildId *int64) {
	var buildId int64

	for _, ability := range player.AbilityUpgrades {
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

	ctx.addUnits(matchId, player)
}
func (ctx DALContext) addTeam(match MatchResult) {
	if match.RadiantCaptain != 0 {
		ctx.stmtAddCaptain.Exec(match.MatchID, match.RadiantCaptain, true)
	}

	if match.DireCaptain != 0 {
		ctx.stmtAddCaptain.Exec(match.MatchID, match.DireCaptain, false)
	}

	if match.RadiantTeamId != 0 {
		ctx.stmtAddTeam.Exec(match.MatchID, match.RadiantTeamId, match.RadiantName, match.RadiantLogo, match.RadiantTeamComplete, true)
	}

	if match.DireTeamId != 0 {
		ctx.stmtAddTeam.Exec(match.MatchID, match.DireTeamId, match.DireName, match.DireLogo, match.DireTeamComplete, false)
	}
}
func (ctx DALContext) addPicks(match MatchResult) {
	var err error

	for _, pickBan := range match.PicksBans {
		_, err = ctx.stmtAddPickBans.Exec(match.MatchID, pickBan.Order, pickBan.IsPick, pickBan.HeroId, pickBan.Team)
		if err != nil {
			log.Critical("Failed to get id: " + err.Error())
		}
	}
}
func (ctx DALContext) addMatchData(match MatchResult) {
	_, err := ctx.stmtAddMatch.Exec(
		match.Season,
		match.RadiantWin,
		match.Durration,
		match.StartTime,
		match.MatchID,
		match.MatchSeq,
		match.Cluster,
		match.FirstBloodTime,
		match.LobbyType,
		match.HumanPlayers,
		match.LeagueId,
		match.PositiveVotes,
		match.NegativeVotes,
		match.GameMode,
		match.TowerStatusRadiant,
		match.TowerStatusDire,
		match.BarracksStatusRadiant,
		match.BarracksStatusDire)

	if err != nil {
		log.Critical("Failed to get id: " + err.Error())
	}

	ctx.addPicks(match)
	ctx.addTeam(match)
}
func (ctx DALContext) AddMatch(match *MatchResult) {
	log.Tracef("Add match, matchId = %d\n", match.MatchID)

	ctx.addMatchData(*match)
	for _, player := range match.Players {
		ctx.addPlayer(match.MatchID, player)
	}
}

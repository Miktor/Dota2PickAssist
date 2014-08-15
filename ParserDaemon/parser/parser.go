// Parser
package parser

import (
	"../dal"
	"fmt"
	log "github.com/cihub/seelog"
	"time"
)

const maxUint = ^uint32(0)

func preValidateMatch(match *dal.Match) error {
	for _, player := range match.Players {
		if player.AccountId == maxUint {
			return fmt.Errorf("One of players have invalid ID (id == -1).")
		}
	}
	return nil
}

func validateMatch(match *dal.MatchDetailsResult) error {
	return nil
}

func Start(apiKey string) {

	var matchDetails dal.MatchDetailsResult
	var matches dal.MatchHistoryResult
	var err error

	start := time.Now()

	var ctx dal.DALContext

	var startMatchId uint64
	var count uint16

	for i := 0; i < 10; i++ {
		ctx, err = dal.Begin()

		if err != nil {
			log.Criticalf("Failed to begin transaction, error: %v\n", err)
			continue
		}

		defer ctx.Close()

		for {
			err = dal.GetMatchHistory(apiKey, startMatchId, count, &matches)

			if err != nil {
				log.Errorf("Failed to get MatchHistory, error: %v\n", err)
			}

			log.Tracef("GetMatchHistory header:\n"+
				"\tStatus           = %d\n"+
				"\tNumResults       = %d\n"+
				"\tTotalResults     = %d\n"+
				"\tResultsRemaining = %d\n",
				matches.Result.Status, matches.Result.NumResults, matches.Result.TotalResults, matches.Result.ResultsRemaining)

			if matches.Result.NumResults == 0 {
				log.Info("GetMatchHistory returned 0 matches\n")
				break
			}

			if matches.Result.NumResults != uint32(len(matches.Result.Matches)) {
				log.Warnf("GetMatchHistory NumResults mismatch (%d != %d)\n", matches.Result.NumResults, len(matches.Result.Matches))
			}

			startMatchId = matches.Result.Matches[matches.Result.NumResults-1].MatchId - 1

			for _, match := range matches.Result.Matches {

				err = preValidateMatch(&match)
				if err != nil {
					log.Infof("Invalid match (%d), pre validation error = %s\n", match.MatchId, err)
					continue
				}

				err = ctx.GetMatchDetails(apiKey, match.MatchId, &matchDetails)
				if err != nil {
					log.Errorf("Failed to get MatchDetails, error: %v\n", err)
				}

				err = validateMatch(&matchDetails)
				if err != nil {
					log.Infof("Invalid match (%d), validation error = %s\n", match.MatchId, err)
					continue
				}
				ctx.AddMatch(&matchDetails)
			}
		}
	}

	elapsed := time.Since(start)
	log.Tracef("add took %s", elapsed)
}

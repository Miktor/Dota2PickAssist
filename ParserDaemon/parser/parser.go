// Parser
package parser

import (
	"../dal"
	"fmt"
	"log"
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

	err = dal.GetMatchHistory(apiKey, 0, 0, &matches)

	log.Printf("GetMatchHistory returned %d matches\n", matches.Result.NumResults)

	if err != nil {
		log.Printf("Failed to get MatchHistory, error: %v\n", err)
	}

	start := time.Now()

	var ctx dal.DbContext
	ctx, err = dal.Begin()

	if err != nil {
		log.Printf("Failed to begin transaction, error: %v\n", err)
		panic(err)
	}

	for _, match := range matches.Result.Matches {

		err = preValidateMatch(&match)
		if err != nil {
			log.Printf("Invalid match (%d), pre validation error = %s\n", match.MatchId, err)
			continue
		}

		err = dal.GetMatchDetails(apiKey, match.MatchId, &matchDetails)
		if err != nil {
			log.Printf("Failed to get MatchDetails, error: %v\n", err)
		}

		err = validateMatch(&matchDetails)
		if err != nil {
			log.Printf("Invalid match (%d), validation error = %s\n", match.MatchId, err)
			continue
		}
		ctx.AddMatch(&matchDetails)
	}

	ctx.Close()

	elapsed := time.Since(start)
	log.Printf("add took %s", elapsed)
}

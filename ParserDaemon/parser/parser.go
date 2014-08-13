// Parser
package parser

import (
	"../dal"
	"log"
	"time"
)

func Start(apiKey string) {

	var matchDetails dal.MatchDetailsResult
	var matches dal.MatchHistoryResult
	var err error

	err = dal.GetMatchHistory(apiKey, 0, 0, &matches)

	log.Fatalln("GetMatchHistory returned %d matches", matches.Result.NumResults)

	if err != nil {
		log.Fatalln("Failed to get MatchHistory, error: %v", err)
	}

	start := time.Now()

	var ctx dal.DbContext
	ctx, err = dal.Begin()

	if err != nil {
		log.Fatalln("Failed to begin transaction, error: %v", err)
		panic(err)
	}

	for _, match := range matches.Result.Matches {
		err = dal.GetMatchDetails(apiKey, match.MatchId, &matchDetails)
		if err != nil {
			log.Fatalln("Failed to get MatchDetails, error: %v", err)
		}

		ctx.AddMatch(&matchDetails)
	}

	ctx.Close()

	elapsed := time.Since(start)
	log.Printf("add took %s", elapsed)
}

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

func addMatches(ctx *dal.DALContext, apiKey string, matches *dal.MatchHistoryResult) {
	var matchDetails dal.MatchDetailsResult
	for _, match := range matches.Result.Matches {
		err := ctx.NeedMatch(match.MatchId)
		if err != nil {
			log.Infof("Match (%d) alredy added or error = %s\n", match.MatchId, err)
			continue
		}

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
		ctx.AddMatch(&matchDetails.Match)
	}
}

func updateAccountMatches(apiKey string, accountId uint32, startMatchId uint64, count uint16) error {
	var matches dal.MatchHistoryResult

	log.Trace("Update Account(%d) Matches", accountId)

	ctx, err := dal.Begin()

	if err != nil {
		log.Criticalf("Failed to begin transaction, error: %v\n", err)
		return err
	}
	defer ctx.Close()

	var retries uint8
	for {
		log.Tracef("GetMatchHistory accountId=%d, retries=%d, startMatchId=%d", accountId, retries, startMatchId)
		err = dal.GetMatchHistory(apiKey, accountId, startMatchId, 0, &matches)

		if err != nil {
			log.Errorf("Failed to get MatchHistory, error: %v\n", err)
		}

		log.Tracef("GetMatchHistory header: Status = %d, NumResults = %d, TotalResults = %d, ResultsRemaining = %d",
			matches.Result.Status, matches.Result.NumResults, matches.Result.TotalResults, matches.Result.ResultsRemaining)

		if matches.Result.NumResults == 0 {
			log.Info("GetMatchHistory returned 0 matches\n")
			if matches.Result.Status == 1 && retries < 10 {
				retries++
				startMatchId--
				continue
			}
			return nil
		}

		retries = 0

		if matches.Result.NumResults != uint32(len(matches.Result.Matches)) {
			log.Warnf("GetMatchHistory NumResults mismatch (%d != %d)\n", matches.Result.NumResults, len(matches.Result.Matches))
		}

		startMatchId = matches.Result.Matches[matches.Result.NumResults-1].MatchId - 1
		addMatches(&ctx, apiKey, &matches)
	}
	return nil
}

func updateMatchesFromSeq(apiKey string, seqNum uint64) (err error, lastSeqNum uint64) {
	var result dal.MatchHistorySeqNumResult
	log.Trace("Add matches by seq num")
	err = dal.GetMatchHistoryBySeqNum(apiKey, seqNum, 0, &result)

	if err != nil {
		return
	}
	ctx, err := dal.Begin()

	if err != nil {
		log.Criticalf("Failed to begin transaction, error: %v\n", err)
		return
	}
	defer ctx.Close()

	for _, match := range result.Result.Matches {
		ctx.AddMatch(&match)
		lastSeqNum = match.MatchSeq
	}
	return
}

func Start(apiKey string) {
	start := time.Now()

	var seqNum uint64
	{
		log.Trace("Enter main loop")
		ctx, err := dal.Begin()

		if err != nil {
			log.Criticalf("Failed to begin transaction, error: %s\n", err)
		}
		defer ctx.Close()

		err, seqNum = ctx.GetLastMatchSeqNum()
		if err != nil {
			log.Criticalf("Failed to get Last Match Seq Num. err = %s\n", err)
		}
	}

	for i := 0; i < 1; i++ {
		seqNum++
		err, seqNum := updateMatchesFromSeq(apiKey, seqNum)

		if err != nil {
			log.Critical(err)
		}

		seqNum++
	}

	elapsed := time.Since(start)
	log.Tracef("add took %s", elapsed)
}

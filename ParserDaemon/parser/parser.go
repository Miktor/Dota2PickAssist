// Parser
package parser

import (
	"../dal"
	"code.google.com/p/go.net/context"
	"fmt"
	log "github.com/cihub/seelog"
	"sync"
	"time"
)

const maxUint = ^uint32(0)

func preValidateMatch(match dal.Match) error {
	for _, player := range match.Players {
		if player.AccountId == maxUint {
			return fmt.Errorf("One of players have invalid ID (id == -1).")
		}
	}
	return nil
}

func validateMatch(match dal.MatchDetailsResult) error {
	return nil
}

func addMatchesWithValidation(ctx dal.DALContext, apiKey string, matches dal.MatchHistoryResult) {
	var matchDetails dal.MatchDetailsResult
	for _, match := range matches.Result.Matches {
		err := ctx.NeedMatch(match.MatchId)
		if err != nil {
			log.Infof("Match (%d) alredy added or error = %s\n", match.MatchId, err)
			continue
		}

		err = preValidateMatch(match)
		if err != nil {
			log.Infof("Invalid match (%d), pre validation error = %s\n", match.MatchId, err)
			continue
		}

		err = ctx.GetMatchDetails(apiKey, match.MatchId, &matchDetails)
		if err != nil {
			log.Errorf("Failed to get MatchDetails, error: %v\n", err)
		}

		err = validateMatch(matchDetails)
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
		addMatchesWithValidation(ctx, apiKey, matches)
	}
	return nil
}

func addMatches(matches []dal.MatchResult) (err error) {
	var ctx dal.DALContext
	ctx, err = dal.Begin()

	if err != nil {
		log.Criticalf("Failed to begin transaction, error: %v\n", err)
		return
	}
	defer ctx.Close()

	var wg sync.WaitGroup
	wg.Add(len(matches))
	for _, match := range matches {
		go func(match dal.MatchResult) {
			ctx.AddMatch(&match)
			wg.Done()
		}(match)
	}

	wg.Wait()

	return
}

func Start(execCtx context.Context, apiKey string) {
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

	var m sync.Mutex
	matchHistorySeqNumResults := make(chan dal.MatchHistorySeqNumResult, 1)

	go getMatchesBySeq(apiKey, &seqNum, matchHistorySeqNumResults, m)

	for {
		select {
		case <-execCtx.Done():
			close(matchHistorySeqNumResults)
			break
		case res := <-matchHistorySeqNumResults:
			go getMatchesBySeq(apiKey, &seqNum, matchHistorySeqNumResults, m)

			err := addMatches(res.Result.Matches)

			if err != nil {
				log.Critical(err)
			}
		}
	}

	elapsed := time.Since(start)
	log.Tracef("add took %s", elapsed)
}

func getMatchesBySeq(apiKey string, seqNum *uint64, hist chan dal.MatchHistorySeqNumResult, m sync.Mutex) error {
	m.Lock()
	defer m.Unlock()

	var result dal.MatchHistorySeqNumResult
	err := dal.GetMatchHistoryBySeqNum(apiKey, (*seqNum)+1, 0, &result)

	if err != nil {
		return err
	}

	*seqNum = result.Result.Matches[len(result.Result.Matches)-1].MatchSeq
	hist <- result
	return err
}

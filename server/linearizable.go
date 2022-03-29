package server

import (
	"context"
	"encoding/binary"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
	"time"
)

func (s *Server) linearizableReadNotify(ctx context.Context) error {
	sharedV := s.readIndexSharedNtf.CurrentShared(func() {
		s.readIndexWaitC <- ctx
	})
	select {
	case <-sharedV.Wait():
		return sharedV.Value().(error)
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopped:
		return ErrStopped
	}
}

func (s *Server) linearizableReadIndexLoop() {
	for {
		var ctx context.Context
		reqID := s.reqIDGen.Next()
		select {
		case <-s.stopped:
			return
		case ctx = <-s.readIndexWaitC:
		}
		select {
		case <-time.After(time.Duration(s.Config.ReadIndexBatchTimeoutMs) * time.Millisecond):
		case <-s.stopped:
			return
		}
		oldSV := s.readIndexSharedNtf.NextShared()
		s.fw.Attach(func() {
			index, err := s.requestReadIndex(ctx, reqID)
			if err != nil {
				oldSV.Notify(err)
			}
			select {
			case <-s.timelineNtf.Wait(index):
				oldSV.Notify(nil)
			case <-ctx.Done():
				oldSV.Notify(ctx.Err())
			case <-s.stopped:
				oldSV.Notify(ErrStopped)
				return
			}
		})
	}
}

const readIndexRetryTime = 500 * time.Millisecond

func (s *Server) requestReadIndex(ctx context.Context, reqID uint64) (index uint64, err error) {
	leaderChangeC := s.leaderChangeNtf.Wait()
	indexCh := s.reqNotifier.Register(reqID)
	defer s.reqNotifier.Notify(reqID, nil)
	ctx, cancelF := context.WithTimeout(ctx, s.Config.GetRequestTimeout())
	defer cancelF()
	err = s.sendReadIndex(ctx, reqID)
	if err != nil {
		return 0, err
	}
	retryTimer := time.NewTimer(readIndexRetryTime)
	defer retryTimer.Stop()
	for {
		select {
		case <-leaderChangeC:
			leaderChangeC = s.leaderChangeNtf.Wait()
			return 0, ErrLeaderChanged
		case <-ctx.Done():
			err = ctx.Err()
			if err != nil {
				return 0, err
			}
			return 0, ErrTimeout
		case <-retryTimer.C:
			s.lg.Warn("waiting for ReadIndex response took too long, retrying",
				zap.Uint64("sent-request-id", reqID),
				zap.Duration("retry-timeout", readIndexRetryTime),
			)
			err = s.sendReadIndex(ctx, reqID)
			if err != nil {
				return 0, err
			}
			retryTimer.Reset(readIndexRetryTime)
		case i := <-indexCh:
			if i == nil {
				return 0, ErrTimeout
			}
			index = i.(uint64)
			return
		}
	}
}

func reqID2ReadIndexContext(ID uint64) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, ID)
	return ret
}

func readIndexContext2ReqID(ctx []byte) uint64 {
	return binary.BigEndian.Uint64(ctx)
}

func (s *Server) sendReadIndex(ctx context.Context, reqID uint64) error {
	err := s.raftNode.ReadIndex(ctx, reqID2ReadIndexContext(reqID))
	if err != nil {
		s.lg.Warn("failed to call ReadIndex from Raft", zap.Error(err))
	}
	return err
}

// reading raftNode's ReadStatesC
func (s *Server) processReadStates(states []raft.ReadState) {
	var readIndexReqID uint64
	for _, st := range states {
		readIndexReqID = readIndexContext2ReqID(st.RequestCtx)
		s.reqNotifier.Notify(readIndexReqID, st.Index)
	}
}

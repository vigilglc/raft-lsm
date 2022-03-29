package cluster

import (
	"crypto/sha1"
	"encoding/binary"
)

type Member struct {
	ID uint64 `json:"ID"`
	AddrInfo
	IsLearner bool `json:"isLearner"`
}

func NewMember(clusterName string, addrInfo AddrInfo, assignedID *uint64, isLearner bool) *Member {
	var memberID uint64
	if assignedID == nil {
		memberID = computeMemberID(clusterName, addrInfo.Name, addrInfo.Host)
	} else {
		memberID = *assignedID
	}
	return &Member{
		ID:        memberID,
		AddrInfo:  addrInfo,
		IsLearner: isLearner,
	}
}

func computeMemberID(clusterName, memberName, host string) uint64 {
	var data = make([]byte, 0, len(clusterName)+len(memberName)+len(host))
	data = append(data, []byte(clusterName)...)
	data = append(data, []byte(memberName)...)
	data = append(data, []byte(host)...)
	sum := sha1.Sum(data)
	return binary.BigEndian.Uint64(sum[:8])
}

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

func NewMember(clusterName string, addrInfo AddrInfo, isLearner bool) *Member {
	var memberID = ComputeMemberID(clusterName, addrInfo)
	return &Member{
		ID:        memberID,
		AddrInfo:  addrInfo,
		IsLearner: isLearner,
	}
}

func ComputeMemberID(clusterName string, addrInfo AddrInfo) uint64 {
	var data = make([]byte, 0, len(clusterName)+len(addrInfo.Name)+len(addrInfo.Host))
	data = append(data, []byte(clusterName)...)
	data = append(data, []byte(addrInfo.Name)...)
	data = append(data, []byte(addrInfo.Host)...)
	sum := sha1.Sum(data)
	return binary.BigEndian.Uint64(sum[:8])
}

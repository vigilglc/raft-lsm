package benchmark

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func newRandomBytes(len int) []byte {
	bs := make([]byte, len)
	_, err := rand.Read(bs)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	return bs
}

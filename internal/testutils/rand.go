package testutils

import (
	"fmt"
	"math/rand"
)

const (
	// CharSetAlphaNum is the alphanumeric character set for use with
	// RandStringFromCharSet
	CharSetAlphaNum = "abcdefghijklmnopqrstuvwxyz012346789"

	// CharSetAlpha is the alphabetical character set for use with
	// RandStringFromCharSet
	CharSetAlpha = "abcdefghijklmnopqrstuvwxyz"
)

// RandInt generates a random integer
func RandInt() int {
	return rand.Int()
}

// RandIntWithPrefix is used to generate a unique name with a prefix
func RandIntWithPrefix(name string) string {
	return fmt.Sprintf("%s-%d", name, RandInt())
}

// RandIntRange returns a random integer between min (inclusive) and max (exclusive)
func RandIntRange(min int, max int) int {
	return min + rand.Intn(max-min)
}

// RandString generates a random alphanumeric string of the length specified
func RandString(strlen int) string {
	return RandStringFromCharSet(strlen, CharSetAlphaNum)
}

// RandStringFromCharSet generates a random string by selecting characters from
// the charset provided
func RandStringFromCharSet(strlen int, charSet string) string {
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = charSet[RandIntRange(0, len(charSet))]
	}
	return string(result)
}

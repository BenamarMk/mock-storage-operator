package util

import (
	"fmt"
	"hash/crc32"

	"github.com/google/uuid"
)

func GetRID() string {
	return GetHashedName(uuid.New().String())
}

func GetHashedName(name string) string {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(name)))
}

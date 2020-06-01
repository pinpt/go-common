package strings

import "github.com/google/uuid"

// NewUUIDV4 returns a new UUID v4
func NewUUIDV4() string {
	v4, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	return v4.String()
}

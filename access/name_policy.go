package access

import (
	"errors"
	"strconv"

	"github.com/pocketstation-io/relay/access/names"
)

// NameAllocationPolicy selects supported readable-name lengths. The opaque
// join code, never the name length, controls delegated access.
type NameAllocationPolicy string

const (
	AutoTwoThenThree NameAllocationPolicy = "auto"
	FixedTwo         NameAllocationPolicy = "2"
	FixedThree       NameAllocationPolicy = "3"
)

// ParseNameAllocationPolicy is shared by standalone and managed compositions.
func ParseNameAllocationPolicy(getenv func(string) string) (NameAllocationPolicy, error) {
	value := NameAllocationPolicy(getenv("POCKETSTATION_NAME_POLICY"))
	if value == "" {
		return AutoTwoThenThree, nil
	}
	if value == AutoTwoThenThree {
		return value, nil
	}
	if _, valid := value.fixedCount(); valid {
		return value, nil
	}
	return "", errors.New("POCKETSTATION_NAME_POLICY must be auto or an integer from 2 through 15")
}

// FixedNamePolicy configures exactly count words. It never falls back to another
// length; auto is the separate default two/three-word collision policy.
func FixedNamePolicy(count int) (NameAllocationPolicy, error) {
	if count < names.MinWordCount || count > names.MaxWordCount {
		return "", ErrInvalidWordCount
	}
	return NameAllocationPolicy(strconv.Itoa(count)), nil
}
func (policy NameAllocationPolicy) fixedCount() (int, bool) {
	count, err := strconv.Atoi(string(policy))
	return count, err == nil && count >= names.MinWordCount && count <= names.MaxWordCount && strconv.Itoa(count) == string(policy)
}

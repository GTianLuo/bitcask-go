package utils

func IsValidKey(key []byte) bool {
	if key == nil || len(key) == 0 {
		return false
	}
	return true
}

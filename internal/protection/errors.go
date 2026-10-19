package protection

import "errors"

// errProtectionMisconfigured is returned when hosted protection is enabled
// with missing or invalid configuration. Messages never include key material.
var errProtectionMisconfigured = errors.New("ARCJET_KEY must be a valid Arcjet key when ARCJET_ENABLED is true")

// ErrProtectionMisconfigured reports whether err is a protection config error.
func ErrProtectionMisconfigured(err error) bool {
	return errors.Is(err, errProtectionMisconfigured)
}

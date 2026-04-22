package b503session

import "errors"

// Sentinel errors surfaced by the session manager. Callers MUST use
// errors.Is for classification; the string forms are stable but not a
// part of the API contract.
var (
	// ErrSessionBusy indicates either a second claimant racing with an
	// existing owner, or a post-refresh-failure state where the session
	// is no longer usable and must be re-enabled.
	ErrSessionBusy = errors.New("b503session: session held under a different issuer_token")
	// ErrTransportDown is returned by the refresh func when the underlying
	// transport cannot be re-established.
	ErrTransportDown = errors.New("b503session: transport currently disconnected")
	// ErrNotActive indicates the session is not in Active state for the
	// provided transport key.
	ErrNotActive = errors.New("b503session: session not active")
	// ErrWrongToken indicates a Disable call whose issuer_token does not
	// match the currently-held session.
	ErrWrongToken = errors.New("b503session: issuer_token mismatch")
)

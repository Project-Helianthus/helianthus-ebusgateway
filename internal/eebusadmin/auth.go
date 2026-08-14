package eebusadmin

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	ownerSessionCookie = "helianthus_eebus_admin"
	maxOwnerSessions   = 32
	credentialMinBytes = 12
	credentialMaxBytes = 256
)

type ownerSession struct {
	id        string
	csrf      string
	expiresAt time.Time
}

type authentication struct {
	mu sync.Mutex

	ownerUsername string
	ownerSecret   []byte
	haSecret      []byte
	ownerOrigin   string
	sessionTTL    time.Duration
	now           func() time.Time
	random        io.Reader
	sessions      map[string]ownerSession
}

type authenticatedRequest struct {
	principal Principal
	session   ownerSession
}

func newAuthentication(config AuthConfig) (*authentication, error) {
	if !validUsername(config.OwnerUsername) {
		return nil, errors.New("owner username is invalid")
	}
	if !validCredential(config.OwnerSecret) || !validCredential(config.HASecret) || subtle.ConstantTimeCompare(config.OwnerSecret, config.HASecret) == 1 {
		return nil, errors.New("admin credentials are invalid")
	}
	origin, err := url.Parse(config.OwnerOrigin)
	if err != nil || (origin.Scheme != "http" && origin.Scheme != "https") || origin.Host == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return nil, errors.New("owner origin is invalid")
	}
	if config.SessionTTL <= 0 || config.SessionTTL > 24*time.Hour || config.Random == nil {
		return nil, errors.New("owner session configuration is invalid")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &authentication{
		ownerUsername: config.OwnerUsername,
		ownerSecret:   append([]byte(nil), config.OwnerSecret...),
		haSecret:      append([]byte(nil), config.HASecret...),
		ownerOrigin:   strings.TrimSuffix(config.OwnerOrigin, "/"),
		sessionTTL:    config.SessionTTL,
		now:           config.Now,
		random:        config.Random,
		sessions:      make(map[string]ownerSession),
	}, nil
}

func validCredential(value []byte) bool {
	return len(value) >= credentialMinBytes && len(value) <= credentialMaxBytes && visibleASCII(value, false)
}

func validUsername(value string) bool {
	return len(value) > 0 && len(value) <= 64 && visibleASCII([]byte(value), true)
}

func visibleASCII(value []byte, rejectColon bool) bool {
	for _, character := range value {
		if character < 0x21 || character > 0x7e || (rejectColon && character == ':') {
			return false
		}
	}
	return true
}

func (auth *authentication) authenticate(w http.ResponseWriter, request *http.Request) (authenticatedRequest, string) {
	if auth == nil {
		return authenticatedRequest{}, "admin_boundary_unavailable"
	}
	authorization := request.Header.Get("Authorization")
	if strings.HasPrefix(authorization, "Bearer ") {
		if len(request.Cookies()) != 0 || request.Header.Get("Origin") != "" || request.Header.Get("Referer") != "" {
			return authenticatedRequest{}, "forbidden"
		}
		candidate := []byte(strings.TrimPrefix(authorization, "Bearer "))
		if subtle.ConstantTimeCompare(candidate, auth.haSecret) != 1 {
			return authenticatedRequest{}, "unauthenticated"
		}
		return authenticatedRequest{principal: PrincipalHAIntegration}, ""
	}
	if authorization != "" && !strings.HasPrefix(authorization, "Basic ") {
		return authenticatedRequest{}, "unauthenticated"
	}
	if cookie, err := request.Cookie(ownerSessionCookie); err == nil && cookie.Value != "" {
		if session, ok := auth.ownerSession(cookie.Value); ok {
			w.Header().Set("X-CSRF-Token", session.csrf)
			return authenticatedRequest{principal: PrincipalPortalOwner, session: session}, ""
		}
	}
	if username, password, ok := request.BasicAuth(); ok {
		usernameMatch := subtle.ConstantTimeCompare([]byte(username), []byte(auth.ownerUsername))
		passwordMatch := subtle.ConstantTimeCompare([]byte(password), auth.ownerSecret)
		if usernameMatch != 1 || passwordMatch != 1 {
			return authenticatedRequest{}, "unauthenticated"
		}
		session, err := auth.issueOwnerSession()
		if err != nil {
			return authenticatedRequest{}, "admin_boundary_unavailable"
		}
		auth.writeOwnerSession(w, request, session)
		return authenticatedRequest{principal: PrincipalPortalOwner, session: session}, ""
	}
	return authenticatedRequest{}, "unauthenticated"
}

func (auth *authentication) issueOwnerSession() (ownerSession, error) {
	auth.mu.Lock()
	defer auth.mu.Unlock()
	now := auth.now()
	for key, session := range auth.sessions {
		if !now.Before(session.expiresAt) {
			delete(auth.sessions, key)
		}
	}
	if len(auth.sessions) >= maxOwnerSessions {
		return ownerSession{}, errors.New("owner session capacity exhausted")
	}
	id, err := randomToken(auth.random)
	if err != nil {
		return ownerSession{}, err
	}
	csrf, err := randomToken(auth.random)
	if err != nil {
		return ownerSession{}, err
	}
	session := ownerSession{id: id, csrf: csrf, expiresAt: now.Add(auth.sessionTTL)}
	auth.sessions[id] = session
	return session, nil
}

func (auth *authentication) ownerSession(id string) (ownerSession, bool) {
	auth.mu.Lock()
	defer auth.mu.Unlock()
	session, ok := auth.sessions[id]
	if !ok || !auth.now().Before(session.expiresAt) {
		delete(auth.sessions, id)
		return ownerSession{}, false
	}
	return session, true
}

func (auth *authentication) writeOwnerSession(w http.ResponseWriter, request *http.Request, session ownerSession) {
	http.SetCookie(w, &http.Cookie{
		Name:     ownerSessionCookie,
		Value:    session.id,
		Path:     "/admin/eebus/v1/",
		HttpOnly: true,
		Secure:   strings.HasPrefix(auth.ownerOrigin, "https://"),
		SameSite: http.SameSiteStrictMode,
		Expires:  session.expiresAt,
		MaxAge:   int(session.expiresAt.Sub(auth.now()).Seconds()),
	})
	w.Header().Set("X-CSRF-Token", session.csrf)
}

func (auth *authentication) validateCSRF(request *http.Request, session ownerSession) bool {
	if request.Method == http.MethodGet || request.Method == http.MethodHead {
		return true
	}
	if request.Header.Get("Origin") != auth.ownerOrigin || subtle.ConstantTimeCompare([]byte(request.Header.Get("X-CSRF-Token")), []byte(session.csrf)) != 1 {
		return false
	}
	referer, err := url.Parse(request.Header.Get("Referer"))
	if err != nil || referer.User != nil || referer.Scheme+"://"+referer.Host != auth.ownerOrigin {
		return false
	}
	return true
}

func randomToken(random io.Reader) (string, error) {
	buffer := make([]byte, 32)
	if _, err := io.ReadFull(random, buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

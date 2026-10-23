package portal

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

// IngressPathHeader is the request header in which a Home Assistant Supervisor
// Ingress proxy reports the path prefix it stripped before forwarding. Portal
// uses it only to build the URLs it writes back in the same response; it never
// selects a route, a mount or an authorization decision.
const IngressPathHeader = "X-Ingress-Path"

var errInvalidIngressPath = errors.New("invalid ingress path header")

// validPortalPath accepts only unambiguous absolute paths. Configuration is
// rejected rather than cleaned, so it cannot acquire a different route meaning.
func validPortalPath(value string) bool {
	if value == "" || !strings.HasPrefix(value, "/") || strings.ContainsAny(value, "\\%?#") || strings.Contains(value, "//") {
		return false
	}
	for _, c := range value {
		if c <= ' ' || c >= 127 {
			return false
		}
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	u, err := url.Parse(value)
	return err == nil && u.Scheme == "" && u.Host == "" && u.Path == value
}

func validatePortalPaths(opts Options) error {
	if opts.ExternalPrefix != "" && (!validPortalPath(opts.ExternalPrefix) || strings.HasSuffix(opts.ExternalPrefix, "/")) {
		return errors.New("invalid portal path field: ExternalPrefix")
	}
	if !validPortalPath(opts.BasePath) || !strings.HasSuffix(opts.BasePath, "/") {
		return errors.New("invalid portal path field: BasePath")
	}
	for _, endpoint := range []struct{ name, value string }{
		{"GraphQLPath", opts.GraphQLPath},
		{"PortalCatalogPath", opts.PortalCatalogPath},
		{"SnapshotPath", opts.SnapshotPath},
		{"SubscriptionPath", opts.SubscriptionPath},
		{"MCPPath", opts.MCPPath},
		{"EEBusAdminPath", opts.EEBusAdminPath},
	} {
		if endpoint.value != "" && (!validPortalPath(endpoint.value) || strings.HasSuffix(endpoint.value, "/")) {
			return errors.New("invalid portal path field: " + endpoint.name)
		}
	}
	return nil
}

// servedURLs holds the external URL roots of one response: prefix is the
// proxy-reported Ingress prefix followed by the configured ExternalPrefix, and
// base is the Portal directory under that prefix.
type servedURLs struct {
	prefix string
	base   string
}

// endpoint maps a configured gateway endpoint to its served URL exactly once.
// A disabled endpoint stays empty instead of becoming the prefix alone.
func (u servedURLs) endpoint(endpoint string) string {
	if endpoint == "" {
		return ""
	}
	return u.prefix + endpoint
}

// servedURLs derives the URL roots for r. The Ingress prefix is validated by
// the configured-path rules; an empty, repeated or malformed value is an error
// rather than a silent fallback to root-absolute URLs the proxy cannot route.
func (h *handler) servedURLs(r *http.Request) (servedURLs, error) {
	ingress := ""
	switch values := r.Header.Values(IngressPathHeader); len(values) {
	case 0:
	case 1:
		if !validPortalPath(values[0]) || strings.HasSuffix(values[0], "/") {
			return servedURLs{}, errInvalidIngressPath
		}
		ingress = values[0]
	default:
		return servedURLs{}, errInvalidIngressPath
	}
	prefix := ingress + h.opts.ExternalPrefix
	return servedURLs{prefix: prefix, base: prefix + h.mountPath + "/"}, nil
}

// responseURLs resolves the URL roots for a response whose body or Location
// embeds Portal URLs and marks it as varying with the Ingress prefix. A bad
// prefix fails only that response with a generic message.
func (h *handler) responseURLs(w http.ResponseWriter, r *http.Request) (servedURLs, bool) {
	w.Header().Add("Vary", IngressPathHeader)
	urls, err := h.servedURLs(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return servedURLs{}, false
	}
	return urls, true
}

func (h *handler) externalEndpoint(endpoint string) string {
	if endpoint == "" {
		return ""
	}
	return h.opts.ExternalPrefix + endpoint
}

func validPortalRequestPath(u *url.URL) bool {
	// An empty path is the historical standalone shell entry. Encoded path
	// separators and dot segments are never cleaned or redirected by FileServer.
	return u.RawPath == "" && (u.Path == "" || validPortalPath(u.Path))
}

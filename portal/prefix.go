package portal

import (
	"errors"
	"net/url"
	"strings"
)

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
		return errors.New("invalid external prefix")
	}
	if !validPortalPath(opts.BasePath) || !strings.HasSuffix(opts.BasePath, "/") {
		return errors.New("invalid portal directory mount")
	}
	for _, endpoint := range []string{opts.GraphQLPath, opts.PortalCatalogPath, opts.SnapshotPath, opts.SubscriptionPath, opts.MCPPath, opts.EEBusAdminPath} {
		if endpoint != "" && (!validPortalPath(endpoint) || strings.HasSuffix(endpoint, "/")) {
			return errors.New("invalid gateway endpoint")
		}
	}
	return nil
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

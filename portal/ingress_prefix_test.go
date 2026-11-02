package portal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// fixtureIngressPrefix has the shape the Home Assistant Supervisor sends; the
// token is synthetic.
const fixtureIngressPrefix = "/api/hassio_ingress/fixture-token_0"

var (
	shellBasePattern    = regexp.MustCompile(`<base href="([^"]*)"`)
	shellGraphQLPattern = regexp.MustCompile(`data-graphql-path="([^"]*)"`)
)

func ingressRequest(h http.Handler, target string, ingress ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	for _, value := range ingress {
		r.Header.Add(IngressPathHeader, value)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func varyIncludesIngress(w *httptest.ResponseRecorder) bool {
	for _, value := range w.Header().Values("Vary") {
		for _, name := range strings.Split(value, ",") {
			if http.CanonicalHeaderKey(strings.TrimSpace(name)) == IngressPathHeader {
				return true
			}
		}
	}
	return false
}

type ingressBootstrap struct {
	Endpoints map[string]string `json:"endpoints"`
	Read      struct {
		BasePath string `json:"base_path"`
	} `json:"portal_read"`
}

func decodeIngressBootstrap(t *testing.T, w *httptest.ResponseRecorder) ingressBootstrap {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", w.Code, w.Body.String())
	}
	var out ingressBootstrap
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestIngressPrefixBuildsEveryServedURL serves the handler the way production
// mounts it (StripPrefix of the mount after the proxy stripped its prefix) and
// varies each input of the served base: the Ingress header (mutation: header
// ignored), the configured ExternalPrefix (mutation: prefixes concatenated in
// the wrong order or one dropped) and the mount (mutation: base built from
// the request path instead of the configured mount).
func TestIngressPrefixBuildsEveryServedURL(t *testing.T) {
	for _, tc := range []struct{ name, prefix, mount string }{
		{"portal_mount", "", "/portal/"},
		{"configured_prefix", "/console", "/portal/"},
		{"custom_mount", "", "/ui/cards/"},
		{"root_mount", "", "/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := NewHandler(Options{
				ExternalPrefix: tc.prefix, BasePath: tc.mount, EEBusAdminPath: "/admin/eebus/v1",
				ListRegistry: func() []RegistryDevice { return nil },
			})
			mountDir := strings.TrimSuffix(tc.mount, "/")
			served := http.Handler(handler)
			if mountDir != "" {
				served = http.StripPrefix(mountDir, handler)
			}
			for _, ingress := range []string{"", fixtureIngressPrefix} {
				headers := []string{}
				if ingress != "" {
					headers = append(headers, ingress)
				}
				prefix := ingress + tc.prefix
				base := prefix + tc.mount
				for _, entry := range []string{tc.mount, tc.mount + "index.html"} {
					shell := ingressRequest(served, entry, headers...)
					if shell.Code != http.StatusOK || !varyIncludesIngress(shell) {
						t.Fatalf("shell %s ingress=%q: status=%d vary=%v", entry, ingress, shell.Code, shell.Header().Values("Vary"))
					}
					if match := shellBasePattern.FindStringSubmatch(shell.Body.String()); len(match) != 2 || match[1] != base {
						t.Fatalf("shell base=%v; want %q", match, base)
					}
					if match := shellGraphQLPattern.FindStringSubmatch(shell.Body.String()); len(match) != 2 || match[1] != prefix+"/graphql" {
						t.Fatalf("shell GraphQL path=%v; want %q", match, prefix+"/graphql")
					}
				}

				direct := decodeIngressBootstrap(t, ingressRequest(served, tc.mount+"api/v1/bootstrap"))
				result := ingressRequest(served, tc.mount+"api/v1/bootstrap", headers...)
				if !varyIncludesIngress(result) {
					t.Fatalf("bootstrap Vary=%v", result.Header().Values("Vary"))
				}
				bootstrap := decodeIngressBootstrap(t, result)
				if bootstrap.Read.BasePath != base {
					t.Fatalf("portal_read.base_path=%q; want %q", bootstrap.Read.BasePath, base)
				}
				if len(bootstrap.Endpoints) != len(direct.Endpoints) || bootstrap.Endpoints["eebus_admin"] == "" {
					t.Fatalf("endpoint set changed: %v", bootstrap.Endpoints)
				}
				for name, value := range direct.Endpoints {
					if value == "" {
						if bootstrap.Endpoints[name] != "" {
							t.Errorf("disabled %s became %q", name, bootstrap.Endpoints[name])
						}
						continue
					}
					if want := ingress + value; bootstrap.Endpoints[name] != want {
						t.Errorf("%s=%q; want %q", name, bootstrap.Endpoints[name], want)
					}
					if !strings.HasPrefix(bootstrap.Endpoints[name], prefix+"/") {
						t.Errorf("%s=%q escapes served prefix %q", name, bootstrap.Endpoints[name], prefix)
					}
				}

				if mountDir == "" {
					continue
				}
				// The bare mount reaches the handler unstripped in production.
				redirect := ingressRequest(handler, mountDir+"?q=fixture%2Fvalue", headers...)
				if redirect.Code != http.StatusMovedPermanently || !varyIncludesIngress(redirect) {
					t.Fatalf("bare mount status=%d vary=%v", redirect.Code, redirect.Header().Values("Vary"))
				}
				if location := redirect.Header().Get("Location"); location != base+"?q=fixture%2Fvalue" {
					t.Fatalf("bare mount Location=%q; want %q", location, base+"?q=fixture%2Fvalue")
				}
			}
		})
	}
}

// TestIngressPrefixRejectsMalformedHeader varies the header value and count.
// Mutations caught: no validation (open or ambiguous URLs written back),
// repeated values joined or first-wins, and the value echoed in the error.
func TestIngressPrefixRejectsMalformedHeader(t *testing.T) {
	handler := NewHandler(Options{BasePath: "/portal/", ListRegistry: func() []RegistryDevice { return nil }})
	served := http.StripPrefix("/portal", handler)
	invalid := [][]string{
		{""}, {"relative"}, {"/"}, {fixtureIngressPrefix + "/"}, {"//outside.invalid"},
		{"https://outside.invalid"}, {"/a//b"}, {"/a/./b"}, {"/a/../b"}, {"/a%2fb"}, {"/a?x"},
		{"/a#x"}, {"/a\\b"}, {"/a b"}, {"/a, /b"}, {fixtureIngressPrefix, fixtureIngressPrefix},
	}
	for _, values := range invalid {
		for _, target := range []string{"/portal/", "/portal/index.html", "/portal/api/v1/bootstrap"} {
			w := ingressRequest(served, target, values...)
			if w.Code != http.StatusBadRequest || w.Header().Get("Location") != "" || !varyIncludesIngress(w) {
				t.Fatalf("header %q on %s: status=%d headers=%v", values, target, w.Code, w.Header())
			}
			for _, value := range values {
				if len(value) > 2 && strings.Contains(w.Body.String(), value) {
					t.Fatalf("error echoed header value %q", value)
				}
			}
		}
		w := ingressRequest(handler, "/portal?q=1", values...)
		if w.Code != http.StatusBadRequest || w.Header().Get("Location") != "" {
			t.Fatalf("header %q on bare mount: status=%d Location=%q", values, w.Code, w.Header().Get("Location"))
		}
	}
	// Responses that embed no URL never read the header.
	for _, target := range []string{"/portal/api/v1/health", "/portal/assets/app.js", "/portal/api/v1/timeline/events?limit=1"} {
		if w := ingressRequest(served, target, "relative"); w.Code != http.StatusOK {
			t.Fatalf("%s with malformed header: status=%d", target, w.Code)
		}
	}
}

// TestIngressPrefixNeverSelectsRoutes proves the header is not a routing or
// stripping input. Mutation caught: the handler strips the header value from
// the request path or treats it as a mount.
func TestIngressPrefixNeverSelectsRoutes(t *testing.T) {
	handler := NewHandler(Options{BasePath: "/portal/", ListRegistry: func() []RegistryDevice { return nil }})
	for _, target := range []string{fixtureIngressPrefix + "/portal/api/v1/health", fixtureIngressPrefix + "/portal/"} {
		if w := ingressRequest(handler, target, fixtureIngressPrefix); w.Code != http.StatusNotFound || w.Header().Get("Location") != "" {
			t.Fatalf("unstripped %s routed by header: status=%d", target, w.Code)
		}
	}
	for _, header := range []string{"/portal", "/portal/api"} {
		w := ingressRequest(handler, "/api/v1/health", header)
		plain := ingressRequest(handler, "/api/v1/health")
		if w.Code != http.StatusOK || plain.Code != http.StatusOK || w.Header().Get("Vary") != "" {
			t.Fatalf("health changed by header %q: status=%d vary=%v", header, w.Code, w.Header().Values("Vary"))
		}
	}
}

func TestIngressPrefixStandaloneShellStaysRelative(t *testing.T) {
	w := ingressRequest(NewHandler(Options{}), "/", fixtureIngressPrefix)
	if match := shellBasePattern.FindStringSubmatch(w.Body.String()); w.Code != http.StatusOK || len(match) != 2 || match[1] != "./" {
		t.Fatalf("standalone shell base=%v status=%d", match, w.Code)
	}
	if match := shellGraphQLPattern.FindStringSubmatch(w.Body.String()); len(match) != 2 || match[1] != fixtureIngressPrefix+"/graphql" {
		t.Fatalf("standalone shell GraphQL path=%v", match)
	}
}

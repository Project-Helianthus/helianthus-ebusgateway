package portal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

func m24Request(h http.Handler, target string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func m24Bootstrap(t *testing.T, h http.Handler, target string) (map[string]string, portalReadBootstrap) {
	t.Helper()
	w := m24Request(h, target)
	if w.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", w.Code, w.Body.String())
	}
	var out struct {
		Endpoints map[string]string   `json:"endpoints"`
		Read      portalReadBootstrap `json:"portal_read"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Endpoints, out.Read
}

func TestM24Routes(t *testing.T) {
	for _, tc := range []struct{ name, prefix, mount string }{
		{"root", "", "/"}, {"mount", "", "/portal/"},
		{"nested", "/console/site", "/portal/"}, {"custom_mount", "/console", "/ui/cards/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := tc.prefix + tc.mount
			h := NewHandler(Options{ExternalPrefix: tc.prefix, BasePath: tc.mount, ListRegistry: func() []RegistryDevice { return nil }})
			for _, entry := range []string{base, base + "index.html"} {
				w := m24Request(h, entry)
				if w.Code != http.StatusOK {
					t.Fatalf("shell %s: %d", entry, w.Code)
				}
				if !strings.Contains(w.Body.String(), `data-graphql-path="`+tc.prefix+`/graphql"`) {
					t.Fatal("shell GraphQL endpoint lost prefix")
				}
				for _, reference := range []string{`href="assets/app.css"`, `src="assets/app.js"`} {
					if !strings.Contains(w.Body.String(), reference) {
						t.Fatalf("shell missing relative asset reference %s", reference)
					}
				}
				match := regexp.MustCompile(`<base href="([^"]+)"`).FindStringSubmatch(w.Body.String())
				if len(match) != 2 || match[1] != base {
					t.Fatalf("shell base=%v; want %s", match, base)
				}
				document, err := url.Parse(entry)
				if err != nil {
					t.Fatal(err)
				}
				baseURL, err := document.Parse(match[1])
				if err != nil {
					t.Fatal(err)
				}
				for _, suffix := range []string{"assets/app.js", "assets/app.css", "api/v1/health", "api/v1/bootstrap", "api/v1/timeline/events?limit=1"} {
					resolved, err := baseURL.Parse(suffix)
					if err != nil {
						t.Fatal(err)
					}
					if resolved.String() != base+suffix {
						t.Fatalf("resolved %s; want %s", resolved, base+suffix)
					}
					result := m24Request(h, resolved.String())
					if result.Code != http.StatusOK {
						t.Fatalf("route %s: %d %s", resolved, result.Code, result.Body.String())
					}
				}
				for _, opaqueID := range []string{"asset:fixture/a?b&c#d", "asset:fixture/other", ""} {
					query := url.Values{"q": {opaqueID}}
					link, err := baseURL.Parse("api/v1/search?" + query.Encode())
					if err != nil {
						t.Fatal(err)
					}
					if link.Query().Get("q") != opaqueID || !strings.HasPrefix(link.Path, base) {
						t.Fatal("opaque query value or mount changed")
					}
					if result := m24Request(h, link.String()); result.Code != http.StatusOK {
						t.Fatalf("query status=%d", result.Code)
					}
				}
				stream, _ := baseURL.Parse("api/v1/stream?max_events_per_second=2&interval_ms=1000")
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				req := httptest.NewRequest(http.MethodGet, stream.String(), nil).WithContext(ctx)
				result := httptest.NewRecorder()
				h.ServeHTTP(result, req)
				if result.Code != http.StatusOK || result.Header().Get("Content-Type") != "text/event-stream" {
					t.Fatalf("stream status=%d headers=%v", result.Code, result.Header())
				}
			}
			if base != "/" {
				result := m24Request(h, strings.TrimSuffix(base, "/")+"?q=fixture%2Fvalue")
				if result.Code != http.StatusMovedPermanently || result.Header().Get("Location") != base+"?q=fixture%2Fvalue" {
					t.Fatalf("redirect=%d %q", result.Code, result.Header().Get("Location"))
				}
			}
			// The production host strips the mount after the proxy strips the prefix.
			stripped := http.StripPrefix(strings.TrimSuffix(tc.mount, "/"), h)
			if result := m24Request(stripped, tc.mount+"api/v1/bootstrap"); result.Code != http.StatusOK {
				t.Fatalf("stripped mount status=%d", result.Code)
			}
		})
	}
}

func TestM24EveryAdvertisedEndpoint(t *testing.T) {
	for _, field := range []string{"defaults", "graphql", "catalog", "snapshot", "subscriptions", "mcp", "admin"} {
		t.Run(field, func(t *testing.T) {
			opts := Options{BasePath: "/cards/", EEBusAdminPath: "/admin/eebus"}
			switch field {
			case "graphql":
				opts.GraphQLPath = "/query/custom"
			case "catalog":
				opts.PortalCatalogPath = "/catalog/custom"
			case "snapshot":
				opts.SnapshotPath = "/capture/custom"
			case "subscriptions":
				opts.SubscriptionPath = "/events/custom"
			case "mcp":
				opts.MCPPath = "/tools/custom"
			case "admin":
				opts.EEBusAdminPath = "/admin/custom"
			}
			root, rootRead := m24Bootstrap(t, NewHandler(opts), "/api/v1/bootstrap")
			opts.ExternalPrefix = "/console/site"
			nested, nestedRead := m24Bootstrap(t, NewHandler(opts), "/api/v1/bootstrap")
			if len(nested) != len(root) {
				t.Fatal("endpoint set changed")
			}
			for name, value := range root {
				want := value
				if want != "" {
					want = opts.ExternalPrefix + want
				}
				if nested[name] != want {
					t.Errorf("%s=%q; want %q", name, nested[name], want)
				}
			}
			if rootRead.BasePath != "/cards/" || nestedRead.BasePath != "/console/site/cards/" {
				t.Fatal("bootstrap base lost mount or prefix")
			}
			if nestedRead.StreamEndpoint != nil {
				t.Fatal("invented shared-read stream")
			}
		})
	}
	opts := Options{ExternalPrefix: "/console", BasePath: "/portal/"}
	endpoints, _ := m24Bootstrap(t, NewHandler(opts), "/api/v1/bootstrap")
	if endpoints["eebus_admin"] != "" {
		t.Fatal("disabled endpoint became prefix alone")
	}
}

func TestM24RejectMalformedConfiguration(t *testing.T) {
	for _, value := range []string{"relative", "/", "/console/", "//outside", "https://invalid", "/a//b", "/a/./b", "/a/../b", "/a%2fb", "/a?x", "/a#x", "/a\\b", "/a b", "/a\nb"} {
		t.Run("prefix_"+value, func(t *testing.T) {
			h := NewHandler(Options{ExternalPrefix: value})
			for _, target := range []string{"/", "/api/v1/bootstrap", "/assets/app.js"} {
				w := m24Request(h, target)
				if w.Code != http.StatusInternalServerError || w.Header().Get("Location") != "" {
					t.Fatalf("invalid config served or redirected: %d %v", w.Code, w.Header())
				}
			}
		})
	}
	for _, field := range []string{"mount", "graphql", "catalog", "snapshot", "subscriptions", "mcp", "admin"} {
		for _, value := range []string{"relative", "//outside", "/a//b", "/a/../b", "/a%2fb", "/a?x", "/a#x", "/a\\b"} {
			t.Run(field+"_"+value, func(t *testing.T) {
				opts := Options{}
				switch field {
				case "mount":
					opts.BasePath = value
				case "graphql":
					opts.GraphQLPath = value
				case "catalog":
					opts.PortalCatalogPath = value
				case "snapshot":
					opts.SnapshotPath = value
				case "subscriptions":
					opts.SubscriptionPath = value
				case "mcp":
					opts.MCPPath = value
				case "admin":
					opts.EEBusAdminPath = value
				}
				w := m24Request(NewHandler(opts), "/api/v1/bootstrap")
				if w.Code != http.StatusInternalServerError || w.Header().Get("Location") != "" {
					t.Fatalf("invalid %s accepted: %d", field, w.Code)
				}
			})
		}
	}
}

func TestM24RejectAmbiguousRequests(t *testing.T) {
	h := NewHandler(Options{ExternalPrefix: "/console", BasePath: "/portal/"})
	for _, target := range []string{"//outside", "/console//portal/", "/assets/../index.html", "/assets/./app.js", "/assets/%2e%2e/index.html", "/api%2fv1/bootstrap", "/assets/%5capp.js", "/assets/%252e%252e/index.html", "/console/portal/../", "/assets/app.js%3fx"} {
		t.Run(target, func(t *testing.T) {
			w := m24Request(h, target)
			if w.Code != http.StatusBadRequest || w.Header().Get("Location") != "" {
				t.Fatalf("ambiguous path served or redirected: %d %v", w.Code, w.Header())
			}
		})
	}
	for _, target := range []string{"/assets", "/assets/", "/cards/deep", "/console/portal/card", "/INDEX.HTML"} {
		w := m24Request(h, target)
		if w.Code != http.StatusNotFound || w.Header().Get("Location") != "" {
			t.Fatalf("non-entry path %s served or redirected: %d", target, w.Code)
		}
	}
}

func TestM24StandaloneAndCallerContext(t *testing.T) {
	for _, target := range []string{"/", "/index.html", "/assets/app.js", "/api/v1/bootstrap"} {
		if w := m24Request(NewHandler(Options{}), target); w.Code != http.StatusOK {
			t.Fatalf("standalone %s: %d", target, w.Code)
		}
	}
	type callerKey struct{}
	first, second := new(int), new(int)
	var expected any
	calls := 0
	h := NewHandler(Options{ExternalPrefix: "/console", BasePath: "/portal/", SemanticPVEnabled: true, SemanticPV: func(ctx context.Context) (ForwardedResponse, error) {
		calls++
		if got := ctx.Value(callerKey{}); got != expected {
			t.Error("caller context invented, lost or reused across requests")
		}
		return ForwardedResponse{Status: http.StatusOK, ContentType: "application/json", Body: []byte(`{}`)}, nil
	}})
	for _, caller := range []any{nil, first, second, nil} {
		expected = caller
		req := httptest.NewRequest(http.MethodGet, "/console/portal/api/v1/semantic/pv/current", nil)
		if caller != nil {
			req = req.WithContext(context.WithValue(req.Context(), callerKey{}, caller))
		}
		req.Header.Set("X-Forwarded-Prefix", "//outside")
		req.Header.Set("X-Forwarded-User", "fixture")
		req.Header.Set("Forwarded", "path=/spoof")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("read denied or lost: %d", w.Code)
		}
	}
	if calls != 4 {
		t.Fatalf("read calls=%d; want 4", calls)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil)
	req.Header.Set("X-Forwarded-Prefix", "/spoof")
	req.Header.Set("X-Forwarded-User", "fixture")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if strings.Contains(w.Body.String(), "spoof") || strings.Contains(w.Body.String(), "fixture") {
		t.Fatal("header-derived caller or path serialized")
	}
}

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

// ingressTestPrefix has the shape of the Supervisor Ingress path; the token is
// synthetic.
const ingressTestPrefix = "/api/hassio_ingress/fixture-token_0"

// portalDynamicRequestTargets lists every request target in the Portal bundle
// that is not a relative literal. Each one is fed by a bootstrap endpoint or a
// local relative literal; a new dynamic target must be classified here.
var portalDynamicRequestTargets = map[string]string{
	"endpoint":                           "bootstrap graphql or portal_catalog",
	"`${endpoint}?${params.toString()}`": "relative explorer read literal",
	"`${this._eebusAdminPath}${path}`":   "bootstrap eebus_admin",
}

var portalRequestCallPattern = regexp.MustCompile(`(?:\bfetch|\bnew EventSource)\(\s*`)

// portalRequestTargets returns the first argument of every fetch and
// EventSource call in the served Portal bundle.
func portalRequestTargets(t *testing.T, source string) []string {
	t.Helper()
	targets := []string{}
	for _, loc := range portalRequestCallPattern.FindAllStringIndex(source, -1) {
		rest := source[loc[1]:]
		if rest == "" {
			t.Fatal("truncated request call")
		}
		switch quote := rest[0]; quote {
		case '"', '\'', '`':
			end := strings.IndexByte(rest[1:], quote)
			if end < 0 {
				t.Fatalf("unterminated request target near %q", rest[:min(len(rest), 40)])
			}
			targets = append(targets, rest[:end+2])
		default:
			end := strings.IndexAny(rest, ",)")
			if end < 0 {
				t.Fatalf("unterminated request target near %q", rest[:min(len(rest), 40)])
			}
			targets = append(targets, strings.TrimSpace(rest[:end]))
		}
	}
	return targets
}

// staticRelativeTarget returns the literal part of a request target written
// relative to the Portal base, or false for a dynamic target.
func staticRelativeTarget(target string) (string, bool) {
	if len(target) < 2 || !strings.ContainsRune(`"'`+"`", rune(target[0])) {
		return "", false
	}
	literal := target[1 : len(target)-1]
	if target[0] == '`' {
		literal, _, _ = strings.Cut(literal, "${")
	}
	return literal, literal != ""
}

// TestPortalUnderIngressPrefixBuildsEveryRequestUnderPrefix serves the
// production Portal composition behind a simulated Supervisor Ingress proxy
// (strip the prefix, send it in portal.IngressPathHeader) and asserts that the
// redirect, the shell, its assets, every request target in the served bundle
// and every bootstrap endpoint the page uses resolve under the prefix and
// reach the gateway route they name once the proxy strips it.
func TestPortalUnderIngressPrefixBuildsEveryRequestUnderPrefix(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	var reached []string
	stub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = append(reached, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	// Stand-ins for the GraphQL and eeBUS admin routes the control plane
	// registers on the same listener.
	mux.Handle("/graphql", stub)
	mux.Handle("/admin/eebus/v1/", stub)
	registerHTTPPortalRoutes(mux, "/portal", portal.Options{
		GraphQLPath: "/graphql", SnapshotPath: "/snapshot", SubscriptionPath: "/graphql/subscriptions",
		MCPPath: "/mcp", EEBusAdminPath: "/admin/eebus/v1",
		ListRegistry: func() []portal.RegistryDevice { return nil },
	}, newGatewayPortalCatalogSource(nil, nil, nil, contributions))

	var escaped []string
	ingress := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest, ok := strings.CutPrefix(r.URL.Path, ingressTestPrefix)
		if !ok || !strings.HasPrefix(rest, "/") {
			escaped = append(escaped, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		forwarded := r.Clone(r.Context())
		forwarded.URL.Path, forwarded.URL.RawPath, forwarded.RequestURI = rest, "", ""
		forwarded.Header.Set(portal.IngressPathHeader, ingressTestPrefix)
		mux.ServeHTTP(w, forwarded)
	})
	page, err := url.Parse("https://ha.example.invalid" + ingressTestPrefix + "/portal")
	if err != nil {
		t.Fatal(err)
	}
	do := func(method string, target *url.URL, body string) *httptest.ResponseRecorder {
		t.Helper()
		if target.Host != page.Host {
			t.Fatalf("request %s leaves the page origin", target)
		}
		if !strings.HasPrefix(target.Path, ingressTestPrefix+"/") {
			t.Fatalf("request path %q is outside the Ingress prefix", target.Path)
		}
		w := httptest.NewRecorder()
		ingress.ServeHTTP(w, httptest.NewRequest(method, target.RequestURI(), strings.NewReader(body)))
		return w
	}

	entry := *page
	entry.RawQuery = "view=fixture"
	redirect := do(http.MethodGet, &entry, "")
	if redirect.Code != http.StatusMovedPermanently {
		t.Fatalf("bare mount status=%d", redirect.Code)
	}
	document, err := entry.Parse(redirect.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if document.Path != ingressTestPrefix+"/portal/" || document.RawQuery != "view=fixture" {
		t.Fatalf("bare mount redirect resolved to %s", document)
	}
	shell := do(http.MethodGet, document, "")
	match := regexp.MustCompile(`<base href="([^"]+)"`).FindStringSubmatch(shell.Body.String())
	if shell.Code != http.StatusOK || len(match) != 2 {
		t.Fatalf("shell status=%d body=%s", shell.Code, shell.Body.String())
	}
	base, err := document.Parse(match[1])
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(reference string) *url.URL {
		t.Helper()
		resolved, err := base.Parse(reference)
		if err != nil {
			t.Fatalf("resolve %q: %v", reference, err)
		}
		if !strings.HasPrefix(resolved.Path, ingressTestPrefix+"/portal/") {
			t.Fatalf("%q resolves to %s outside the served Portal directory", reference, resolved)
		}
		return resolved
	}

	assetBody := map[string]string{}
	for _, asset := range []string{"assets/app.js", "assets/app.css"} {
		w := do(http.MethodGet, resolve(asset), "")
		if w.Code != http.StatusOK {
			t.Fatalf("%s status=%d", asset, w.Code)
		}
		assetBody[asset] = w.Body.String()
	}
	bundle := assetBody["assets/app.js"]
	for _, api := range []string{"XMLHttpRequest", "WebSocket", "sendBeacon", "import(", "new Worker", "location.assign", "window.open"} {
		if strings.Contains(bundle, api) {
			t.Fatalf("bundle uses %s, which this request scan does not cover", api)
		}
	}
	targets := portalRequestTargets(t, bundle)
	if len(targets) < 30 {
		t.Fatalf("found only %d request targets; the scan no longer matches the bundle", len(targets))
	}
	dynamic := map[string]bool{}
	for _, target := range targets {
		literal, ok := staticRelativeTarget(target)
		if !ok {
			if _, known := portalDynamicRequestTargets[target]; !known {
				t.Fatalf("unclassified dynamic request target %s", target)
			}
			dynamic[target] = true
			continue
		}
		if strings.HasPrefix(literal, "/") || strings.Contains(literal, ":") {
			t.Fatalf("request target %s is not relative to the Portal base", target)
		}
		resolve(literal)
	}
	if len(dynamic) != len(portalDynamicRequestTargets) {
		t.Fatalf("dynamic request targets changed: %v", dynamic)
	}
	// The explorer read literals feeding `${endpoint}?...` are relative too.
	for _, literal := range regexp.MustCompile(`"(api/v1/explorer/read/[a-z0-9]+)"`).FindAllStringSubmatch(bundle, -1) {
		resolve(literal[1])
	}

	for _, entry := range []string{"api/v1/health", "api/v1/bootstrap", "api/v1/timeline/events?limit=1"} {
		if w := do(http.MethodGet, resolve(entry), ""); w.Code != http.StatusOK {
			t.Fatalf("%s status=%d", entry, w.Code)
		}
	}
	bootstrapResponse := do(http.MethodGet, resolve("api/v1/bootstrap"), "")
	var bootstrap struct {
		Endpoints map[string]string `json:"endpoints"`
		Read      struct {
			BasePath string `json:"base_path"`
		} `json:"portal_read"`
	}
	if err := json.Unmarshal(bootstrapResponse.Body.Bytes(), &bootstrap); err != nil {
		t.Fatal(err)
	}
	if bootstrap.Read.BasePath != base.Path {
		t.Fatalf("portal_read.base_path=%q; want %q", bootstrap.Read.BasePath, base.Path)
	}
	for name, value := range bootstrap.Endpoints {
		if value != "" && !strings.HasPrefix(value, ingressTestPrefix+"/") {
			t.Fatalf("bootstrap endpoint %s=%q is outside the Ingress prefix", name, value)
		}
	}
	endpoint := func(name string) *url.URL {
		t.Helper()
		target, err := base.Parse(bootstrap.Endpoints[name])
		if err != nil || bootstrap.Endpoints[name] == "" {
			t.Fatalf("bootstrap endpoint %s=%q: %v", name, bootstrap.Endpoints[name], err)
		}
		return target
	}
	catalog := do(http.MethodPost, endpoint("portal_catalog"), `{"operationName":"PortalCatalogV1"}`)
	if catalog.Code != http.StatusOK || !strings.Contains(catalog.Body.String(), `"portalCatalogV1"`) {
		t.Fatalf("catalog through Ingress status=%d body=%s", catalog.Code, catalog.Body.String())
	}
	do(http.MethodPost, endpoint("graphql"), `{"query":"{__typename}"}`)
	admin := endpoint("eebus_admin")
	admin.Path += "/status"
	do(http.MethodGet, admin, "")
	if want := []string{"POST /graphql", "GET /admin/eebus/v1/status"}; !slices.Equal(reached, want) {
		t.Fatalf("gateway routes reached through Ingress = %v; want %v", reached, want)
	}
	if len(escaped) != 0 {
		t.Fatalf("requests escaped the Ingress prefix: %v", escaped)
	}

	// The direct listener keeps root-relative URLs: no header, no prefix.
	direct := httptest.NewRecorder()
	mux.ServeHTTP(direct, httptest.NewRequest(http.MethodGet, "/portal", nil))
	if direct.Code != http.StatusMovedPermanently || direct.Header().Get("Location") != "/portal/" {
		t.Fatalf("direct bare mount status=%d Location=%q", direct.Code, direct.Header().Get("Location"))
	}
	direct = httptest.NewRecorder()
	mux.ServeHTTP(direct, httptest.NewRequest(http.MethodGet, "/portal/api/v1/bootstrap", nil))
	if !strings.Contains(direct.Body.String(), `"graphql":"/graphql"`) || !strings.Contains(direct.Body.String(), `"base_path":"/portal/"`) {
		t.Fatalf("direct bootstrap changed: %s", direct.Body.String())
	}
}

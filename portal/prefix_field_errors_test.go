package portal

import "testing"

func TestInvalidPortalPathField(t *testing.T) {
	fields := []struct {
		name string
		want string
		set  func(*Options, string)
	}{
		{"ExternalPrefix", "invalid portal path field: ExternalPrefix", func(o *Options, v string) { o.ExternalPrefix = v }},
		{"BasePath", "invalid portal path field: BasePath", func(o *Options, v string) { o.BasePath = v }},
		{"GraphQLPath", "invalid portal path field: GraphQLPath", func(o *Options, v string) { o.GraphQLPath = v }},
		{"PortalCatalogPath", "invalid portal path field: PortalCatalogPath", func(o *Options, v string) { o.PortalCatalogPath = v }},
		{"SnapshotPath", "invalid portal path field: SnapshotPath", func(o *Options, v string) { o.SnapshotPath = v }},
		{"SubscriptionPath", "invalid portal path field: SubscriptionPath", func(o *Options, v string) { o.SubscriptionPath = v }},
		{"MCPPath", "invalid portal path field: MCPPath", func(o *Options, v string) { o.MCPPath = v }},
		{"EEBusAdminPath", "invalid portal path field: EEBusAdminPath", func(o *Options, v string) { o.EEBusAdminPath = v }},
	}
	valid := Options{
		ExternalPrefix: "/fixture", BasePath: "/cards/", GraphQLPath: "/query",
		PortalCatalogPath: "/catalog", SnapshotPath: "/snapshot",
		SubscriptionPath: "/events", MCPPath: "/tools", EEBusAdminPath: "/admin",
	}
	if err := validatePortalPaths(valid); err != nil {
		t.Fatalf("valid controls: %v", err)
	}
	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			for _, value := range []string{
				"fixture-relative", "//fixture", "/fixture//entry", "/fixture/./entry",
				"/fixture/../entry", "/fixture%2fentry", "/fixture?query", "/fixture#fragment",
				"/fixture\\entry", "/fixture entry", "/fixture\nentry", "/fixture/é",
			} {
				opts := valid
				field.set(&opts, value)
				err := validatePortalPaths(opts)
				// Exact independent expectations reject generic/swapped labels and
				// any disclosure of the configured value, including appended text.
				if err == nil || err.Error() != field.want {
					t.Fatalf("malformed path: got %v; want %q", err, field.want)
				}
			}
			opts := valid
			if field.name == "BasePath" {
				for _, value := range []string{"", "/cards"} {
					field.set(&opts, value)
					if err := validatePortalPaths(opts); err == nil || err.Error() != field.want {
						t.Fatalf("invalid directory: got %v; want %q", err, field.want)
					}
				}
				field.set(&opts, "/")
				if err := validatePortalPaths(opts); err != nil {
					t.Fatalf("valid root directory: %v", err)
				}
			} else {
				field.set(&opts, "/fixture/")
				if err := validatePortalPaths(opts); err == nil || err.Error() != field.want {
					t.Fatalf("trailing slash: got %v; want %q", err, field.want)
				}
				field.set(&opts, "")
				if err := validatePortalPaths(opts); err != nil {
					t.Fatalf("empty optional field: %v", err)
				}
			}
		})
	}
}

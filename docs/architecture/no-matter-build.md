# Gateway build profiles

The default gateway build retains the Matter library option, feed projection,
feed routes and lifecycle composition. The `nomatter` build tag excludes those
implementations and their dependency closure at compile time:

```sh
go build -mod=readonly -tags nomatter ./cmd/gateway
python3 scripts/check_no_matter_profile.py
```

The checker requires a provisioned `GOMODCACHE` and runs offline dependency
listing for the actual gateway command under both profiles. It rejects a
Matter dependency in the excluded profile and requires Matter in the default
profile. Local CI and the GitHub CI test job run this guard after testing
both profiles; both CI suites also build and test the gateway with `nomatter`. The guard
checks production and test dependency closures. This covers package dependencies; symbol inspection is supplementary.

The excluded profile registers no Matter flag, host, feed route or projection.
The Portal reports Matter unavailable even when ordinary M2M GraphQL is enabled.
The M2M GraphQL route, TLS configuration, request budgets, native providers and
consumer shutdown order retain their ordinary behavior. The default profile
still fails closed when Matter library qualification inputs are unavailable.

Profile hooks accept only standard-library or gateway-owned types. Matter feed
and library host types remain inside files selected by `!nomatter`. Consumer
catalog coverage stays active in both profiles; only Matter-specific assertions
are selected with the enabled implementation.

This is an artifact-affecting structural change. Release, security and hardware
validation evidence must be renewed for the resulting artifacts. Offline
fixtures and dependency checks establish neither physical qualification nor
deployed acceptance. Full repository CI, independent review and applicable
transport/conformance gates remain shipping requirements. Add-on packaging and
release pins are separate work; this profile does not enable adoption or release.

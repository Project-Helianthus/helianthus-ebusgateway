package adversarial

import _ "embed"

//go:embed testdata/fixtures/positive/offline-all-pass.json
var allPassReport []byte

//go:embed testdata/fixtures/positive/evaluated-fail.json
var evaluatedFailReport []byte

//go:embed testdata/fixtures/positive/execution-error.json
var executionErrorReport []byte

//go:embed testdata/fixtures/positive/infrastructure-block.json
var infrastructureBlockReport []byte

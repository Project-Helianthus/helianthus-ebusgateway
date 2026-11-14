## Three Python test suites in the gate

The local gate and the pull request workflow now run the naming contract test, the dispatch documentation test and the package rename script test in the same pytest invocation as the metrics inventory test. No test, script or product file changes, so the gateway binary is unchanged.

# Slow-test inventory

This page records how long the gateway test suite takes, test by test, so that a
short-mode skip is added only for a test that was measured as slow.

## Rule

- GitHub CI runs the full suite, without `-short`, so every test still runs there.
- The local gate (`scripts/ci_local.sh`) runs the suite in short mode
  (`go test -race -short -count=1 -timeout 40m ./...`).
- A `testing.Short()` skip is added only for a test listed in the table below. The
  skip message names this page, and the test keeps all of its assertions.

## How it was measured

Measured on 2026-11-14 in a checkout of the repository, on one development host
with 12 logical CPUs that was running other work at the same time (load average
between about 9 and 22). The numbers come from one run each on one host. They show
which tests are slow relative to the others; they are not a benchmark.

```sh
GOWORK=off go test -short -count=1 -p 2 -json -timeout 40m ./... > short.json
GOWORK=off go test -race -short -count=1 -p 2 -json -timeout 40m ./... > short-race.json
```

| Run | Wall time | Packages | Summed package time | Tests of 1 s or more | 5 s or more | 10 s or more |
|---|---:|---:|---:|---:|---:|---:|
| short, without race | 361 s | 66 | 635.5 s | 141 | 23 | 7 |
| short, with race | 876 s | 66 | 1661.9 s | 297 | 56 | 21 |

Both runs passed. The times were taken before the short-mode skips below were
added. A test counts once, at its top level; subtests are included in that time.
Packages without test files are not listed.

The short race run of the gateway command package alone
(`GOWORK=off go test -race -short -count=1 -timeout 40m ./cmd/gateway`) took
675 s wall (go reported 642.1 s) before the skips and 568 s wall (go reported
555.9 s) after them, on the same host under a higher load (load average about 11
before and about 21 after). The difference includes the load change and is not a
precise measure of the skips alone.

Under the race detector the slowest gateway command tests are other tests than
without it (the Huawei binding-capacity tests); they still run in short mode.

## Per-package totals

| Package | Without race (s) | With race (s) |
|---|---:|---:|
| `./cmd/gateway` | 277.43 | 618.29 |
| `./internal/modbusadapter` | 20.24 | 193.89 |
| `./internal/adaptermux` | 174.55 | 175.28 |
| `./internal/runtimecontrol` | 42.70 | 120.29 |
| `./internal/semhub` | 20.63 | 72.82 |
| `./internal/coexistence` | 13.63 | 62.76 |
| `./graphql` | 8.22 | 61.32 |
| `./mcp` | 5.06 | 55.82 |
| `./internal/semanticview` | 3.66 | 39.46 |
| `.` | 11.87 | 29.28 |
| `./internal/semanticmetrics` | 3.75 | 28.74 |
| `./m2mgraphql` | 2.33 | 26.99 |
| `./internal/syncevidence` | 2.07 | 24.84 |
| `./internal/candidatefacts` | 2.47 | 23.98 |
| `./matter` | 1.86 | 18.98 |
| `./internal/semanticoperations` | 5.89 | 12.75 |
| `./internal/adversarialpackaging` | 8.45 | 5.86 |
| `./internal/runtimestate` | 8.20 | 7.03 |
| `./portal` | 4.22 | 5.84 |
| `./matter/feedv1` | 0.56 | 5.56 |
| `./internal/teslaobserve` | 3.39 | 4.73 |
| `./internal/eebusoutput` | 0.52 | 4.34 |
| `./internal/promotionlock` | 0.57 | 3.94 |
| `./internal/matrix` | 0.70 | 2.89 |
| `./internal/acceptancecatalog` | 0.49 | 2.82 |
| `./internal/adversarial` | 0.59 | 2.81 |
| `./internal/drivermanager` | 1.52 | 2.58 |
| `./internal/promotioncapture` | 0.31 | 2.49 |
| `./matter/libraryhost` | 0.37 | 2.43 |
| `./internal/adaptermux/v8classifier` | 1.12 | 2.10 |
| `./internal/portalconfig` | 0.74 | 1.90 |
| `./mcp/ebusstandard` | 0.24 | 1.66 |
| `./internal/eebusadmin` | 0.40 | 1.61 |
| `./portal/contributionv1` | 0.29 | 1.55 |
| `./internal/vaillant/b503session` | 0.56 | 1.53 |
| `./internal/m8sourcecapture` | 0.39 | 1.46 |
| `./internal/nmruntime` | 0.36 | 1.44 |
| `./internal/eebuscommand` | 0.26 | 1.42 |
| `./portal/catalogv1` | 0.23 | 1.39 |
| `./internal/ebusdscan` | 0.26 | 1.39 |
| `./internal/growattcanclassify` | 0.21 | 1.38 |
| `./internal/localworkerhost` | 0.20 | 1.36 |
| `./internal/m8sourcecapture/cmd/m8sourcecapture` | 0.22 | 1.36 |
| `./internal/energycalendar` | 0.20 | 1.34 |
| `./internal/vaillant/systemconfig` | 0.25 | 1.34 |
| `./mdns` | 0.21 | 1.32 |
| `./internal/candidatefacts/cmd/rawfirst` | 0.28 | 1.32 |
| `./ui` | 0.28 | 1.28 |
| `./internal/runtimecontrol/contractcheck` | 0.23 | 1.27 |
| `./internal/releasecheck` | 0.24 | 1.26 |
| `./internal/executionpolicy` | 0.20 | 1.25 |
| `./portalgraphql` | 0.24 | 1.25 |
| `./internal/ebus/evidence/discovery` | 0.21 | 1.25 |
| `./internal/ebus/evidence/identification` | 0.21 | 1.25 |
| `./cmd/matrix-runner` | 0.23 | 1.24 |
| `./cmd/growatt-can-classify` | 0.22 | 1.23 |
| `./internal/promotioncapture/cmd/leafpromotionartifacts` | 0.22 | 1.23 |
| `./cmd/ebusdscan` | 0.21 | 1.23 |
| `./internal/rpcsource` | 0.20 | 1.23 |
| `./internal/ebus/evidence/specimens` | 0.20 | 1.23 |

## Tests of 1 s or more

`Short mode` says whether the test runs or is skipped in short mode.

| Package | Test | Without race (s) | With race (s) | Short mode |
|---|---|---:|---:|---|
| `./cmd/gateway` | `TestHuaweiRepeatedReadmissionRefusesBeyondBindingCapacity` | 7.47 | 94.84 | runs |
| `./cmd/gateway` | `TestHuaweiSourceEpochCapacityExhaustionFailsStart` | 5.11 | 65.03 | runs |
| `./graphql` | `TestSemanticSnapshotProviderEdgesSerializedOverflow` | 5.57 | 47.38 | runs |
| `./cmd/gateway` | `TestH10TCPProviderPVSnapshotParity` | 43.21 | 47.04 | skipped |
| `./internal/runtimecontrol` | `TestIssue1375FailureCauseInvariantOverGeneratedText` | 1.43 | 32.61 | runs |
| `./internal/modbusadapter` | `TestAdapterPVConcurrentRefreshAndReadKeepsPublicViewAvailable` | 3.56 | 31.64 | runs |
| `./internal/modbusadapter` | `TestSunSpecProducerRetainsReferencedAccumulatorEvidenceAcrossPartialRefreshes` | 2.47 | 26.19 | runs |
| `./m2mgraphql` | `TestM2MRateLimitInFlightEntrySurvivesRotatingHostChurn` | 2.06 | 25.31 | runs |
| `./internal/modbusadapter` | `TestSunSpecProducerRefreshRetainsCurrentSemRegEvidenceWithBoundedEviction` | 2.39 | 25.25 | runs |
| `./internal/modbusadapter` | `TestSunSpecProducerProspectiveRefreshEvidenceCapacity` | 2.37 | 24.90 | runs |
| `./cmd/gateway` | `TestHTTPProviderCompositionAvailability` | 1.50 | 20.20 | runs |
| `./cmd/gateway` | `TestH10PartialGroupFailurePreservesFactFreshness` | 19.09 | 20.02 | skipped |
| `./internal/coexistence` | `TestMSP08EveryProtectedViewRejectsCandidateLeakAndConsumerDrift` | 1.59 | 19.16 | runs |
| `./mcp` | `TestTeslaGen3EVSESemanticPublicationPrometheusCurrentAtStaysCoherentDuringPublish` | 1.24 | 15.61 | runs |
| `./internal/adaptermux` | `TestIssue993_InjectsBoundedOfflineContentionCoverage` | 15.41 | 15.60 | runs |
| `./cmd/gateway` | `TestIssue1373FailedInitHandshakeDuringRecoveryIsRetriedUntilTheAdapterAnswers` | 11.32 | 11.57 | skipped |
| `./cmd/gateway` | `TestH11ComposedHuaweiPathOffline` | 5.38 | 11.46 | runs |
| `./internal/adaptermux` | `TestT02_ArbitrationExitStateMachine` | 11.17 | 11.38 | runs |
| `./internal/adaptermux` | `TestSessionPacer_NotificationArrivesDuringBacklogFlush` | 10.84 | 10.84 | runs |
| `./internal/semanticview` | `TestOutcomeReasonPrecedence` | 0.90 | 10.83 | runs |
| `./cmd/gateway` | `TestRuntimeControlDrainingTasksBlockReplacement` | 10.33 | 10.34 | runs |
| `./internal/runtimecontrol` | `TestO20FailedReservationRollback` | 0.52 | 9.40 | runs |
| `./cmd/gateway` | `TestRefreshRadioDevices_IncompleteCurrentFM5IdentityTupleRetainsPriorClassification` | 8.87 | 8.89 | runs |
| `.` | `TestQ04FrameBytePairDomain` | 1.52 | 8.78 | runs |
| `./internal/runtimecontrol` | `TestP11RestoreClockHighCoversRetainedIntents` | 1.36 | 8.32 | runs |
| `./internal/adversarialpackaging` | `TestCompiledPublisherRunsOutsideCheckoutWithoutPython` | 8.24 | 4.61 | runs |
| `./cmd/gateway` | `TestM38UnavailableValuesAreHonest` | 0.75 | 8.09 | runs |
| `./cmd/gateway` | `TestCX20ConcurrentSnapshotCountMatchesNativeObservation` | 4.35 | 7.80 | runs |
| `./internal/semanticview` | `TestP12CausalKeys` | 0.70 | 7.68 | runs |
| `./internal/semhub` | `TestSinceResumesOrRequiresResync` | 0.68 | 7.27 | runs |
| `./cmd/gateway` | `TestIssue743StablePublicSurfaceHasNoCandidateRawAliasOrTierSelector` | 0.26 | 6.81 | runs |
| `./internal/runtimecontrol` | `TestRecoveryCompactionCrashBeforeDuringAfterAndCapacity` | 1.26 | 6.78 | runs |
| `./internal/semhub` | `TestP06PriorExactAdmission` | 0.57 | 6.48 | runs |
| `./internal/coexistence` | `TestMSP08VerificationAndGenerationAreDeterministicAndDoNotMutateInputs` | 0.50 | 6.38 | runs |
| `./cmd/gateway` | `TestIssue1373ClosedEndpointRecoversOnSchedule` | 5.80 | 5.91 | runs |
| `./cmd/gateway` | `TestH10ThirdGenerationWithdrawsOldestRetainedForCapacity` | 0.49 | 5.89 | runs |
| `./cmd/gateway` | `TestControllerlessSitePollingOverSimulatedTime` | 5.21 | 5.87 | runs |
| `./internal/semhub` | `TestCommitLatencyIsBoundedWithAPermanentlyBlockedSubscriber` | 0.67 | 5.87 | runs |
| `./internal/modbusadapter` | `TestPVCharacterizationGenerationAndEpochReplacement` | 0.56 | 5.72 | runs |
| `./internal/candidatefacts` | `TestMSP07ClosedSchemaBoundsAndFirstErrorCategory` | 0.47 | 5.72 | runs |
| `./cmd/gateway` | `TestH11StorageAndMeterStayWithheldUnknownNeverZero` | 4.84 | 5.72 | runs |
| `./cmd/gateway` | `TestM38ConcurrentScrapesShareOneRuntime` | 0.61 | 5.59 | runs |
| `./internal/semanticmetrics` | `TestM36Quantities` | 0.48 | 5.57 | runs |
| `./cmd/gateway` | `TestControllerlessSiteReadinessAgainstBootDeadline` | 5.34 | 5.52 | runs |
| `./internal/modbusadapter` | `TestManagedReplacementRetainsRefreshEvidenceAcrossIdentities` | 0.53 | 5.42 | runs |
| `./internal/semanticoperations` | `TestM56RouterBoundsTheWait` | 5.09 | 5.37 | runs |
| `./cmd/gateway` | `TestH10MappingValuesAndWithheldFacts` | 4.80 | 5.27 | runs |
| `./cmd/gateway` | `TestControllerlessSiteControllerAppearsAfterAbsent` | 4.96 | 5.22 | runs |
| `./internal/semhub` | `TestActivateTimesOutWhileReprojectionKeepsFailing` | 5.03 | 5.21 | runs |
| `./cmd/gateway` | `TestRefreshRadioDevicesPreservesSnapshotsWhenRediscoveryOnlyTimesOut` | 5.16 | 5.16 | runs |
| `./cmd/gateway` | `TestM03SupersededStartupPreservesNativeFailureDuringStopRetry` | 0.48 | 5.14 | runs |
| `./cmd/gateway` | `TestM03ProvisionalNativeFailureRecordedBeforeSemanticCleanup` | 0.48 | 5.12 | runs |
| `./cmd/gateway` | `TestRun_AddressTableInserterWiresThroughPassiveReconstructor` | 5.07 | 5.09 | runs |
| `./internal/semhub` | `TestOverlappingResettleRunsOneReprojectionAttempt` | 5.01 | 5.07 | runs |
| `./internal/semhub` | `TestActivateReturnsSettleTimeoutForABlockingCallback` | 5.01 | 5.07 | runs |
| `./cmd/gateway` | `TestM03ManagerRecoversPartialActivationCleanupAfterJournalRecovery` | 0.46 | 5.02 | runs |
| `.` | `TestSourceSelectionBusAdapter_WarmupObservationSilentBusForwardsNothing` | 5.00 | 5.00 | runs |
| `./cmd/gateway` | `TestM03ManagerRetryAfterPartialActivation` | 0.46 | 4.92 | runs |
| `./cmd/gateway` | `TestIssue1373FailedInitHandshakeAtBootEntersRecoveryAndRuns` | 4.72 | 4.83 | runs |
| `./internal/semanticmetrics` | `TestM36Symbols` | 0.43 | 4.78 | runs |
| `./cmd/gateway` | `TestControllerlessSiteStartupWithoutController` | 4.74 | 4.77 | runs |
| `./graphql` | `TestM56GraphQLStructuralRefusalsNeverDispatch` | 0.73 | 4.67 | runs |
| `./internal/syncevidence` | `TestIssue764OneShotPublishesThenRepeatsAndRestartsWithoutAcquisition` | 0.32 | 4.46 | runs |
| `./matter` | `TestAA03TargetAgreement` | 0.40 | 4.37 | runs |
| `./cmd/gateway` | `TestM03PublicationScopeFencesOldEpoch` | 0.63 | 4.37 | runs |
| `./internal/modbusadapter` | `TestManagedPartialActivationRecoversSameEpoch` | 0.41 | 4.31 | runs |
| `.` | `TestK02RuntimeLabelDimensions` | 1.20 | 4.29 | runs |
| `./internal/modbusadapter` | `TestPVEvidenceTimezoneDigests` | 0.43 | 4.26 | runs |
| `./internal/runtimecontrol` | `TestO20AmbiguousReservationRetained` | 0.23 | 4.20 | runs |
| `./internal/modbusadapter` | `TestManagedInheritedReplayCoverDoesNotRepublish` | 0.41 | 4.18 | runs |
| `./cmd/gateway` | `TestM65RestartedDriverAdmitsOperationsOnItsSuccessor` | 1.08 | 4.12 | runs |
| `./cmd/gateway` | `TestM38EachScrapeUsesOneCoherentCapture` | 0.39 | 4.03 | runs |
| `./internal/coexistence` | `TestMSP08ReviewRejectsStableToolInventoryDrift` | 0.31 | 3.96 | runs |
| `./internal/modbusadapter` | `TestPVRejectedNewEpochDraftKeepsTheOldEpochWorking` | 0.39 | 3.89 | runs |
| `./internal/modbusadapter` | `TestPVPublicationCoreFencesGenerationAndRetiresEpoch` | 0.39 | 3.89 | runs |
| `./internal/modbusadapter` | `TestManagedWithdrawalRecoversAfterPartialSourceFencing` | 0.39 | 3.88 | runs |
| `./internal/modbusadapter` | `TestPVPendingRetirementIsRetriedBeforeRequalification` | 0.39 | 3.88 | runs |
| `.` | `TestM54AbsoluteMetricSeriesMatrix` | 1.62 | 3.85 | runs |
| `./internal/runtimecontrol` | `TestRecoveryAmbiguousRecorderCommitArmsReadback` | 0.71 | 3.84 | runs |
| `./cmd/gateway` | `TestM03StartupWorkerFailureQuarantinesWhileSemanticWithdrawalBlocked` | 2.18 | 3.80 | runs |
| `./internal/runtimecontrol` | `TestRecoveryStreamHasNoReceiptsAndStaysBounded` | 0.57 | 3.80 | runs |
| `./cmd/gateway` | `TestM03StartupEndpointFailureQuarantinesWhileSemanticWithdrawalBlocked` | 2.18 | 3.79 | runs |
| `./cmd/gateway` | `TestCharacterizationConsumersOfAllThreeFamilies` | 0.36 | 3.77 | runs |
| `./cmd/gateway` | `TestM38FailedCaptureNeverReusesAnEarlierScrape` | 0.37 | 3.75 | runs |
| `./internal/adaptermux` | `TestEscapePair_TCPEnhVectorsComplete` | 3.70 | 3.74 | runs |
| `./internal/modbusadapter` | `TestM03PartialActivationCleanupObligationsProgressIndependently` | 0.37 | 3.68 | runs |
| `./internal/coexistence` | `TestMSP08HarnessNegativeMutationMatrix` | 3.14 | 3.57 | runs |
| `./internal/syncevidence` | `TestIssue764RetainedLookupSurvivesRestartWithoutEntropyOrStaging` | 0.25 | 3.53 | runs |
| `./matter` | `TestAA03CanonicalValueAndReportAgreement` | 0.31 | 3.52 | runs |
| `./cmd/gateway` | `TestRefreshSystemDoesNotAcquireAPIIdentity` | 3.49 | 3.49 | runs |
| `./internal/runtimecontrol` | `TestServiceEventsFollowTheirView` | 3.44 | 1.88 | runs |
| `./cmd/gateway` | `TestM62BoilerUnavailableLeavesBoilerFieldsUnknownAndPublishesCircuit` | 3.36 | 3.37 | runs |
| `./internal/syncevidence` | `TestIssue764RetainedLookupConflictsOnMultipleCandidates` | 0.24 | 3.36 | runs |
| `./cmd/gateway` | `TestH10ManagedDriverStopRestartFencesHuaweiWork` | 0.29 | 3.35 | runs |
| `./internal/coexistence` | `TestMSP08PublicRedactionRejectsSecretsAndStableProtocolIdentity` | 0.30 | 3.31 | runs |
| `./cmd/gateway` | `TestM03SameEpochReplacementPublishesNextGeneration` | 0.34 | 3.27 | runs |
| `./internal/coexistence` | `TestMSP08CapturedRuntimePublicReplayProvesFourConnectedStates` | 2.11 | 3.24 | runs |
| `./internal/runtimecontrol` | `TestP11ReprojectionFailureKeepsDriverRecoverySuccessful` | 0.62 | 3.17 | runs |
| `./internal/coexistence` | `TestMSP08ReviewRejectsStructuredCandidateLeakKeyVariantsWithRecomputedEvidence` | 0.26 | 3.13 | runs |
| `./cmd/gateway` | `TestCharacterizationM2MGraphQLOfAllThreeFamilies` | 0.49 | 3.10 | runs |
| `./internal/modbusadapter` | `TestPVFailedGenerationReplacementKeepsThePreviousViewAndFencesNothing` | 0.30 | 3.05 | runs |
| `./cmd/gateway` | `TestPUBLIC05SunSpecSemRegSurvivesGatewayProvidersAndPortal` | 0.61 | 3.04 | runs |
| `./internal/adaptermux` | `TestSession_LegitimateENHClientNotClosedByDiagnostic` | 3.00 | 3.00 | runs |
| `./internal/coexistence` | `TestMSP08ReviewRejectsUnversionedEEBusPublicIdentifiersWithRecomputedEvidence` | 0.23 | 2.95 | runs |
| `./internal/adaptermux` | `TestArbitrationWinnerSynthesis_SkippedWhenExternalSessionWins` | 2.90 | 2.90 | runs |
| `./internal/adaptermux` | `TestArbitrationWinnerSynthesis_MultiSessionDeliversToNonWinner` | 2.90 | 2.90 | runs |
| `./internal/modbusadapter` | `TestPVCoverFailureAfterCommitRetainsTheStagedEvidence` | 0.30 | 2.86 | runs |
| `./cmd/gateway` | `TestRefreshConfigDHWOnlySuccessKeepsZonesCacheSource` | 2.83 | 2.83 | runs |
| `./internal/semhub` | `TestP06HistoricalControlAfterRestart` | 0.23 | 2.76 | runs |
| `./internal/modbusadapter` | `TestAdapterPVReadClampsWallRollbackToDetachedSnapshotFloor` | 0.27 | 2.73 | runs |
| `./internal/adaptermux` | `TestConnect_INITRetrySucceeds` | 2.70 | 2.70 | runs |
| `./cmd/gateway` | `TestO18RefreshDoesNotAcquireRawTemperature` | 2.69 | 2.69 | runs |
| `./internal/modbusadapter` | `TestAdapterPVCurrentSingleFollowsLatestIdentityRotation` | 0.27 | 2.67 | runs |
| `./cmd/gateway` | `TestM11StorageProviderAging` | 0.27 | 2.67 | runs |
| `./internal/modbusadapter` | `TestSunSpecProducerRetainsEvidenceForEveryCurrentIdentity` | 0.27 | 2.66 | runs |
| `./internal/adaptermux` | `TestSessionFrame_EnqueuedAtPropagatesThroughWriteLoop` | 2.65 | 2.65 | runs |
| `./cmd/gateway` | `TestM03StartupCleanupFencesPublicationWhileWorkCloseBlocks` | 0.25 | 2.63 | runs |
| `./cmd/gateway` | `TestM03SemanticCleanupRetriesAfterJournalRecovery` | 0.27 | 2.62 | runs |
| `./internal/adaptermux` | `TestBroadcastReset_ConcurrentFeatureUpdate` | 2.60 | 2.60 | runs |
| `./internal/adaptermux` | `TestSession_StartCancelWithoutOwnershipIsNoOp` | 2.60 | 2.60 | runs |
| `./internal/adaptermux` | `TestSession_StartCancelReleasesOwnership` | 2.60 | 2.60 | runs |
| `./internal/semanticmetrics` | `TestSlotFileStoreCapacityAndDetachedExports` | 0.89 | 2.60 | runs |
| `./internal/adaptermux` | `TestProxyListenerMultipleConnections` | 2.60 | 2.60 | runs |
| `./internal/adaptermux` | `TestSession_StartCancelSYN` | 2.60 | 2.60 | runs |
| `./internal/adaptermux` | `TestSession_ResetDeliveryIsLossless` | 2.60 | 2.60 | runs |
| `./cmd/gateway` | `TestMatterProfileCharacterizationFeed` | 0.28 | 2.59 | runs |
| `./cmd/gateway` | `TestM03StalledCompensatingFenceReportsFenceTimeout` | 0.24 | 2.58 | runs |
| `./cmd/gateway` | `TestM57LifecycleToolsReplaceGenerationsAndRefuseLatePublication` | 0.68 | 2.57 | runs |
| `./internal/syncevidence` | `TestIssue764RetainedLookupConflictsOnCloudContentMismatch` | 0.18 | 2.57 | runs |
| `./internal/adaptermux` | `TestProxyListenerAcceptsConnection` | 2.55 | 2.55 | runs |
| `./internal/adaptermux` | `TestSession_FailedCarriesInitiator` | 2.55 | 2.55 | runs |
| `./internal/adaptermux` | `TestSession_StartFailedByResetDeliversResetted` | 2.55 | 2.55 | runs |
| `./internal/adaptermux` | `TestProxyListenerContextCancel` | 2.55 | 2.55 | runs |
| `./cmd/gateway` | `TestM03PartialActivationCleanupRetriedAfterJournalRecovery` | 0.25 | 2.55 | runs |
| `./internal/adaptermux` | `TestSession_StartedCarriesInitiator` | 2.55 | 2.55 | runs |
| `./internal/adaptermux` | `TestSession_StartFailedByCollisionDeliversFailed` | 2.55 | 2.55 | runs |
| `./internal/adaptermux` | `TestAddSession_RejectsAfterShutdown` | 2.55 | 2.55 | runs |
| `./internal/adaptermux` | `TestMux_CloseAfterContextCancel` | 2.55 | 2.55 | runs |
| `./cmd/gateway` | `TestM61SuccessorGenerationPublishesStorageAfterSameEpochRestart` | 0.22 | 2.54 | runs |
| `./internal/adaptermux` | `TestSession_RawTCPDiagnosticFiresAfterFloodWithoutHandshake` | 2.52 | 2.50 | runs |
| `./cmd/gateway` | `TestWireAdapterDirect_SerialFormsReachTheMux` | 2.51 | 2.51 | runs |
| `./internal/adaptermux` | `TestProxyListenerBindError` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestBroadcastReset_ZeroFeaturesPassesThrough` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestSession_InitReturnsUpstreamFeatures` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestSession_StartedArrivesBeforeReceivedBytes` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestSessionRemoteAddrSideIndex` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestArbitrationWinnerSynthesis_DeliveredToExternalSession` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestMux_CloseConcurrent` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestBroadcastReset_MultipleSessionsAllGetFeatures` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestSession_SendWithoutOwnershipReturnsErrorHost` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestAddSession_AcceptsBeforeShutdown` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestSession_InitEchoesWhenUpstreamUnknown` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestBroadcastReset_CarriesUpstreamFeatures` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestProxyListenerCloseStopsAccepting` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestProxyListenerFatalAcceptDropsReadinessAndReportsOnce` | 2.50 | 2.50 | runs |
| `./internal/adaptermux` | `TestMux_CloseIdempotent` | 2.50 | 2.50 | runs |
| `./internal/runtimecontrol` | `TestServicePrunesOnlyAfterRecoveryMaterialSecured` | 2.47 | 2.48 | runs |
| `./cmd/gateway` | `TestM03WithdrawalIndependentOfNativeShutdown` | 0.24 | 2.46 | runs |
| `./internal/modbusadapter` | `TestPVCharacterizationFirstIncarnationLegacyIdentityAndCanonicalSnapshots` | 0.25 | 2.44 | runs |
| `./matter/feedv1` | `TestConcurrentCaptureKeepsOneCursor` | 0.16 | 2.38 | runs |
| `./cmd/gateway` | `TestH10FenceIsolationSuccessorAndLatePublication` | 0.21 | 2.35 | runs |
| `./internal/candidatefacts` | `TestMSP07RawFirstBuildConsumesM625PathsAndCloudObservation` | 0.18 | 2.33 | runs |
| `./internal/runtimestate` | `TestControlConcurrentCacheAndControlWrites` | 2.33 | 1.34 | runs |
| `./cmd/gateway` | `TestM61ManagedSidecarRestartRepublishesStorage` | 0.20 | 2.30 | runs |
| `./cmd/gateway` | `TestSunSpecLiveSmokeDecisionThroughRealQualificationPath` | 0.27 | 2.30 | runs |
| `./cmd/gateway` | `TestFM5PositiveIdentityDoesNotRequireNegativeNamespaceCompleteness` | 2.30 | 2.30 | runs |
| `./cmd/gateway` | `TestTeslaObserveWorksheetFailurePreservesOutputPair` | 2.27 | 2.21 | runs |
| `./cmd/gateway` | `TestEnqueueAddressIdentityProbe_SaturatedQueueCoalescesGuaranteedOrderedFM5Recovery` | 2.25 | 2.25 | runs |
| `./internal/semanticmetrics` | `TestM36Unavailable` | 0.20 | 2.24 | runs |
| `./mcp` | `TestRuntimeControlStrictDecoding` | 0.13 | 2.24 | runs |
| `./internal/semanticmetrics` | `TestSlotFileStoreIdentity` | 0.55 | 2.22 | runs |
| `./internal/runtimecontrol` | `TestIssue1371MalformedRecoveringStartFailsClosed` | 2.22 | 2.16 | runs |
| `./internal/coexistence` | `TestMSP08HarnessFailsClosedWithoutPartialReport` | 2.04 | 2.21 | runs |
| `./internal/runtimecontrol` | `TestP11DurableRestoreHonorsScopedInstallRefusal` | 0.40 | 2.13 | runs |
| `./internal/modbusadapter` | `TestAdapterPVCurrentSingleAtRetriesNewerSameAssetAtDetachedFloor` | 0.22 | 2.12 | runs |
| `./internal/candidatefacts` | `TestMSP07BuildVerifyReplayAreDeterministicAndCanonical` | 0.20 | 2.10 | runs |
| `./cmd/gateway` | `TestRefreshRadioDevices_CompleteNegativeFM5ScanSupersedesRetainedRegistryIdentity` | 2.09 | 2.10 | runs |
| `./internal/semanticview` | `TestM08CandidateStateDisposition` | 0.20 | 2.09 | runs |
| `./cmd/gateway` | `TestM38LabelsStayBoundedWithManyHubAssets` | 0.20 | 2.09 | runs |
| `./cmd/gateway` | `TestStartDiscoveryScanLoop_EbusdPreloadFailedRecoveryContinuesRestrictedScansUntilActiveSuccess` | 2.07 | 2.08 | runs |
| `./mcp` | `TestIssue755MutationToolsRejectPartialResultCategorically` | 0.14 | 2.07 | runs |
| `./cmd/gateway` | `TestM38ScrapeRouteServesTransportAndSemanticInventory` | 0.20 | 2.07 | runs |
| `./cmd/gateway` | `TestControllerlessSiteHeatPumpClassUnitAdoptedAsRoot` | 1.98 | 2.06 | runs |
| `./internal/modbusadapter` | `TestAdapterPVReadCapturesContextAfterDetachingSnapshot` | 0.21 | 2.06 | runs |
| `./matter` | `TestAA03SourceJoins` | 0.19 | 2.05 | runs |
| `./internal/adaptermux` | `TestIssue993_CollisionRecoveryCancelAfterActiveReadTimeout` | 2.05 | 2.05 | runs |
| `./internal/adaptermux` | `TestEscapePair_MissingEchoIsCountedAsTimeoutOfItsStep` | 2.01 | 2.01 | runs |
| `./internal/syncevidence` | `TestIssue764OneShotRetainedCloudMismatchConflictsWithoutAcquisition` | 0.15 | 2.00 | runs |
| `./internal/runtimecontrol` | `TestServicePublishedCursorEventsExist` | 2.00 | 1.52 | runs |
| `./internal/coexistence` | `TestMSP08ProductionHarnessConformance` | 1.87 | 1.97 | runs |
| `./internal/runtimecontrol` | `TestRecoveryS39WalGapResolvesPredecessorAndMissingIntentFailsSource` | 0.34 | 1.94 | runs |
| `./matter` | `TestAA03EvaluationJoins` | 0.18 | 1.90 | runs |
| `./internal/syncevidence` | `TestIssue764PublishedBundlesRemaskCloudSubjectPerBundle` | 0.15 | 1.89 | runs |
| `./internal/candidatefacts` | `TestMSP07RequiresSeparatelyVerifiedMSP065BundleAndReplay` | 0.15 | 1.87 | runs |
| `./cmd/gateway` | `TestIssue1375LossRecoveryPublishesTheMemberAtEachRetry` | 1.71 | 1.85 | runs |
| `./internal/semanticview` | `TestM08FivePacks` | 0.18 | 1.85 | runs |
| `./portal` | `TestO09PrefixedStreamFlushAndCancel` | 1.81 | 1.82 | runs |
| `./internal/runtimecontrol` | `TestRecoveryS39AcceptedPublicationsAndFence` | 0.34 | 1.80 | runs |
| `./internal/coexistence` | `TestMSP08CapturedRuntimeRejectsArtifactRestartRollbackAndSecretFaults` | 0.17 | 1.79 | runs |
| `./mcp` | `TestM10FivePackM09Parity` | 0.19 | 1.74 | runs |
| `./internal/semanticoperations` | `TestM56RouterProjectsDistinctEvidence` | 0.16 | 1.73 | runs |
| `./internal/semanticmetrics` | `TestM36BindingIdentity` | 0.15 | 1.72 | runs |
| `./internal/semanticview` | `TestM08IdentityFieldMutations` | 0.16 | 1.71 | runs |
| `./cmd/gateway` | `TestI41HistoryCanonicalBeforeCap` | 0.10 | 1.71 | runs |
| `./internal/adaptermux` | `TestEscapePair_SerialWrongEchoFailsAsCollisionLoggedAndCounted` | 1.71 | 1.71 | runs |
| `./cmd/gateway` | `TestRefreshRadioDevicesClearsStaleSnapshotsWhenDiscoveryFindsNoRefreshableSlots` | 1.69 | 1.70 | runs |
| `./cmd/gateway` | `TestDiscoverDeviceSlotsKeepsDisconnectedFunctionalModuleIdentityEvidence` | 1.69 | 1.70 | runs |
| `./cmd/gateway` | `TestDiscoverDeviceSlotsExcludesDisconnectedRegulatorAndThermostatSlots` | 1.69 | 1.69 | runs |
| `./internal/adaptermux` | `TestActivePath_SoakCycle_NoSustainedGrowth` | 1.68 | 1.68 | runs |
| `./cmd/gateway` | `TestM15FreshSystemFlowMirrorPublishesChangedDHWWithFailedB509` | 1.67 | 1.67 | runs |
| `./internal/adaptermux` | `TestIssue1401_MaxOwnershipDefaultIsLastResortGuard` | 1.66 | 1.66 | runs |
| `./cmd/gateway` | `TestIssue1369AdapterDirectStallHealsAfterFastAttemptsWithoutRestart` | 1.65 | 1.66 | runs |
| `./matter` | `TestAA03QualificationAndPrecision` | 0.15 | 1.62 | runs |
| `./cmd/gateway` | `TestM2MGraphQLRuntime_ServerTLSAndClosesListener` | 1.35 | 1.61 | runs |
| `./internal/coexistence` | `TestMSP08ClosedJSONOrderingAndLimitsFailClosed` | 0.13 | 1.58 | runs |
| `./mcp` | `TestRuntimeControlOutputsGolden` | 0.08 | 1.57 | runs |
| `./cmd/gateway` | `TestIssue1367AdapterDirectStallPastAM27RecoversThroughLoggedBackoff` | 1.52 | 1.57 | runs |
| `./internal/semanticview` | `TestM08EvaluationContextIdentity` | 0.15 | 1.56 | runs |
| `./internal/semanticview` | `TestM08FactKeyFields` | 0.15 | 1.56 | runs |
| `./cmd/gateway` | `TestIssue851AdapterDirectBroadcastPassiveBridgeRetiresWithGeneration` | 1.53 | 1.56 | runs |
| `./internal/coexistence` | `TestMSP08ReviewRejectsImpossibleUTCTimestamps` | 0.14 | 1.56 | runs |
| `./cmd/gateway` | `TestM38MalformedRegistryFailsComposition` | 0.15 | 1.56 | runs |
| `./internal/runtimecontrol` | `TestServiceStartSuccessAfterBudgetExpiredDuringHoldIsWithdrawn` | 1.55 | 1.51 | runs |
| `./cmd/gateway` | `TestM15UnqualifiedFlowOnlyCyclePublishesChangedDHW` | 1.54 | 1.55 | runs |
| `./cmd/gateway` | `TestM57RestartRejectsLateOperationCompletion` | 0.42 | 1.55 | runs |
| `./internal/modbusadapter` | `TestSunSpecProducerReportsRegistryFlavorEvaluationsInRegistryOrder` | 0.19 | 1.54 | runs |
| `./internal/semanticmetrics` | `TestO21RetirementExhaustionAndDetached` | 0.10 | 1.50 | runs |
| `./cmd/gateway` | `TestM57ComposedDriversShareOneNorthboundPath` | 0.28 | 1.48 | runs |
| `./internal/matrix` | `TestAdapterMuxV2ExplorerScanIsGuarded` | 0.29 | 1.48 | runs |
| `./internal/semanticview` | `TestOutcomeReasonSTD01` | 0.12 | 1.48 | runs |
| `./internal/runtimecontrol` | `TestRecoveryClockContinuityAfterProcessClockReset` | 0.22 | 1.45 | runs |
| `./cmd/gateway` | `TestRefreshSystem_FM5ConfigurationRequiresCurrentAcquisition` | 1.45 | 1.45 | runs |
| `./internal/semhub` | `TestExactCaptureAdmissionGates` | 0.11 | 1.43 | runs |
| `./cmd/gateway` | `TestM38RetiredSlotsLeaveSiblingsExported` | 0.14 | 1.42 | runs |
| `./cmd/gateway` | `TestM11MixedRevisionRejectedByBothHandlers` | 0.16 | 1.42 | runs |
| `./graphql` | `TestM09EveryRevisionComponent` | 0.24 | 1.41 | runs |
| `./internal/semhub` | `TestRecoveryNegativeCheckpointPreconditions` | 0.12 | 1.40 | runs |
| `./internal/modbusadapter` | `TestPUBLIC05InvalidFrequencyIsUnavailableWhilePowerRefreshes` | 0.14 | 1.38 | runs |
| `./internal/runtimecontrol` | `TestServiceStopOrRestartDuringStartingOrBackoffFencesAndKeepsCleanup` | 1.27 | 1.37 | runs |
| `./internal/adaptermux` | `TestEscapePair_SerialEnsVectorsComplete` | 1.32 | 1.36 | runs |
| `./internal/coexistence` | `TestMSP08M7InputsAreValidatedNotCallerAttributed` | 0.12 | 1.35 | runs |
| `./cmd/gateway` | `TestM2MGraphQLRuntime_ServesTLSToClientsWithAndWithoutCertificates` | 1.34 | 1.29 | runs |
| `./internal/runtimecontrol` | `TestRecoveryS39TwoAssetsOneAcceptedOneRejectedControl` | 0.22 | 1.32 | runs |
| `./cmd/gateway` | `TestIssue1371OperationBudgetBoundsOnlyTheOpeningAttempt` | 1.28 | 1.29 | runs |
| `./cmd/gateway` | `TestScanRequestShape_AdapterDirect` | 1.29 | 1.29 | runs |
| `./cmd/gateway` | `TestH10LostChildIdentityWithdrawsOnlyThatChild` | 0.11 | 1.29 | runs |
| `./mcp` | `TestRuntimeControlOutputsObeyTheContractDriverRule` | 0.08 | 1.28 | runs |
| `./internal/modbusadapter` | `TestPVPublicViewFollowsTheHubControlGate` | 0.12 | 1.26 | runs |
| `./cmd/gateway` | `TestM03StopFencesPublicationWhileInstallationBlocked` | 0.12 | 1.26 | runs |
| `./cmd/gateway` | `TestM57FailedAndWithheldSidecarsArePresentedTruthfully` | 0.32 | 1.26 | runs |
| `.` | `TestEEBusregV0118RuntimeAndGatewayBoundary` | 0.16 | 1.25 | runs |
| `./cmd/gateway` | `TestGrowattBMSRS485SemanticStorageSerializesConcurrentObservations` | 0.10 | 1.25 | runs |
| `./internal/runtimecontrol` | `TestRecoveryCorruptAssetAndChangedValidatorFailClosed` | 0.28 | 1.23 | runs |
| `./internal/modbusadapter` | `TestM03ManagedInjectedClockPublishesAndWithdraws` | 0.12 | 1.23 | runs |
| `.` | `TestTransportRuntimeMetricsScrapeIsStableDuringLifecycleReplacement` | 0.47 | 1.22 | runs |
| `./cmd/gateway` | `TestM03BoundSemanticGenerationSurvivesNativeRetry` | 0.12 | 1.22 | runs |
| `./mcp` | `TestRuntimeControlMCPServiceEquivalence` | 0.07 | 1.22 | runs |
| `./internal/adaptermux` | `TestMuxSerial_ReopensThePathAfterTheDeviceWentAway` | 1.20 | 1.21 | runs |
| `./internal/adaptermux` | `TestIssue1401_ActiveReadTimeoutReleasesAtOnce` | 1.21 | 1.21 | runs |
| `./internal/runtimecontrol` | `TestRecoveryS39LossBeforeHubReturnAndAfterAcceptance` | 0.24 | 1.21 | runs |
| `./cmd/gateway` | `TestH10ConcurrentSemanticFencesShareOneRetirement` | 0.12 | 1.20 | runs |
| `./internal/semanticview` | `TestM08IdentityRejection` | 0.11 | 1.20 | runs |
| `./matter` | `TestAA03ReportAccountingAndCurrentMetadata` | 0.11 | 1.20 | runs |
| `./graphql` | `TestM56GraphQLRefusalsProduceNoSend` | 0.19 | 1.19 | runs |
| `./internal/semanticoperations` | `TestM56RouterValidatesRequestBounds` | 0.11 | 1.18 | runs |
| `./internal/semanticoperations` | `TestM56RouterRefusalsProduceNoSend` | 0.11 | 1.16 | runs |
| `./cmd/gateway` | `TestM62BoilerPumpComesOnlyFromB509` | 1.14 | 1.16 | runs |
| `./mcp` | `TestMCPToolInventory` | 0.08 | 1.16 | runs |
| `./internal/modbusadapter` | `TestAdapterReevaluatesPVFreshnessAtEveryRead` | 0.12 | 1.15 | runs |
| `./internal/runtimecontrol` | `TestRecoveryS39MidCleanupKeepsAttemptsAndNativeCloseIndependent` | 0.29 | 1.15 | runs |
| `./internal/semanticview` | `TestM08OrderingAndUnavailable` | 0.11 | 1.15 | runs |
| `./internal/semanticview` | `TestM08PerSourceKeyAvailability` | 0.11 | 1.14 | runs |
| `./cmd/gateway` | `TestM06CapturesFollowAdmissionAfterConfigureAndRestart` | 0.12 | 1.14 | runs |
| `./internal/adaptermux` | `TestAbsorbReset_ContinuousRapidArmsDoNotLivelock` | 1.11 | 1.10 | runs |
| `./cmd/gateway` | `TestRecordBusAdmissionTransitionWithStabilityRefreshPublishesOneShotActive` | 1.10 | 1.10 | runs |
| `./cmd/gateway` | `TestIssue1371DownAtBootBacksOffThenRunsWhenTheEndpointAppears` | 0.87 | 1.10 | runs |
| `./internal/syncevidence` | `TestMSP065CanonicalFixturesPositiveReplayAndAllNegatives` | 0.07 | 1.09 | runs |
| `./cmd/gateway` | `TestIssue1432StopCancelsScheduledRetry` | 1.06 | 1.09 | runs |
| `./cmd/gateway` | `TestIssue1367ProxyClassTransportLossRecoversThroughSameLifecycleOwner` | 0.99 | 1.08 | runs |
| `./internal/semanticmetrics` | `TestO21LoadIdentity` | 0.07 | 1.07 | runs |
| `./internal/runtimecontrol` | `TestServiceTwoDriversStayResponsiveWhileOneBlocks` | 0.99 | 1.06 | runs |
| `./internal/adaptermux` | `TestIssue993_CollisionResyncSurvivesActiveReadTimeout` | 1.05 | 1.05 | runs |
| `./internal/modbusadapter` | `TestAdapterQualificationEvidenceStagesBeforePublicationAndRollsBack` | 0.12 | 1.04 | runs |
| `./internal/semhub` | `TestP06MissingFenceControl` | 0.09 | 1.04 | runs |
| `./cmd/gateway` | `TestControllerlessSiteHeatPumpBusControllerAppearsClearsReason` | 1.00 | 1.04 | runs |
| `.` | `TestNewGateway_InvalidAdmissionStabilityReturnsBeforeDial` | 0.01 | 1.04 | runs |
| `./internal/adaptermux` | `TestIssue993_NonCollisionActiveReadTimeoutStillTerminates` | 1.03 | 1.03 | runs |
| `./internal/modbusadapter` | `TestForeignSourceSunSpecPVBatchNamesOnlyOwnRecords` | 0.12 | 1.03 | runs |
| `./cmd/gateway` | `TestIssue851FatalProxyAcceptRecoversOnceAndRejectsStaleGeneration` | 1.03 | 1.03 | runs |
| `./internal/semanticmetrics` | `TestSlotFileStoreFencing` | 0.00 | 1.02 | runs |
| `./internal/modbusadapter` | `TestAdapterPublishesOneSemRegPVProjection` | 0.10 | 1.02 | runs |
| `./internal/adaptermux` | `TestAbsorbSkipLog_RateLimitedWithSuppressedSuffix` | 1.02 | 1.02 | runs |
| `./internal/adaptermux` | `TestTryGrantAndStart_ShutdownRace_NoAddAfterWait` | 1.01 | 0.02 | runs |
| `./internal/adaptermux` | `TestPopulateInfoCache_CachesResetInfoWhenBootloaderSupported` | 1.00 | 1.00 | runs |
| `./internal/coexistence` | `TestMSP08ReviewRejectsUnapprovedV1AliasInDebugWithRecomputedEvidence` | 0.08 | 1.00 | runs |
| `./internal/adaptermux` | `TestMuxSerial_StartOpensDeviceAtTheProtocolSpeed` | 1.00 | 1.00 | runs |
| `./internal/drivermanager` | `TestLifecycleHookLateAdmissionCloseReportCarriesCorrelation` | 1.00 | 1.00 | runs |
| `./cmd/gateway` | `TestIssue851ProxyReadinessWithdrawsAndRecoversWithAdmittedListener` | 1.00 | 1.00 | runs |

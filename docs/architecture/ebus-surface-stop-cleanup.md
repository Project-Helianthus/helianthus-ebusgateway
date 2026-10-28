# eBUS surface STOP cleanup

This note covers edge 1 of [issue #1263](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1263): a process-surface installation already inside a generation commit can outlast STOP's bounded fence wait.

The executor registers one cleanup continuation as joined work of the exact predecessor before fencing it. The continuation waits on the runner's existing serialization signal. Once an installation admitted before the fence has returned, it calls the bridge's bounded STOP once. No timer loop or independent retry owner is introduced. Repeated STOP calls share the recorded obligation and its result.

Predecessor matching checks driver ID, source epoch and driver generation. An explicit source ID must also match; an absent source ID retains the existing configured-source binding. STOP's operation sequence is separate from predecessor identity. The stored run key also retains its source, sequence and native-generation mapping; cleanup never selects a successor from a later bridge view.

While installation or cleanup is outstanding, the generation remains DRAINING and executor start and bridge replacement admission are refused. Native close and semantic retirement remain independent bridge obligations. A completed continuation is insufficient for admission: both native-close proof and semantic-cleanup proof must be present. An unproven close remains blocked, including after the task set joins. An initial empty StopOutcome remains unconfirmed; the continuation does not report durable service success. A later service STOP can observe the recorded proof.

Proven by focused synthetic offline cases: installation can finish after STOP's fence timeout and still receive cleanup without another STOP; native close held open keeps the generation draining; failed close keeps successor admission blocked; identity fields are tested independently. This is software evidence, separate from protocol qualification, freshness and physical acceptance. Full composed lifecycle behavior and physical transport acceptance remain Unknown here. No protocol meaning, MCP envelope, GraphQL exposure or stability promise changes; surfaces remain pre-stable before Helianthus 1.0.

The retained poller source rebind and passive reconstructor publication/close edges in issue #1263 remain outside this change. Actual transport/passive classification and full repository gates remain separate acceptance work.

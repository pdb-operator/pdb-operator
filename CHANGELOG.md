# Changelog

All notable changes to PDB Operator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Group-aware PDBs for LeaderWorkerSet (`leaderworkerset.x-k8s.io/v1`): multi-host inference workloads restart in gangs, so `minAvailable` is quantized to whole groups (`ceil(class% x replicas)` clamped to `replicas - 1`, times `size`). Single-group sets get no PDB plus a Warning event instead of a budget that would permanently block node drains; `size: 1` keeps plain pod-level semantics. The StatefulSet controller now skips LWS-internal StatefulSets (leader and per-group workers, detected by the `leaderworkerset.sigs.k8s.io/name` label) so a workload never matches multiple PDBs, which would make the eviction API fail outright. Support activates only when the LWS CRD is installed (#81)

### Security
- Bumped Go to 1.26.6 to resolve six standard library vulnerabilities reported by govulncheck: GO-2026-6218 (`net/url`), GO-2026-6091 (`html/template`), GO-2026-6090 (`crypto/tls`), GO-2026-6089 and GO-2026-5026 (`net/http`), GO-2026-5972 (`encoding/asn1`) (#79)

## [0.3.1] - 2026-07-11

### Changed
- Upgraded Kubernetes libraries to v0.36.2 and controller-runtime to v0.24.1, and migrated the API package off the deprecated `scheme.Builder` to the apimachinery `runtime.NewSchemeBuilder` registration pattern (#61)

### Security
- Bumped Go to 1.26.5 to resolve GO-2026-5856 (CVE-2026-42505) in the standard library `crypto/tls` (#67)

### Testing
- Bumped Ginkgo to v2.32.0 (#65) and Gomega to v1.42.1 (#64)

## [0.3.0] - 2026-06-27

### Added
- Proactive maintenance-window requeue: a workload with a configured window now wakes at the next window start (capped to an hourly heartbeat) so its PDB is relaxed on time without waiting for an unrelated event (#46)

### Changed
- Circuit-breaker latency percentiles sort via the standard library `slices.Sort` (O(n log n)) instead of a hand-rolled insertion sort (#47)

### Fixed
- Policy-level maintenance windows are now evaluated. Windows defined in a `PDBPolicy` (`spec.maintenanceWindows`) were silently ignored: the resolved configuration dropped them and only the workload `pdboperator.io/maintenance-window` annotation was parsed. Structured windows (timezone, `daysOfWeek`, multiple windows, and overnight spans) are now honored for both Deployments and StatefulSets (#45)
- `make deploy` no longer blocks `PDBPolicy` creation. The default kustomize config registered the admission webhooks but ran the manager without `--enable-webhook`, so every `PDBPolicy` write failed with "connection refused"; the default now enables the webhook server (#51)
- OpenTelemetry tracing now initializes instead of failing at startup. The `semconv` schema URL (1.39.0) conflicted with the SDK's `resource.Default()` (1.41.0), silently disabling tracing; the import is aligned to v1.41.0 (#52)
- The workload state tracker no longer swallows transient API errors. A failed PDB `Get` during change detection was treated as "PDB absent"; it now propagates so the reconciler requeues with backoff instead of acting on a stale fingerprint (#49)
- Corrected the misleading "Failed to remove finalizer" message logged while adding the `PDBPolicy` finalizer (#48)

### Testing
- Maintenance-window evaluation reads time through an injectable clock (`k8s.io/utils/clock`), making active/inactive window behavior deterministically testable (#50)

## [0.2.2] - 2026-06-21

### Fixed
- Grant `apps/deployments/finalizers` and `apps/statefulsets/finalizers` RBAC, required to set `blockOwnerDeletion` on PDB owner references on clusters that enforce ownerReference rules (e.g. OpenShift / `OwnerReferencesPermissionEnforcement`). Without it, PDB creation failed with "cannot set blockOwnerDeletion if an ownerReference refers to a resource you can't set finalizers on" (#41)

## [0.2.1] - 2026-06-21

### Fixed
- Deployment scaled below 2 replicas now cleans up its PDB instead of orphaning it, which previously blocked evictions, node drains and cluster-autoscaler on the surviving pod (#35)

### Testing
- e2e coverage for PDB cleanup on scale-down for both Deployments and StatefulSets (#36)
- De-flaked the time-dependent maintenance-window unit tests (#38)

## [0.2.0] - 2026-06-20

### Added
- StatefulSet support: new StatefulSetController providing the same policy-driven PDB management for StatefulSets (#14)
- Shared `WorkloadAccessor` abstraction unifying the PDB lifecycle across the Deployment and StatefulSet controllers
- `ManagedStatefulSets` Prometheus metric

### Changed
- DeploymentController now routes its PDB lifecycle through the shared workload helpers, removing ~390 lines of duplicated logic (#32)
- PDB-to-workload mappers now enqueue via the controller owner reference, eliminating cross-kind reconcile traffic (#14)
- `PDBPolicy` status `appliedToWorkloads` keys now include the workload kind (`namespace/Kind/name`) to avoid collisions between same-named Deployments and StatefulSets (#14)
- StatefulSet change-detection diagnostics dropped to `V(1)` to reduce production log noise (#31)
- Maintenance-mode annotation keys promoted to named constants (#31)

### Fixed
- A StatefulSet scaled below 2 replicas now cleans up its PDB instead of orphaning it (#14)
- `cleanupDuplicatePDBs` no longer deletes a PDB owned by another workload kind with an overlapping selector (#14)
- The StatefulSet finalizer is re-added if removed out-of-band, ensuring PDB cleanup on deletion (#14)

## [0.1.0] - 2026-03-01

### Added
- PDBPolicy CRD (`availability.pdboperator.io/v1alpha1`) with availability classes, enforcement modes, and maintenance windows
- PDBPolicyController for policy reconciliation and status management
- DeploymentController for automated PDB creation, update, and deletion
- Admission webhooks for PDBPolicy validation and defaulting
- Five availability classes: `non-critical` (20%), `standard` (50%), `high-availability` (75%), `mission-critical` (90%), `custom`
- Three enforcement modes: `strict`, `flexible`, `advisory`
- Workload function awareness with automatic security workload boosting
- Maintenance window support with timezone and day-of-week configuration
- Custom PDB configuration with `minAvailable`/`maxUnavailable` and `unhealthyPodEvictionPolicy`
- Policy priority system for conflict resolution
- Annotation-based overrides with optional reason requirement
- Prometheus metrics: reconciliation duration, PDB operations, compliance status, maintenance windows, enforcement decisions
- OpenTelemetry distributed tracing via OTLP protocol
- Structured JSON logging with audit trails and correlation IDs
- Kubernetes event recording for policy and PDB lifecycle events
- Circuit breaker pattern for Kubernetes API calls
- Policy caching layer for efficient reconciliation
- Retry logic with exponential backoff for API operations
- Leader election for high-availability deployments
- Health check endpoints (`/healthz`, `/readyz`)
- Secure metrics serving with TLS support
- Multi-architecture container images (amd64, arm64)
- CNCF governance files: LICENSE, CODE_OF_CONDUCT, CONTRIBUTING, GOVERNANCE, SECURITY, MAINTAINERS, ADOPTERS
- GitHub Actions CI/CD: unit tests, linting, e2e tests, DCO verification, container image releases
- GitHub issue and PR templates

### Technical Details
- Built with operator-sdk v1.42.0 and controller-runtime v0.23.1
- Go 1.26.0 with latest dependency versions
- Kubernetes client v0.35.2 compatibility
- gobreaker v2 with generics for circuit breaker
- OpenTelemetry v1.40.0 SDK (OTLP-only, no deprecated Jaeger exporter)
- Distroless container base image (`gcr.io/distroless/static:nonroot`)
- Comprehensive test suite with 71-93% coverage across packages

[Unreleased]: https://github.com/pdb-operator/pdb-operator/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/pdb-operator/pdb-operator/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/pdb-operator/pdb-operator/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/pdb-operator/pdb-operator/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/pdb-operator/pdb-operator/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/pdb-operator/pdb-operator/compare/v0.1.1...v0.2.0
[0.1.0]: https://github.com/pdb-operator/pdb-operator/releases/tag/v0.1.0

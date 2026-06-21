# Changelog

All notable changes to PDB Operator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/pdb-operator/pdb-operator/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/pdb-operator/pdb-operator/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/pdb-operator/pdb-operator/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/pdb-operator/pdb-operator/compare/v0.1.1...v0.2.0
[0.1.0]: https://github.com/pdb-operator/pdb-operator/releases/tag/v0.1.0

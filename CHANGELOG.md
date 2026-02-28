# Changelog

All notable changes to PDB Operator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- Kubernetes client v0.35.1 compatibility
- gobreaker v2 with generics for circuit breaker
- OpenTelemetry v1.40.0 SDK (OTLP-only, no deprecated Jaeger exporter)
- Distroless container base image (`gcr.io/distroless/static:nonroot`)
- Comprehensive test suite with 71-93% coverage across packages

[Unreleased]: https://github.com/pdb-operator/pdb-operator/commits/main

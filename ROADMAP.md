# Roadmap

This document outlines the planned development of PDB Operator. Priorities may shift based on community feedback and contributions.

## v0.1.0 - Initial Release

- [x] PDBPolicy CRD with availability classes and enforcement modes
- [x] Two-controller architecture (PDBPolicy + Deployment controllers)
- [x] Workload selector (labels, names, functions, namespaces)
- [x] Maintenance window support
- [x] Validating and mutating webhooks
- [x] Prometheus metrics and OpenTelemetry tracing
- [x] Grafana dashboards and alerting rules
- [x] HA deployment with leader election
- [x] CI/CD pipelines
- [ ] Helm chart
- [ ] Published container images (GHCR)
- [ ] Install manifests (`dist/install.yaml`)

## v0.2.0 - StatefulSet and DaemonSet Support

- [ ] Extend PDB management to StatefulSets
- [ ] Extend PDB management to DaemonSets
- [ ] Per-workload-type PDB calculation strategies

## v0.3.0 - Advanced Policy Features

- [ ] Namespace-scoped default policies
- [ ] Cluster-wide default policy
- [ ] Policy inheritance and composition
- [ ] Dry-run mode for policy evaluation without creating PDBs
- [ ] PDB drift detection and auto-remediation for manually modified PDBs

## v0.4.0 - Observability and Operations

- [ ] Operator health dashboard (built-in status endpoint)
- [ ] Policy compliance reports
- [ ] Audit log integration (external audit sink)
- [ ] PDB change history tracking on resource annotations

## Future Considerations

- Multi-cluster policy distribution
- Integration with cluster autoscaler for coordinated disruption management
- Custom availability class definitions via CRD
- Webhook-based PDB validation for non-managed PDBs
- OLM (Operator Lifecycle Manager) bundle and OperatorHub listing

## Contributing

Feature requests and feedback are welcome. Open an [issue](https://github.com/pdb-operator/pdb-operator/issues) or join the discussion to influence the roadmap.

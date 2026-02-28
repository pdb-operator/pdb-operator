# Contributing to PDB Operator

Thank you for your interest in contributing to PDB Operator! This document provides guidelines and information for contributors.

## Developer Certificate of Origin (DCO)

This project uses the [Developer Certificate of Origin (DCO)](https://developercertificate.org/). All commit messages must contain the `Signed-off-by` line with an email address that matches the commit author.

```bash
git commit -s -m "feat: add new feature"
```

If you forget to sign off, you can amend your last commit:

```bash
git commit --amend -s
```

## Getting Started

### Prerequisites

- Go 1.26+
- Docker or Podman
- kubectl
- [operator-sdk](https://sdk.operatorframework.io/docs/installation/) v1.42+
- A Kubernetes cluster (kind, minikube, or remote)
- [cert-manager](https://cert-manager.io/) (for webhook TLS)

### Development Setup

1. Fork and clone the repository:

```bash
git clone https://github.com/<your-username>/pdb-operator.git
cd pdb-operator
```

2. Install dependencies:

```bash
go mod tidy
```

3. Run tests:

```bash
make test
```

4. Run linter:

```bash
make lint
```

5. Build the operator:

```bash
make build
```

### Local Development with Kind

```bash
# Create a local cluster
make kind-create

# Build and load the image
make docker-build kind-load-images

# Deploy the operator
make deploy
```

## Making Changes

### Branching

- Create a feature branch from `main`
- Use descriptive branch names: `feat/add-policy-validation`, `fix/pdb-reconciliation-race`

### Coding Standards

- Follow standard Go conventions and idioms
- Run `make lint` before submitting
- Add tests for new functionality
- Update documentation for user-facing changes

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation changes
- `test:` adding or updating tests
- `refactor:` code refactoring
- `chore:` maintenance tasks

### Pull Request Process

1. Ensure all tests pass: `make test`
2. Ensure linter passes: `make lint`
3. Update relevant documentation
4. Sign off all commits (DCO)
5. Open a PR against `main`
6. Fill in the PR template
7. Wait for review from a maintainer

### Running Tests

```bash
# Unit tests
make test

# End-to-end tests (requires a running cluster)
make test-e2e

# Generate coverage report
make test COVERPROFILE=coverage.out
go tool cover -html=coverage.out
```

## Reporting Issues

### Bugs

Use the [bug report template](https://github.com/pdb-operator/pdb-operator/issues/new?template=bug_report.md) and include:

- Kubernetes version
- Operator version
- Steps to reproduce
- Expected vs actual behavior

### Feature Requests

Use the [feature request template](https://github.com/pdb-operator/pdb-operator/issues/new?template=feature_request.md) and describe:

- The problem you're trying to solve
- Your proposed solution
- Any alternatives you've considered

## Code of Conduct

This project follows the [CNCF Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

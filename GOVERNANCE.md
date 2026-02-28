# Governance

This document describes the governance model for the PDB Operator project.

## Principles

- **Open**: The project is open source and welcomes contributions from anyone.
- **Transparent**: All decisions are made in public (GitHub issues, discussions, PRs).
- **Merit-based**: Contributions and demonstrated commitment determine advancement in roles.

## Roles

### Contributors

Anyone who contributes to the project (code, documentation, issues, reviews).

### Reviewers

Contributors who have demonstrated:
- Understanding of the codebase and project goals
- Consistent, high-quality code reviews
- Active participation over a sustained period

Reviewers can approve PRs but cannot merge without a maintainer's approval.

**How to become a reviewer**: Nominated by a maintainer, approved by majority of maintainers.

### Maintainers

Maintainers have full commit access and are responsible for:
- Setting the project direction and roadmap
- Reviewing and merging pull requests
- Managing releases
- Triaging issues
- Ensuring the project adheres to its governance and code of conduct

Current maintainers are listed in [MAINTAINERS.md](MAINTAINERS.md).

**How to become a maintainer**: Nominated by an existing maintainer, approved by supermajority (2/3) of current maintainers. Candidates should have been active reviewers for at least 3 months.

## Decision Making

- **Lazy consensus**: Most decisions are made by lazy consensus. A proposal is considered accepted if no maintainer objects within 72 hours.
- **Voting**: For contentious issues, a vote may be called. Each maintainer gets one vote. Simple majority wins, except for governance changes which require supermajority (2/3).
- **Conflict resolution**: If consensus cannot be reached, the lead maintainer makes the final decision.

## Changes to Governance

Changes to this governance document require:
- A pull request with the proposed changes
- Supermajority (2/3) approval from maintainers
- A minimum review period of 7 days

## Code of Conduct

All participants must follow our [Code of Conduct](CODE_OF_CONDUCT.md).

# Security Policy

The PDB Operator project takes security seriously. We appreciate your efforts to responsibly disclose your findings.

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |

## Reporting a Vulnerability

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to **nonicked@protonmail.com**.

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

Please include the following information in your report:

- Type of issue (e.g., privilege escalation, RBAC bypass)
- Full paths of source file(s) related to the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

## Disclosure Timeline

- **Day 0**: Vulnerability reported
- **Day 1-2**: Initial response and acknowledgment
- **Day 7**: Assessment and fix development begins
- **Day 30**: Target for fix release (may vary based on complexity)
- **Day 90**: Public disclosure (coordinated with reporter)

## Scope

The following are in scope:

- The PDB Operator controller
- Admission webhooks
- RBAC configurations
- Container images published under `ghcr.io/pdb-operator/`

The following are out of scope:

- Vulnerabilities in upstream dependencies (report to upstream projects)
- Vulnerabilities in Kubernetes itself
- Issues in user-deployed configurations

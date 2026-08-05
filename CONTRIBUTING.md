# Contributing to Fivetran Community Connectors

Thank you for contributing. This repository is an open-source collection of connectors built with the [Fivetran Connector SDK](https://fivetran.com/docs/connectors/connector-sdk).

This guide explains how to prepare and submit a contribution.

## Ways to contribute

Community contributions may include:

- New connectors
- Bug fixes
- Performance or reliability improvements
- Documentation improvements
- Reusable Connector SDK patterns

Keep each pull request focused on one purpose. If a change combines unrelated functionality, refactoring, or multiple connectors, split it into separate pull requests where practical.

Community contributions should focus on new connectors and targeted improvements to
existing connectors. Repository-wide tooling, policy, or multi-connector changes are
accepted only when requested or explicitly approved in advance by a Fivetran
maintainer.

## Before you begin

1. Read the [Connector SDK documentation](https://fivetran.com/docs/connectors/connector-sdk).
2. Review similar connectors in this repository.
3. Install and configure the Connector SDK using the [setup guide](https://fivetran.com/docs/connector-sdk/setup-guide).
4. Fork and clone the repository.

Optionally, run `.github/scripts/setup-hooks.sh` from the repository root if you want staged Python files formatted automatically. The script configures a repository-local Git hook and may install Black if it is not already available.

For implementation guidance, refer to:

- [Python Coding Standards](https://github.com/fivetran/community_connectors/blob/main/PYTHON_CODING_STANDARDS.md)
- [Fivetran Coding Principles](https://github.com/fivetran/community_connectors/blob/main/FIVETRAN_CODING_PRINCIPLES.md)
- [Connector SDK Best Practices](https://fivetran.com/docs/connector-sdk/best-practices)
- [Template Connector](https://github.com/fivetran/community_connectors/tree/main/_template_connector)

## Prepare your contribution

### New connectors

Add each new connector in an appropriately named directory. A new connector must include:

- The connector implementation
- A `requirements.txt` or `pyproject.toml` file when the connector uses external dependencies
- A connector-level `README.md` based on the template connector
- Any other files required to configure or run the connector

Include only the README template sections relevant to the connector; concise documentation is sufficient when it explains how to configure and run the connector.

Also add the connector to the appropriate section of the repository's root `README.md`, with a short description and a link to its directory.

Do not include credentials, personal data, customer information, or other sensitive values in code, configuration, documentation, test data, or logs.

### Changes to existing connectors

Modify an existing connector when the change fixes or improves that connector. Create a separate connector when the contribution demonstrates a substantially different approach or use case.

Update the connector's `README.md` when a change affects its configuration, authentication, behavior, tables or other output, or run instructions.

Avoid unrelated cleanup. It makes the contribution harder to understand and review.

## Validate your contribution

Connector contributions must be tested before submission.

From the connector directory, run:

```bash
fivetran debug
```

If the connector uses a configuration file, run:

```bash
fivetran debug --configuration=configuration.json
```

Confirm that the command completes successfully and produces records. Include lightweight proof that the connector code ran, such as a screenshot of the successful output or a short relevant log excerpt. Remove credentials, personal data, and other sensitive information from the evidence.

You do not need to submit a detailed test report. Documentation-only and repository-maintenance changes do not require `fivetran debug` evidence; briefly describe any validation appropriate to the change.

## Submit a pull request

Create a branch from the latest `main` branch, commit your changes, and open a pull request against `fivetran/community_connectors:main`.

The pull request should include:

- A clear title
- What changed and why
- For connector code changes, lightweight, sanitized proof that the code ran, such as a screenshot or short log excerpt
- Any known limitations or context useful to the reviewer, if relevant

Keep the branch current and resolve merge conflicts when necessary.

## Acceptance criteria

A contribution may be accepted when all applicable criteria below are met:

- **One human approval:** Approval from one Fivetran reviewer.
- **Required checks pass:** All applicable automated checks pass, including formatting, linting, root README, and Contributor License Agreement checks.
- **Copilot review completed:** GitHub Copilot has reviewed the pull request, and the human reviewer has confirmed that every critical finding is addressed.
- **Validation evidence provided:** Connector changes include lightweight, sanitized proof of a successful run.
- **Repository requirements met:** Required connector files and documentation are present, and new connectors are listed in the root `README.md`.
- **Contribution is usable:** The code is not obviously broken and has no material flaw that would make the connector unsafe, unusable, or substantially incorrect.

The review is a minimum acceptance check, not an exhaustive validation or a search for the best possible implementation.

## Review process

One Fivetran reviewer evaluates both code and documentation.

Your contribution is reviewed for minimum acceptance, not for the best possible implementation. It should be approved when all applicable automated checks and repository requirements are met, validation evidence is present for connector code changes, Copilot review is complete, and no material blocker remains.

The reviewer is not expected to prove that every code path is correct, reproduce your environment, or redesign the connector.

### What may block approval

You may be asked to make changes when the reviewer identifies a material blocker, such as:

- Code that clearly cannot run or connect as described
- Incorrect schema, state, checkpointing, or data handling likely to cause missing, duplicated, or corrupted data
- A major reliability or scalability flaw
- Exposure of credentials, personal data, or other sensitive information
- An obvious security risk
- Missing required files, documentation, validation evidence, or failed required checks

The reviewer may ask questions to understand how your connector works or how you validated it. You will not be asked to rewrite working code unless the discussion reveals a material blocker.

### Copilot review

Copilot review is mandatory. A Copilot finding is critical only when the human reviewer determines that it meets the material-blocker criteria above, regardless of the severity or wording Copilot assigns to it.

You must address every critical finding before approval. You may address a finding by fixing it or explaining why it is not applicable or is a false positive; the human reviewer makes the final decision.

Non-critical Copilot comments are advisory. You do not need to respond to, resolve, or implement them.

### Non-blocking feedback

Feedback about the following is non-blocking:

- Personal style, naming, or wording preferences
- Formatting already covered by automated tools
- Optional refactoring or alternative implementations
- Additional features outside the contribution's stated scope
- Speculative edge cases unlikely to affect normal use

You are not expected to act on low-value or nit feedback from either human or AI review. Suggestions that may be useful but are not required for acceptance will be labeled **Non-blocking**. You may ignore them and do not need to acknowledge or resolve them.

### Acceptance decision

The approving reviewer makes the final acceptance decision. If all applicable criteria are met and no material blocker remains, your pull request should be approved and may be merged.

If your pull request is not accepted, the reviewer will identify the unmet criterion or material blocker clearly and concisely.

## Automated checks

Pull requests run the following checks when applicable:

### Python formatting and linting

Black checks formatting across the repository. Flake8 checks the Python files changed by the pull request. To run equivalent checks locally, use Black from the repository root and Flake8 on each changed Python file:

```bash
black --check --line-length 99 .
flake8 path/to/changed_python_file.py
```

To apply Black formatting, run `.github/scripts/fix-python-formatting.sh` from the repository root.

### Root README

A check verifies that new connector directories are documented in the root `README.md`.

### Contributor License Agreement

Contributors must sign the [Fivetran Contributor License Agreement](https://cla-assistant.io/fivetran/community_connectors). The CLA bot provides instructions on a contributor's first pull request; it only needs to be signed once.

## Code of conduct

This project follows the [Contributor Covenant Code of Conduct](https://github.com/fivetran/community_connectors/blob/main/CODE_OF_CONDUCT.md). By participating, you are expected to uphold it.

## Security vulnerabilities

Do not disclose a suspected security vulnerability in a public issue, pull request, or discussion. Report it privately to `security@fivetran.com` and follow the [Fivetran responsible disclosure policy](https://fivetran.com/docs/security-and-privacy/security#responsible-disclosure-policy).

## Need help?

Use pull request comments to ask questions or request clarification. For issues that cannot be resolved in the pull request, contact [Fivetran Support](https://support.fivetran.com/hc/en-us).

# Releasing crucible-ingestion to PyPI

`crucible-ingestion` publishes to public PyPI on a version tag, using PyPI Trusted Publishing (OpenID Connect). No API token is stored anywhere: PyPI verifies the GitHub Actions run by its OIDC identity (owner, repository, workflow, environment) and issues a short-lived upload credential for that run only.

The workflow is `.github/workflows/release.yml`. It fires on tags like `v0.2.0` (and PEP 440 pre-releases `v0.2.0a1`, `v0.2.0b1`, `v0.2.0rc1`), checks that the tag equals the `version` in `pyproject.toml`, builds the sdist and wheel with `uv build`, and publishes from a job bound to the `pypi` GitHub Environment.

## One-time setup (before the first publish)

Both steps are done once by whoever owns the PyPI project and this GitHub repository. Until they are done, a tag push builds but the publish step fails.

### 1. Create the GitHub Environment `pypi`

In this repository on GitHub: Settings -> Environments -> New environment -> name it exactly `pypi`. No secrets are needed. Optionally add a protection rule (for example a required reviewer, or restricting it to the `main` branch's tags) so a publish needs a human approval; the workflow already scopes `id-token: write` to the publish job only.

### 2. Add the PyPI pending publisher

The project does not exist on PyPI yet, so register a "pending" trusted publisher, which creates the project on the first successful publish.

On https://pypi.org (signed in as the account or organization that will own the project): Your account -> Publishing -> Add a new pending publisher, with these exact values:

| Field | Value |
| --- | --- |
| PyPI Project Name | `crucible-ingestion` |
| Owner | `MolecularFoundryCrucible` |
| Repository name | `crucible-ingestion` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

The owner, repository, workflow filename, and environment name must match the GitHub run exactly, or PyPI rejects the upload. If any of them later change, update the publisher on PyPI to match.

After the first publish succeeds the project exists; the same binding keeps working, and it can be managed from the project's own Settings -> Publishing page instead of the pending-publisher list.

## Dry run

Run the workflow by hand from the Actions tab (Release to PyPI -> Run workflow) on any branch to check that the package builds. A manual run never publishes: the publish job runs only for a tag.

## Cutting a release

1. Decide the version `X.Y.Z` (semver, independent of the `ingestion-cloud-consumer` harness version).
2. Set `version = "X.Y.Z"` in `pyproject.toml`, on a branch, and merge it the normal way.
3. From the merged commit on `main`, create and push the tag:

   ```bash
   git checkout main && git pull
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

4. The `Release to PyPI` workflow runs: it verifies the tag matches `pyproject.toml`, builds, and (after any environment approval) publishes `crucible-ingestion==X.Y.Z` to PyPI.
5. Confirm it is live: https://pypi.org/project/crucible-ingestion/X.Y.Z/ and `pip index versions crucible-ingestion`.

A tag whose version does not match `pyproject.toml` fails at the build job before anything is uploaded. PyPI never accepts the same version twice, so a botched release needs a new version number, not a re-tag.

## Note on the harness and ghcr

This file covers the library only. The `ingestion-cloud-consumer` harness publishes a container image and an OCI Helm chart to GitHub Container Registry (ghcr), which uses the run's `GITHUB_TOKEN` with `packages: write`, a different mechanism from PyPI trusted publishing. That setup lives in that repository, in a later phase of the deployment-integration plan.

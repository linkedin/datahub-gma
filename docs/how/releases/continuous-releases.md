# GMA Continuous Releases

We create [GitHub releases](https://github.com/linkedin/datahub-gma/releases) and publish our artifacts to
[Bintray](https://bintray.com/linkedin/maven/datahub-gma).

We use [shipkit-auto-version](https://github.com/shipkit/shipkit-auto-version),
[shipkit-changelog](https://github.com/shipkit/shipkit-changelog), and shipkit-github-release (part of
shipkit-changelog) to help create our continuous releases.

## Automatic Versioning

We follow the semantic versioning 2.0 scheme as defined at [https://semver.org](https://semver.org).

Versioning is determined by the [shipkit-auto-version plugin](https://github.com/shipkit/shipkit-auto-version). The
current version is defined in the [version.properties](../../../version.properties) file.

We automatically create a new release for every push of the master branch. We feel this simplifies development and makes
it clear what the latest state of the repository and releases are, as well as making it very easy for contributors to
pick up their changes once submitted. We do not currently support continuous beta releases.

Note that when we make a release, we also tag the commit with the version, making it easy to go back through the git
history to a specific version if needed.

## Automatic Changelog

We use shipkit changelog to create the release notes of each version. See
[shipkit-changelog](https://github.com/shipkit/shipkit-changelog) for more details.

## Automatic Publishing

Automation is done with a [GitHub workflow](../../.github/workflows/gh-version.yml) that will create the GitHub release
and then publish to bintray for every push of the `main` branch.

This workflow relies on the `BINTRAY_USER` and `BINTRAY_KEY` secrets, which should be a user on Bintray with permissions
to our project, and their api key, respectively. The exact user and key don't really matter, it is very easy to change
in the event that we need to.

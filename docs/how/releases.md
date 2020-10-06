# Releases

We create [GitHub releases](https://github.com/linkedin/datahub-gma/releases) and publish our artifacts to
[Bintray](https://bintray.com/linkedin/maven/datahub-gma).

We use shipkit to help create our releases and auto-increment our versions.

We automatically create releases for every commit on a branch that starts with `release/%semver%`, where semver is the
wildcard semantic version of that branch (e.g. `release/1.0.x`).

## Versioning

As stated above, we use shipkit for automatically creating version tags. See
[shipkit-auto-version for details](https://github.com/shipkit/shipkit-auto-version).

We follow the [semantic versioning 2.0.0 convention](https://semver.org/). When bumping the major or minor version,
please make a new `release` branch for it.

## Changelog

We use shipkit changelog to create the release notes of each version. See
[shipkit-changelog](https://github.com/shipkit/shipkit-changelog) for more details.

## Automation

Automation is done with a [GitHub workflow](../../.github/workflows/gh-version.yml) that will create the GitHub release
and then publish to bintray for every commit on a `release` branch.

This workflow relies on the `BINTRAY_USER` and `BINTRAY_KEY` secrets, which should be a user on Bintray with permissions
to our project, and their api key, respectively. The exact user and key don't really matter, it is very easy to change
in the event that we need to.

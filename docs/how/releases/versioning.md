# GMA Versioning

We follow the semantic versioning 2.0 scheme as defined at [https://semver.org](https://semver.org).

We have an automatic release pipeline as detailed [here](./continuous-releases.md). The current version is defined in
the [version.properties](../../../version.properties) file.

We automatically create a new release for every push of the master branch. We feel this simplifies development and makes
it clear what the latest state of the repository and releases are, as well as making it very easy for contributors to
pick up their changes once submitted. We do not currently support continuous beta releases.

Note that when we make a release, we also tag the commit with the version, making it easy to go back through the git
history to a specific version if needed.

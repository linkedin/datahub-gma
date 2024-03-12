# General Metadata Architecture

[![release](https://img.shields.io/github/v/release/linkedin/datahub-gma)](https://github.com/linkedin/datahub-gma/releases/)
[![build & test](https://github.com/linkedin/datahub-gma/workflows/build%20&%20test/badge.svg?branch=master&event=push)](https://github.com/linkedin/datahub-gma/actions?query=workflow%3A%22build+%26+test%22+branch%3Amaster+event%3Apush)
[![Get on Slack](https://img.shields.io/badge/slack-join-orange.svg)](https://join.slack.com/t/datahubspace/shared_invite/zt-dkzbxfck-dzNl96vBzB06pJpbRwP6RA)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/linkedin/datahub-gma/blob/master/docs/CONTRIBUTING.md)
[![License](https://img.shields.io/github/license/linkedin/datahub-gma)](LICENSE)

---

[Documentation](#documentation) | [Roadmap](docs/roadmap.md) | [FAQ](docs/faq.md)

---

> 📣 We've moved from Bintray to [Artifactory](https://linkedin.jfrog.io/artifactory/datahub-gma/)!
>
> As of version [0.2.45](https://github.com/linkedin/datahub-gma/releases/tag/v0.2.45), we are only publishing versions
> to LinkedIn's Artifactory instance rather than Bintray, which is approaching end of life.

## Introduction

General Metadata Architecture (GMA) is the backend for [DataHub](https://github.com/linkedin/datahub), LinkedIn's
generalized metadata search & discovery tool. To learn more about DataHub, check out its
[GitHub page](https://github.com/linkedin/datahub).

You should also visit [GMA Architecture](docs/architecture/architecture.md) to get a better understanding of how GMA is
implemented.

This repository contains the _partial_ source code for GMA. It originally lived in the same GitHub repository as
DataHub. We're still in the process of moving it all over.

## Documentation

- [GMA Developer's Guide](docs/developers.md)
- [GMA Architecture](docs/architecture/architecture.md)
- [GMA Source Code Diagrams](https://sourcespy.com/github/linkedindatahubgma/)

## Releases

See [Releases](https://github.com/linkedin/datahub-gma/releases) page for more details. We follow the
[SemVer Specification](https://semver.org) when versioning the releases and adopt the
[Keep a Changelog convention](https://keepachangelog.com/) for the changelog format.

## FAQs

Frequently Asked Questions about DataHub can be found [here](docs/faq.md).

## Features & Roadmap

Check out GMA's [Features](docs/features.md) & [Roadmap](docs/roadmap.md).

## Contributing

We welcome contributions from the community. Please refer to our [Contributing Guidelines](docs/CONTRIBUTING.md) for
more details. We also have a [contrib](contrib) directory for incubating experimental features.

## Community

Join our [slack workspace](https://join.slack.com/t/datahubspace/shared_invite/zt-dkzbxfck-dzNl96vBzB06pJpbRwP6RA) for
discussions and important announcements.

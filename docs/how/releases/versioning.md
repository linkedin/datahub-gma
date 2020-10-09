# GMA Versioning

We, for the most part, follow the semantic versioning 2.0 scheme as defined at [https://semver.org](https://semver.org).

We have an automatic release pipeline as detailed [here](./continuous-releases.md).

We create a new release for every commit published to a branch that starts with `release/`.

## Differences from semver 2.0

The only difference we have is that any beta releases must be in the format `major.minor.patch-tag.tagVersion` (tag
after the patch version, and its own tag version), e.g. `1.0.0-beta.15` would be the 15th patch of the beta for `1.0.0`.

| **number** | **meaning**                                                           |
| ---------- | --------------------------------------------------------------------- |
| major      | major version, backwards incompatible with the previous major version |
| minor      | minor version, backwards compatible with added features               |
| patch      | patch version, small bug fixes or stylistic improvements              |
| tag        | _optional_ beta release                                               |

So for non-beta releases, this is the same as semantic versioning 2.0.

We are applying a lesson that was learned on the Mockito project, and which you can read more about
[here](https://github.com/mockito/mockito/wiki/Continuous-Delivery-Details#lessons-learned).

## Release branch life cycle

Release branches are in the format of `release/major.x[-beta]`.

The life cycle of a release branch is as follows:

1. The initial branch is made and labeled as a beta branch. e.g. `release/1.x-beta`. When creating this branch, ensure
   the [version.properties](../../../version.properties) matches. We have a gradle check to enforce this, and the build
   will fail otherwise. Breaking changes from the previous major version can be checked into this branch. Beta releases
   built from this branch will be continuously released.
2. Once the branch is considered ready for final (non-beta) release, the branch is renamed to reflect the new version
   (e.g. `release/1.x`, with the `-beta` dropped) and the [version.properties](../../../version.properties) file
   updated. Note that this is indeed _renaming_ the branch, so the `release/1.x-beta` branch will no longer exist. Any
   new breaking changes will instead need to start this cycle again at step 1.
3. Old release branches can be deleted after some time when we are no longer commonly updated them. We can remake these
   branches in the future, should we need to make more patches, by making a branch at the last commit tagged with a
   release from that major version.

## What about non-breaking, but not final (beta) features?

First, any breaking changes warrant a major version bump and should follow the branch lifecycle above. Try to avoid
breaking changes where possible; consider the following workflow:

1. Add a new API (method, class, or interface). This isn't breaking and can be committed to the latest release branch.
2. Mark the old API(s) as deprecated. Use both the JavaDoc `@deprecated` (and document the replacement), as well as the
   `@Deprecated` annotation. Again, this isn't breaking and so can be committed to the latest releae branch.
3. In the next major release, we can consider deleting APIs marked as `@Deprected`.

When following this process, at step #1 your feature may not be final with your initial PR. You may need to add more to
it (submitting over several PRs), or you may wish to give it some time to incubate for considering it ready for release.
In this case you should _not_ submit directly to a release branch.

TODO figure out what we should do instead lol. Beta branches don't make much sense because they are supposed to contain
breaking changes, and really are meant to be a beta for that full release. If we shove incubating APIs there some might
get booted to the next beta come release, which is fine, but requires work to rip them out, so keeping incubating APIs
out will save us work long run. But separate feature branches are not great either, because we don't really want to
publish versions for them. We could suggest feature branches, and they'd work fine for
this-is-pretty-final-but-over-many-PRs. Wouldn't work for this-really-is-incubating-and-subject-to-change. We could just
say submit them to the latest release with some Java annotation (`@Beta` or `@Experimental`), but non are standard.

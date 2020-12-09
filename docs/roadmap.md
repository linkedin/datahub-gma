# GMA Roadmap

Status: **Draft**

GMA is currently on major version 0. This roadmap currently defines the requirements for a 1.0 release. It is currently
a draft, as we need some major discussions around what we want 1.0 to look like.

There are a few options of what we can do with a `1.0` release:

1. `1.0` is a ground up rewrite of our code. Developed in a fork and completely unrelated to our existing code.
2. `1.0` is still a rewrite, but is developed in the same branch. The last release of `0.x` contains the old and new
   code, making a transition to `1.0` easier. The primary issue here is naming; we can't reuse any names in `1.0` that
   exist today.
3. `1.0` is just a small cleanup of our existing APIs where there isn't too much onerous on a migration. Maybe we also
   add some more core features.

So really we have two questions to solve, regarding existing code and new features:

1. What APIs need fixing, if any, and do they warrant complete rewrites? If so, how do we handle those rewrites?
2. What additional features, if any, would we want to consider for a `1.0` release.

---

## Required `1.0` Features

List of features we know we want in a `1.0` release.

Status: **Draft**

- [ ] [Metadata Events v5](./what/mxev5.md) support.
  - [x] Auto generate event definitions from PDL annotations at build time with a gradle plugin.
  - [ ] Support for DAOs to emit MAE v5.
  - [ ] Support customizing kafka topic names / name patterns.
  - [ ] Jobs (or job libraries) to consume MCE v5 and update GMSes.
  - [ ] Jobs (or job libraries) to consume MAE v5 and update Elasticsearch.
  - [ ] Jobs (or job libraries) to consume MAE v5 and update Graph.
  - [ ] Kafka topic auto generation gradle plugin or script.
  - [ ] Migration playbook for users to get off v4.
- [ ] Enable `werror` for all Java code.
- [ ] Remove use of tuples library (not idiomatic Java - replace with helper classes / POJOs).
- [x] Elasticsearch 7 support.
- [ ] Java 11 support.

---

## Potential `1.0` Rewrites / Improvements / Features

Features / improvements we need to discuss further to see if they make sense on have a `1.0` release on.

- Kafka job or libraries are in GMA.
  - The kafka jobs did not move from the DataHub repo. They're still tightly coupled to MCE and MAE v4, which are in
    turn coupled to the models that live in DataHub.
  - We should add the jobs (or very easy to use libraries that can be invoked via a job) for MCE and MAE v5 for `1.0`.
  - Jobs include MCE consumer (consumes MCEs and updates GMSes) and MAE consumers for ElasticSearch and Graph.
- Index builder API should be type safe.
  - Index builders also did not move from the DataHub repo. Are needed for jobs.
  - The API is that you give the super constructor the list-of-snapshots you want to transform. But then the
    transformation method gives you a generic Snapshot. Nothing enforces you have to handle those things in the list you
    said you wanted to handle. We saw this lead to some confusion and a user did get it wrong once.
  - Fixed by making index builders listen to one thing only, and then having the transformation method argument be that
    type.
- Remove extensive use of abstract "Base" classes. Prefer interfaces.
  - Abstract classes are still okay, just not as the root of the inheritance tree.
  - Split classes / interfaces where it makes sense, especially to avoid `UnsupportedOperationException`.
- Improve / publish code coverage.
- Ensure all public methods and classes are documented with Java doc.
  - Add check to prevent regression.
- Remove json "search templates" and instead use something in code.
  - JSON is brittle and hard to get right. Java types make this easier. Elasticsearch already provides APIs for these.
  - Regex replacement is not correct unless we're very careful with escaping the string.
- Search type safety.
  - The input today is a string that has meaning to Elasticsearch. Can be unclear to the client as to how to build this.
  - The search model is not the same as the GMA model, which can lead to confusion.
- Rewrite DAOs for extensibility.
  - SCSI muddled the interface quite a bit, as not all DAOs support SCSI. Part of the issue is the DAO design was not
    extensible to allow SCSI without extending the interfaces.
- Codegen for restli GMSes.
  - Rest.li relies on Java annotations to build IDL files. Does not look at super class annotations. So many methods
    must be copied / pasted / overloaded just to get annotations. Good opportunity for some code gen.
- Codegen for URN.
  - The URN classes in DataHub are pretty much copy and paste. We could easily generate these classes.
- Deletion of references to snapshots and aspect unions.
  - MXE v5 helps to enabled this. Allows more modular GMSes. Also allows models to no longer be in a single repo.
- Rename test models for a more grounded example.
  - "Foo" and "Bar" have no meaning and can be kind of hard to read some times, and are just poor examples. Naga has had
    some fun "foodie" examples in past presentations, maybe we just use that kind of thing for our test models?

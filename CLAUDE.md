# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataHub GMA (General Metadata Architecture) is the backend framework for LinkedIn's DataHub metadata search & discovery platform. It provides type-safe, schema-driven metadata management with event-driven data consistency across multiple storage backends (SQL via Ebean, Elasticsearch for search). Built on LinkedIn's Pegasus data framework and Rest.li for REST APIs.

**Java version**: 1.8 (Java 8) — required, newer versions will cause build failures.

## Build Commands

```bash
./gradlew build                    # Full build + all tests
./gradlew :module-name:build       # Build a specific module (e.g., :dao-api, :dao-impl:ebean-dao)
./gradlew :module-name:test        # Run tests for a specific module
./gradlew spotlessCheck            # Check code formatting (CI enforced)
./gradlew spotlessApply            # Auto-fix formatting
./gradlew checkstyleMain           # Run checkstyle on main sources
./gradlew idea                     # Generate IntelliJ project files
```

Tests use **TestNG** (not JUnit) as the default test framework across all subprojects. The elasticsearch integration tests require Docker.

**Apple Silicon (M-series Mac)**: Requires `brew install mariadb` and uncommenting three lines in `EmbeddedMariaInstance.java` (see `docs/developers.md`).

## Module Architecture

```
core-models/          → Pegasus PDL schemas: Urn, AuditStamp, Url, Time (no Java logic)
core-models-utils/    → URN utility helpers
dao-api/              → DAO abstractions (BaseLocalDAO, BaseSearchDAO, BaseBrowseDAO),
                        event producers, query utilities, retention policies
dao-impl/
  ebean-dao/          → SQL storage via Ebean ORM (EbeanLocalDAO, relationship queries)
  elasticsearch-dao/  → ES 5.x/6.x search implementation
  elasticsearch-dao-7/→ ES 7.x search implementation
restli-resources/     → Rest.li resource base classes (BaseEntityResource,
                        BaseSearchableEntityResource) mapping DAOs to REST endpoints
validators/           → Schema validators ensuring PDL models conform to GMA conventions
                        (AspectValidator, EntityValidator, SnapshotValidator, etc.)
gradle-plugins/       → Annotation parsing (@gma) and code generation for metadata events
testing/              → Test infrastructure, ES integration test harness, test models
```

## Key Architectural Patterns

- **Urn (Universal Resource Name)**: `urn:li:entityType:entityKey` — the universal identifier for all entities. Typed URN subclasses provide entity-specific keys.
- **Aspect Union Pattern**: Each entity type defines a Pegasus union of its supported aspects. Validators enforce that union members are record types only.
- **Aspect Versioning**: Version 0 = latest. Each aspect write creates a new immutable version. Retention policies (indefinite, time-based, version-based) control history.
- **Layered Storage**: BaseLocalDAO (SQL, source of truth) → BaseSearchDAO (Elasticsearch, derived index) → BaseBrowseDAO (hierarchical navigation). BaseRemoteDAO proxies to other GMS instances.
- **Event Sourcing**: Writes to LocalDAO trigger MCE/MAE event emission via BaseMetadataEventProducer. The `gradle-plugins` auto-generate event PDL schemas from `@gma` annotations on aspect PDL files.
- **Generic Type Binding**: DAOs are heavily parameterized with generics (`<ASPECT_UNION, URN>`) and validate type constraints at construction time using reflection via `ModelUtils`.

## Pegasus/Rest.li Data Models

PDL (Pegasus Data Language) schemas live in `src/main/pegasus/` directories and compile to Java `RecordTemplate` classes. Key namespaces:
- `com.linkedin.common.*` — Core types (Urn, AuditStamp)
- `com.linkedin.metadata.aspect.*` — Aspect wrappers
- `com.linkedin.metadata.query.*` — Search/filter structures
- `com.linkedin.metadata.events.*` — Change tracking types
- `com.linkedin.metadata.snapshot.*` — Entity snapshots (versioned aspect collections)

When modifying PDL schemas, the Pegasus gradle plugin regenerates Java bindings automatically during build.

## Commit Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/): `<type>(scope): description`

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `perf`, `style`, `build`, `ci`

Max line length: 88 characters. Use imperative present tense, no capitalized first letter, no trailing dot.

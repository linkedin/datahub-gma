# Metadata Events (v5)

## Status

**In development.** Check the [roadmap](../roadmap.md) for more details.

## Background

Please read about the current ("v4") events [here](./mxe.md) to get a general understanding of the types of events and
what they do.

## What are "v5" Metadata Events?

In brief, we've split the monolithic MCE/MAE/FMCE events into smaller events. One event per aspect-entity combination.

## What were the issues with v4?

We found that the v4 design for events was not scaling for development. The v4 MCE/MAE/FMCE are monolithic events. For
example, MCE v4 is roughly defined as:

```pdl
record MetadataChangeEventv4 {
    snapshot: Snapshot
}

union Snapshot [
    ChartSnapshot,
    CorpUserSnapshot,
    DatasetSnapshot
    // etc. Literally every single possible entity snapshot must be listed here.
]
```

This means that MCE transitively references all possible metadata snapshots / aspects in the ecosystem (and similarly
for MAE/FMCE). We found this to have the follow problems:

- The MXEs referencing all possible models means all models must be in the same project, which:
  - Made it hard to determine what information you needed.
  - Makes distrubted ownership of models is not possible.
    - Internally, this means our team must review all model changes, as only we have review permissions.
    - Externally (open source), this means that people need to fork the repo to add their own models and need to rebuild
      the entire project. This can lead to merge conflicts.
  - We've found internally that this also means that "we're all in this together"; if any team makes a mistake with
    their models, it can impact everyone else.
- Maintaining the definitions for the MXEs by hand can be difficult.
- Singular kafka topic must have very wide ACLs so every job that needs to read it can. This means teams have less
  control over who can read their metadata.

## What are the design goals of v5?

- Break up the event definitions, unblocking<sup>[1](#footnote_1)</sup> distributed ownership of models and resolving
  issues listed above.
- Autogenerate event definitions to cut down on maintenance needed.

## Detailed Design

### Auto Generating Events

Auto generating events are done by applying the
[metadata-event-generator](../../gradle-plugins/metadata-events-generator-plugin/README.md) plugin to a build file, and
then [annotating](TODO) PDL [aspects](./aspect.md) with the associated entity URN.

```pdl
@gma.aspect.entity.urn = "com.linkedin.common.CorpUserUrn"
record CorpUserInfo {
  username: string
}
```

In the above example, we're associating the `CorpUserInfo` _aspect_ with the CorpUser _entity_ by annotating it with the
`CorpUserUrn`.

### Types of Generated Events

The plugin will generate 3 event definitions per annotated aspect, `MetadataChangeEvent`, `MetadataAuditEvent`, and
`FailedMetadataChangeEvent`. These events always have the same name, and will be generated in different Java namespaces
that follow the pattern `com.linkedin.mxe.%entityName%.%aspectName%`, e.g. `com.linkedin.mxe.corpUser.corpUserInfo`.

### Event Structure

The generated `MetadataChangeEvent` will always contain 3 fields:

- `auditHeader` - the kafka audit header.
- `urn` - the urn of the entity.
- `proposedValue` - the value of the aspect.

The generated `MetadataAuditEvent` will always contain 4 fields:

- `auditHeader` - the kafka audit header.
- `urn` - the urn of the entity.
- `oldValue` - the old value of the aspect before the update.
- `newValue` - the new value of the aspect after the update.

Finally, the generated `FailedMetadataChangeEvent` will always contain 3 fields:

- `auditHeader` - the kafka audit header.
- `metadataChangeEvent` - the full `MetadataChangeEvent` that failed to be processed.
- `error` - the error message or stack trace for the failure.

### Example Events

```pdl
namespace com.linkedin.mxe.corpUser.corpUserInfo

import com.linkedin.avro2pegasus.events.KafkaAuditHeader
import com.linkedin.common.CorpUserUrn
import com.linkedin.identity.CorpUserInfo


/**
 * MetadataChangeEvent for the MyEntity with MyAspect aspect.
 */
@MetadataChangeEvent
record MetadataChangeEvent {

  /**
   * Kafka audit header. See go/kafkaauditheader for more info.
   */
  auditHeader: optional KafkaAuditHeader

  /**
   * CorpUserUrn as the key for the MetadataChangeEvent.
   */
  urn: CorpUserUrn

  /**
   * Value of the proposed CorpUserInfo change.
   */
  proposedValue: optional CorpUserInfo

}
```

```pdl
namespace com.linkedin.mxe.corpUser.corpUserInfo

import com.linkedin.avro2pegasus.events.KafkaAuditHeader
import com.linkedin.common.CorpUserUrn
import com.linkedin.identity.CorpUserInfo

/**
 * MetadataAuditEvent for the CorpUserUrn with CorpUserInfo aspect.
 */
@MetadataAuditEvent
    record MetadataAuditEvent {

    /**
     * Kafka audit header for the MetadataAuditEvent.
     */
    auditHeader: optional KafkaAuditHeader

    /**
     * CorpUserUrn as the key for the MetadataAuditEvent.
     */
    urn: CorpUserUrn

    /**
     * Aspect of the CorpUserInfo before the update.
     */
    oldValue: optional CorpUserInfo

    /**
     * Aspect of the CorpUserInfo after the update.
     */
    newValue: CorpUserInfo
}
```

```pdl
namespace com.linkedin.mxe.corpUser.corpUserInfo

import com.linkedin.avro2pegasus.events.KafkaAuditHeader

/**
 * FailedMetadataChangeEvent for the CorpUserUrn with CorpUserInfo aspect.
 */
@FailedMetadataChangeEvent
record FailedMetadataChangeEvent {
    /**
     * Kafka event for capturing a failure to process a MetadataChangeEvent.
     */
    auditHeader: optional KafkaAuditHeader

    /**
     * The event that failed to be processed.
     */
    metadataChangeEvent: MetadataChangeEvent

    /**
     * The error message or the stacktrace for the failure.
     */
    error: string
}
```

---

<sup><a name="footnote_1">[1](#footnote*1)</a></sup> Deleting aspect unions and moving models into separate repositories
is, at time of writing, still blocked on other efforts (namely removing code references to these, e.g. in DAO classes).
The main point is that these things are no longer blocked by \_events*. By extension, models cannot move to separate
repository until the aspect unions are totally done away with.

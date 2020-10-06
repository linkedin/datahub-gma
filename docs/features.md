# GMA Features

GMA, being a general metadata architecture, contains no direct references to any piece of metadata itself. If you are
looking for the kinds of metadata and entities that _DataHub_ uses with GMA, check out
[DataHub's feature list](https://github.com/linkedin/datahub/blob/master/docs/features.md).

## Metadata Sources

You can integrate any data platform to GMA easily. As long as you have a way of _extracting_ metadata from the platform
and _transform_ that into our standard [MCE](what/mxe.md) format, you're free to _load_/ingest metadata to DataHub from
any available platform.

We have provided example [ETL ingestion](architecture/metadata-ingestion.md) scripts for:

- Hive
- Kafka
- RDBMS (MySQL, Oracle, Postgres, MS SQL etc)
- Data warehouse (Snowflake, BigQuery etc)
- LDAP

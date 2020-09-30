# GMA FAQs

## Who are the major contributors in the community?

Currently LinkedIn engineers. However, we’re receiving more and more PRs from individuals working at various companies.

## Is there a contributor selection criteria?

We welcome contributions from everyone in the community. Please read our [contirbuting guidelines](CONTRIBUTING.md). In
general, we will review PRs with the same rigor as our internal code review process to maintain overall quality.

## How does LinkedIn plan to engage with the community?

We plan to organize public town hall meetings at a monthly cadence, which may change depending on the interest from the
community. Also we recently deprecated Gitter and start using
[Slack](https://datahubspace.slack.com/join/shared_invite/zt-cl60ng6o-6odCh_I~ejZKE~a9GG30PA) as one of the main ways to
support the community.

## Should this be the platform we decide upon, we’d like to fully engage and work with LinkedIn and the community. What’s the best way and what level of engagement/involvement should we expect?

The best way to engage is through the
[Slack channel](https://datahubspace.slack.com/join/shared_invite/zt-cl60ng6o-6odCh_I~ejZKE~a9GG30PA). You’ll get to
interact with the developers and the community. We are fairly response on Slack and plan to setup proper oncall support
during normal business hours (Pacific Time). Sometimes we also create working groups with dedicated POC from the
LinkedIn team to tackle specific use cases the community has.

For reproducible technical issues, bugs and code contributions, Github
[issues](https://github.com/linkedin/datahub-gma/issues) and [PRs](https://github.com/linkedin/datahub-gma/pulls) are
the preferred channel.

## What’s the best way to ramp up the product knowledge to properly test and evaluate DataHub?

[Github](https://github.com/linkedin/datahub-gma) is the best resource. We have documented the steps to install and test
DataHub thoroughly there. There is also copious of document on
[overall architecture](https://github.com/linkedin/datahub/tree/master/docs/architecture),
[definitions](https://github.com/linkedin/datahub/tree/master/docs/what), and
[onboarding guides](https://github.com/linkedin/datahub/tree/master/docs/how).

The [DataHub Introduction](https://engineering.linkedin.com/blog/2019/data-hub) and
[Open Sourcing Datahub](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p)
blog posts are also useful resources for getting a high level understanding of the system.

## Where can I learn about the roadmap?

You can learn more about GMA's [product roadmap](roadmap.md), which gets updated regularly.

## Where can I learn about the current list of features/functionalities?

You can learn more about the current [list of features](features.md).

## Are the product strategy/vision/roadmap driven by the LinkedIn Engineering team, community, or a collaborative effort?

Mixed of both LinkedIn GMA team and the community. The roadmap will be a joint effort of both LinkedIn and the
community. However, we’ll most likely prioritize tasks that align with the community's asks.

## How approachable would LinkedIn be to provide insights/support or collaborate on a functionality?

Please take a look at our [roadmap](roadmap.md) & [features](features.md) to get a sense of what’s being open sourced in
the near future. If there’s something missing from the list, we’re open to discussion. In fact, the town hall would be
the perfect venue for such discussions.

## How do LinkedIn Engineering team and the community ensure the quality of the community code for GMA?

All PRs are reviewed by the LinkedIn team.

## How are the ingestion ETL processes scheduled at LinkedIn?

It varies depending on the data platform. HDFS, MySQL, Oracle, Teradata, and LDAP are scheduled on a daily basis. We
rely on real-time pushs to ingest from sveral data platforms such as Hive, Presto, Kafka, Pinot,
[Espresso](https://engineering.linkedin.com/espresso/introducing-espresso-linkedins-hot-new-distributed-document-store),
[Ambry](https://github.com/linkedin/ambry), [Galene](https://engineering.linkedin.com/search/did-you-mean-galene),
[Venice](https://engineering.linkedin.com/blog/2017/02/building-venice-with-apache-helix), and more.

## What are the options for the Kafka Key for MCE, MAE and FailedMCE Topic?

URN is the only sensible option to ensure events for the same entity land in the same parition and get processed in the
chronological order.

## How is the Data Quality of the ingestion messages handled?

In addition to leverage the Kafka schema validation to ensure the MXEs output from metadata producer, we also actively
monitor the ingestion streaming pipeline on the snapshot level with status.

## When using Kafka and Confluent Schema Registry, does DataHub support multiple schemas for the same topic?

You can [configure](https://docs.confluent.io/current/schema-registry/develop/api.html#compatibility) compatibility
level per topic at Confluent Schema Registry. The default being used is “Backward”. So, you’re only allowed to make
backward compatible changes on the topic schema. You can also change this configuration and flex compatibility check.
However, as a best practice, we would suggest not doing backward incompatible changes on the topic schema because this
will fail your old metadata producers’ flows. Instead, you might consider creating a new Kafka topic (new version).

## How do we better document and map transformations within an ETL process? How do we create institutional knowledge and processes to help create a paradigm for tribal knowledge?

We plan to add “fine-grain lineage” in the near future, which should cover the transformation documentation. DataHub
currently has a simple “Docs” feature that allows capturing of tribal knowledge. We also plan to expand it significantly
going forward.

## Does the roadmap have provision for capturing the Data Quality Information of the Dataset?

We’re working on a similar
[feature](https://engineering.linkedin.com/blog/2020/data-sentinel-automating-data-validation) internally. Will evaluate
and update the roadmap once we have a better idea of the timeline.

## Is DataHub capturing/showing column level [constraints](https://www.w3schools.com/sql/sql_constraints.asp) set at table definition?

The
[SchemaField](https://github.com/linkedin/datahub-gma/blob/master/metadata-models/src/main/pegasus/com/linkedin/schema/SchemaField.pdl)
model currently does not capture any property/field corresponding to constraints defined in the table definition.
However, it should be fairly easy to extend the model to support that if needed.

## How does GMA manage extracting metadata from stores residing in different security zones?

MCE is the ideal way to push metadata from different security zones, assuming there is a common Kafka infrastructure
that aggregates the events from various security zones.

## What all data stores does GMA support presently?

Currently, GMA supports all major database providers that are supported by Ebean as the document store i.e. Oracle,
Postgres, MySQL, H2. We also support
[Espresso](https://engineering.linkedin.com/espresso/introducing-espresso-linkedins-hot-new-distributed-document-store),
which is LinkedIn's proprietary document store. Other than that, we support Elasticsearch and Neo4j for search and graph
use cases, respectively. However, as data stores in the backend are all abstracted and accessed through DAOs, you should
be able to easily support other data stores by plugging in your own DAO implementations. Please refer to
[Metadata Serving](architecture/metadata-serving.md) for more details.

## For which stores, you have discovery services?

Supported data sources are listed [here](https://github.com/linkedin/datahub-gma/tree/master/metadata-ingestion). To
onboard your own data source which is not listed there, you can refer to the
[onboarding guide](how/data-source-onboarding.md).

## How is metadata ingested in GMA? Is it real-time?

You can call the [rest.li](https://github.com/linkedin/rest.li) API to ingest metadata into a GMS directly instead of
using Kafka event. Metadata ingestion is real-time if you're updating via rest.li API. It's near real-time in the case
of Kafka events due to the asynchronous nature of Kafka processing.

## Does Kafka support SSL? If so, how?

Yes. We are using the Spring Boot framework to start our apps, including setting up Kafka. You can
[use environment variables to set system properties](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config-relaxed-binding-from-environment-variables),
including
[Kafka properties](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#integration-properties).
From there you can set your SSL configuration for Kafka.

# Reusing Aspects

> Note: This applies to [MXE v5](../what/mxev5.md), which is currently in development.

We only support associating one aspect with one entity via the [`@gma.aspect.entity.urn`](./annotations/aspect.md)
annotation. However, there are still ways to reuse the definitions for models as aspects within GMA.

Reusing model definitions as aspects is rare, but useful. It helps to maintain consistency within the ecosystem.

In the examples below, assume we have this model we want to reuse as a common aspect for more than one entity.

```pdl
record Ownership {
  ownerName: string
}
```

## Option 1: Wrapper Aspect (preferred)

Do not treat the reused model as an aspect itself, instead create aspects which contain the model-to-be-reused as a
field (composition).

```pdl
@gma.aspect.entity.urn = "com.linkedin.example.Dataset"
record DatasetOwnership {
  ownership: Ownership
}
```

```pdl
@gma.aspect.entity.urn = "com.linkedin.example.Metric"
record MetricOwnership {
  ownership: Ownership
}
```

This method is preferred in general. PDL lacks proper inheritance (see option 2). So if you wish to have some common
Java method that accepts or returns an `Ownership`, the only way to reuse that code for both entities is to have
`Ownership` as a field on the aspects.

```java
void accept(Ownership ownership);
Ownership buildSomeOwnership();

DatasetOwnership datasetOwnership;
MetricOwnership metricOwnership;

accept(datasetOwnership.getOwnership());
accept(metricOwnership.getOwnership());

datasetOwnership.setOwnership(buildSomeOwnership());
metricOwnership.setOwnership(buildSomeOwnership());
```

## Option 2: `includes`

PDL has some limited mixin support (not real inheritance). You can `include` your base model in your aspects.

```pdl
@gma.aspect.entity.urn = "com.linkedin.example.Dataset"
record DatasetOwnership includes Ownership {
}
```

```pdl
@gma.aspect.entity.urn = "com.linkedin.example.Metric"
record MetricOwnership includes Ownership {
}
```

However, as stated above, this is not true inheritance (in the generated Java). `Ownership`, `DatasetOwnership`, and
`MetricOwnership` have no type relation to each other (put another way, the latter two do not `extend` nor `implements`
`Ownership`). The latter two just happen to have the same fields as the former. There is no way, barring reflection
(_which is a bad idea_), to have code reuse both `DatasetOwnership` and `MetricOwnership`.

This method is _not_ preferred, but is a possibility.

Some snippets of the generated Java may look like:

```java
class Ownership extends RecordTemplate {
  String getOwnerName() { /* ... */ }
  void setOwnerName(String value) { /* ... */ }
}
```

```java
// no type relation to Ownership
class DatasetOwnership extends RecordTemplate {
  String getOwnerName() { /* ... */ }
  void setOwnerName(String value) { /* ... */ }
}
```

```java
// no type relation to Ownership
class MetricOwnership extends RecordTemplate {
  String getOwnerName() { /* ... */ }
  void setOwnerName(String value) { /* ... */ }
}
```

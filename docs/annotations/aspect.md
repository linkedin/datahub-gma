# Aspect Annotation (`@gma.aspect`)

> Note: This applies to [MXE v5](../what/mxev5.md), which is currently in development.

The `@gma.aspect` annotation denotes that the annotated record is a
[GMA aspect](https://github.com/linkedin/datahub/blob/master/docs/what/aspect.md).

The annotation currently has the following properties.

## `@gma.aspect.entity.urn`

`@gma.aspect.entity.urn` is a string specifying the fully qualified Java class name of the entity that this aspect is
associated with.

Example:

```pdl
@gma.aspect.entity.urn = "com.linkedin.example.MyEntityUrn"
record MyAspect {
}
```

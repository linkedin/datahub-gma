# Finders for the Strongly Consistent Secondary Index

The Strongly Consistent Secondary Index (SCSI) can be used to filter and find a list of 
[URNs](https://github.com/linkedin/datahub-gma/blob/master/docs/what/urn.md) that match the filter criterion. In
addition to filtering, the returned URNs can be sorted by a field, aggregated by a field, and paginated.

## Sorting

By default, the list of returned URNs is sorted by the URN. This list can also be sorted by a field, specified by an 
aspect and path. Currently, only primitives, enums, and nested primitive fields are supported for sorting.

To sort, an [IndexSortCriterion](https://github.com/linkedin/datahub-gma/blob/master/dao-api/src/main/pegasus/com/linkedin/metadata/query/IndexSortCriterion.pdl)
can be passed into the filter method. 

## Pagination

Results can be paginated using offset-based or cursor-based pagination.

For offset-based pagination, a [PagingContext](https://linkedin.github.io/rest.li/user_guide/restli_server#collection-pagination)
has to be provided, specifying the start and count. This will return the list of URNs with pagination information,
including the start, count, and total number of results before pagination.

For cursor-based pagination, the last urn and count can be specified. By default, the last urn will be set to `null`, 
which means that results should start from the first entry and the count defaults to 10.

## Count Aggregation

Results can be aggregated to return a count of the fields after applying the filter criterion. An 
[IndexGroupByCriterion](https://github.com/linkedin/datahub-gma/blob/master/dao-api/src/main/pegasus/com/linkedin/metadata/query/IndexGroupByCriterion.pdl)
specifying the field to group by can be passed in. The filtered results will be grouped by the specified group criterion,
and a map of the field to the count will be returned. Currently, users must specify both the aspect and the path
in the group criterion.


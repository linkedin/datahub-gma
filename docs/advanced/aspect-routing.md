# Aspect Routing

Current routing mechanism dispatches requests based on entity type. We would like to support aspect-based routing.For
example, requests for same aspect of different entities will be routed to same GMS.

## Short term solution

Create a base resource class which will override read and write operations. GMS services can extend this base resource
class to obtain aspect routing capability.

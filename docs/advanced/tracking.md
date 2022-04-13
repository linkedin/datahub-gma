# Metadata Tracking

Metadata tracking enables the distributed tracing and aggregating across the GMA components which provides
high-confident tracking results that help to statisfy the customized tracking purposes.

## Tracking Manager

The TrackingManager class provides the default methods to be implemented based on probing each process state with
timestamp and trackingID.

## Integration and Aggregation

In each process states defined in the trackingUtils, the latency, error rate as well as throughput can be calculated
from the offline aggregation.

All tags are prefixed with `eqx.`

The goal here is to have a span representing `Transact` and `Query` with ALL the context necessary to understand how the system is behaving in production (caveat that we will/should rely on underlying instrumentations of e.g. npqsql and dynamo)

So what is interesting context in this case?

## General information about the stream and store

| Tag         | Type   | Description                | 
|-------------|--------|----------------------------|
| stream_name | string | The full stream name       |
| stream_id   | string | The ID of the stream       |
| category    | string | The category of the stream |
| store       | string | The store being used       |

## Information about the loading of events

| Tag                 | Type   | Description                                                             | 
|---------------------|--------|-------------------------------------------------------------------------|
| read_version        | int    | The version of the stream at read time                                  | 
| batch_size          | int    | The configured batch size                                               |
| batches             | int    | The total number of batches loaded from the store                       | 
| count               | int    | The total number of events loaded from the store                        |
| bytes               | int    | The total bytes loaded during Transact/Query                            |
| loaded_from_version | int    | The version we load forwards from                                       |
| load_method         | string | The load method (BatchForward, LatestEvent, BatchBackward, etc)         |
| requires_leader     | bool   | Whether we needed to use the leader node                                | 
| cache_hit           | bool   | Whether we used a cached value (regardless of whether we also reloaded) | 
| allow_stale         | bool   | Whether we accept AnyCachedValue                                        | 
| snapshot_version    | int    | If a snapshot was loaded, which version was it generated from           |

## Information about the appending of events

| Tag              | Type     | Description                                                 | 
|------------------|----------|-------------------------------------------------------------|
| conflict         | bool     | Whether there was at least one conflict during transact     |
| retry_count      | int      | Store level retry count                                     |
| resync_count     | int      | The number of sync attempts during transact                 |
| append_count     | int      | The number of events we appended                            | 
| append_bytes     | int      | The total bytes we appended                                 | 
| snapshot_written | bool     | Whether a snapshot was written during this transaction      |
| new_version      | int      | The new version of the stream after appending events        |
| append_types     | string[] | In case of conflict, which event types did we try to append |

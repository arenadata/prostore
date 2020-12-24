# Status monitor
Actually subscribes on __consumer_offsets and provides information about status for specified group.id and topic

## Request/Response formats

Request:
```json
{
	"consumerGroup": "tarantool-group-csv",
	"topic": "AK_ACCOUNTS_EXT"
}
```

Response:
```json
{
  "topic": "AK_ACCOUNTS_EXT",
  "consumerGroup": "tarantool-group-csv",
  "consumerOffset": 3,
  "producerOffset": 3,
  "lastCommitTime": 1598280542139
}
```

## How to build and run
Build:

```shell script
cd dtm/dtm-status-monitor
mvn clean package
docker build -t dtm-status-monitor .
```

and then run:
```shell script
docker run -d -e STATUS_MONITOR_BROKERS=10.92.3.31:9092 -p 9095:9095 --name dtm-status-monitor dtm-status-monitor
```

and then check
```shell script
curl --request POST \
  --url http://127.0.0.1:9095/status \
  --header 'content-type: application/json' \
  --data '{
	"consumerGroup": "tarantool-group-csv",
	"topic": "AK_ACCOUNTS_EXT"
}'
```
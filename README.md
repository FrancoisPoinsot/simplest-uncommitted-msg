# simplest-uncommitted-msg

This is a simple hardcoded kafka producer to output both committed and uncommitted messages in a given topic.

## dependencies

require :
- maven
- a running kafka instance
- Java 1.8 (-_-") Although it probably work with some lower versions.

## docker-compose

For simplicity this repo come with a valid docker-compose running a kafka instance.
It sole purpose is to have a kafka broker.

run `docker-compose up` to run it and `docker-compose down` to shut it down.

/!\ Because dealing with `ADVERTISED_HOST` is a pain, this one is "kafka" so that it can be called both from outside and inside the docker-compose thingy.
Of course this  require you add a line in your `host` file to resolve "kafka" as localhost.

## install

` mvn clean install`

## run

unix:
` mvn exec:java -Dexec.mainClass="CustomProducer.Main"`

windows:
`mvn exec:java -D"exec.mainClass"="CustomProducer.Main"`


This will output some messages in topic `topic-test`. You will find both committed and uncommitted message in there.
To check the content, you can use good old `kafka-console-consumer`:

- read all messages: 
`kafka-console-consumer --bootstrap-server kafka:9092 --topic topic-test --from-beginning --timeout-ms 2000 --isolation-level read_uncommitted`
output should look like 
```
uncommitted
uncommitted
Committed 1
Committed 2
uncommitted
uncommitted
Committed 3
Committed 4
```

- read committed only (default): 
` kafka-console-consumer --bootstrap-server kafka:9092 --topic topic-test --from-beginning --timeout-ms 2000 --isolation-level read_committed`
```
Committed 1
Committed 2
Committed 3
Committed 4
```

## test cases

There is a few test case on different topic. 
If you consume committed only you should find no `uncommitted` value.
If you consume all committed messages you should find `Committed 1`, `Committed 2`, ... in consecutive order.

In each topic You should find this number of committed messages:

- `topic-test`: 4 messages
- `topic-test-2`: 2 messages
- `topic-test-3`: 6 messages
- `topic-test-4`: 6 messages
- `topic-test-5`: 1000 messages

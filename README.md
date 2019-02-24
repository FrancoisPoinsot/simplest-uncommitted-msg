# simplest-uncommitted-msg

This is a simple hardcoded kafka producer to output both committed and uncommitted messages in a given topic.

## dependencies

require :
- maven
- a running kafka instance
- Java 1.8 (-_-") Althouh it probably work with some lower versions.

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



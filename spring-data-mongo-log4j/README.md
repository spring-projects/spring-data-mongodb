# MongoDB Log4J Appender

This module sets up a Log4J appender that puts logging events in MongoDB. It is fully configurable
and connects directly to the MongoDB server using the driver. It has no dependency on any Spring package.

To use it, configure a host, port, (optionally) applicationId, and database property in your Log4J configuration:

    log4j.appender.stdout=org.springframework.data.document.mongodb.log4j.MongoLog4jAppender
    log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
    log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] - <%m>%n
    log4j.appender.stdout.host = localhost
    log4j.appender.stdout.port = 27017
    log4j.appender.stdout.database = logs
    log4j.appender.stdout.applicationId = my.application
    log4j.appender.stdout.warnOrHigherWriteConcern = FSYNC_SAFE

It will even support properties in your MDC (so long as they're Strings or support .toString()).

The collection name is configurable as well. If you don't specify anything, it will use the Category name.
If you want to specify a collection name, you can give it a String.format() string which will be passed the
following parameters:

1. Calendar.YEAR
2. Calendar.MONTH
3. Calendar.DAY_OF_MONTH
4. Calendar.HOUR_OF_DAY
5. event.getLevel().toString()
6. event.getLogger().getName()

An example log entry might look like:

    {
      "_id" : ObjectId("4d89341a8ef397e06940d5cd"),
      "applicationId" : "my.application",
      "name" : "org.springframework.data.document.mongodb.log4j.AppenderTest",
      "level" : "DEBUG",
      "timestamp" : "1300837402444",
      "properties" : {
        "property" : "one"
      },
      "message" : "DEBUG message"
    }

To set WriteConcern levels for WARN or higher messages, set warnOrHigherWriteConcern to one of the following:

* FSYNC_SAFE
* NONE
* NORMAL
* REPLICAS_SAFE
* SAFE

[http://api.mongodb.org/java/2.5-pre-/com/mongodb/WriteConcern.html#field_detail](http://api.mongodb.org/java/2.5-pre-/com/mongodb/WriteConcern.html#field_detail)
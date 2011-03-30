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
    log4j.appender.stdout.collectionPattern = %c
    log4j.appender.stdout.applicationId = my.application
    log4j.appender.stdout.warnOrHigherWriteConcern = FSYNC_SAFE

It will even support properties in your MDC (so long as they're Strings or support .toString()).

The collection name is configurable as well. If you don't specify anything, it will use the Category name.
If you want to specify a collection name, you can give it a Log4J pattern layout format string which will have
the following additional MDC variables in the context when the collection name is rendered:

    "year" = Calendar.YEAR
    "month" = Calendar.MONTH + 1
    "day" = Calendar.DAY_OF_MONTH
    "hour" = Calendar.HOUR_OF_DAY
    "applicationId" = configured applicationId

An example log entry might look like:

    {
      "_id" : ObjectId("4d89341a8ef397e06940d5cd"),
      "applicationId" : "my.application",
      "name" : "org.springframework.data.document.mongodb.log4j.AppenderTest",
      "level" : "DEBUG",
      "timestamp" : ISODate("2011-03-23T16:53:46.778Z"),
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
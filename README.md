# Spring Data MongoDB

The primary goal of the [Spring Data](http://projects.spring.io/spring-data) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.

The Spring Data MongoDB project aims to provide a familiar and consistent Spring-based programming model for new datastores while retaining store-specific features and capabilities. The Spring Data MongoDB project provides integration with the MongoDB document database. Key functional areas of Spring Data MongoDB are a POJO centric model for interacting with a MongoDB DBCollection and easily writing a repository style data access layer.

## Getting Help

For a comprehensive treatment of all the Spring Data MongoDB features, please refer to:

* the [User Guide](http://docs.spring.io/spring-data/mongodb/docs/current/reference/html/)
* the [JavaDocs](http://docs.spring.io/spring-data/mongodb/docs/current/api/) have extensive comments in them as well.
* the home page of [Spring Data MongoDB](http://projects.spring.io/spring-data-mongodb) contains links to articles and other resources.
* for more detailed questions, use [Spring Data Mongodb on Stackoverflow]http://stackoverflow.com/questions/tagged/spring-data-mongodb).

If you are new to Spring as well as to Spring Data, look for information about [Spring projects](http://projects.spring.io/).


## Quick Start

### Maven configuration

Add the Maven dependency:

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-mongodb</artifactId>
  <version>1.5.0.RELEASE</version>
</dependency>
```

If you'd rather like the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-mongodb</artifactId>
  <version>1.6.0.BUILD-SNAPSHOT</version>
</dependency>

<repository>
  <id>spring-libs-snapshot</id>
  <name>Spring Snapshot Repository</name>
  <url>http://repo.spring.io/libs-snapshot</url>
</repository>
```

### MongoTemplate

MongoTemplate is the central support class for Mongo database operations. It provides:

* Basic POJO mapping support to and from BSON
* Convenience methods to interact with the store (insert object, update objects) and MongoDB specific ones (geo-spatial operations, upserts, map-reduce etc.)
* Connection affinity callback
* Exception translation into Spring's [technology agnostic DAO exception hierarchy](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/dao.html#dao-exceptions).

### Spring Data repositories

To simplify the creation of data repositories Spring Data MongoDB provides a generic repository programming model. It will automatically create a repository proxy for you that adds implementations of finder methods you specify on an interface.  

For example, given a `Person` class with first and last name properties, a `PersonRepository` interface that can query for `Person` by last name and when the first name matches a like expression is shown below:

```java
public interface PersonRepository extends CrudRepository<Person, Long> {

  List<Person> findByLastname(String lastname);

  List<Person> findByFirstnameLike(String firstname);
}
```

The queries issued on execution will be derived from the method name. Extending `CrudRepository` causes CRUD methods being pulled into the interface so that you can easily save and find single entities and collections of them.

You can have Spring automatically create a proxy for the interface by using the following JavaConfig:

```java
@Configuration
@EnableMongoRepositories
class ApplicationConfig extends AbstractMongoConfiguration {

  @Override
  public Mongo mongo() throws Exception {
    return new MongoClient();
  }

  @Override
  protected String getDatabaseName() {
    return "springdata";
  }
}
```

This sets up a connection to a local MongoDB instance and enables the detection of Spring Data repositories (through `@EnableMongoRepositories`). The same configuration would look like this in XML:

```xml
<bean id="template" class="org.springframework.data.mongodb.core.MongoTemplate">
  <constructor-arg>
    <bean class="com.mongodb.MongoClient">
       <constructor-arg value="localhost" />
       <constructor-arg value="27017" />
    </bean>
  </constructor-arg>
  <constructor-arg value="database" />
</bean>

<mongo:repositories base-package="com.acme.repository" />
```

This will find the repository interface and register a proxy object in the container. You can use it as shown below:

```java
@Service
public class MyService {

  private final PersonRepository repository;

  @Autowired
  public MyService(PersonRepository repository) {
    this.repository = repository;
  }

  public void doWork() {

     repository.deleteAll();

     Person person = new Person();
     person.setFirstname("Oliver");
     person.setLastname("Gierke");
     person = repository.save(person);

     List<Person> lastNameResults = repository.findByLastname("Gierke");
     List<Person> firstNameResults = repository.findByFirstnameLike("Oli*");
 }
}
```

## Contributing to Spring Data

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on Stackoverflow and help out on the [spring-data-mongodb](http://stackoverflow.com/questions/tagged/spring-data-mongodb) tag by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/DATADOC) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://spring.io/blog) to spring.io.

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.

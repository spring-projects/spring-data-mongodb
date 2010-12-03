Spring Data - Document
======================

The primary goal of the [Spring Data](http://www.springsource.org/spring-data) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.
As the name implies, the **Document** modules provides integration with document databases such as [MongoDB](http://www.mongodb.org/) and [CouchDB](http://couchdb.apache.org/).

Getting Help
------------

Read the main project [website](http://www.springsource.org/spring-data)) and the [User Guide](http://static.springsource.org/spring-data/datastore-keyvalue/snapshot-site/reference/html/) (A work in progress). 

At this point your best bet is to look at the Look at the [JavaDocs](http://static.springsource.org/spring-data/data-document/docs/1.0.0.BUILD-SNAPSHOT/spring-data-mongodb/apidocs/) for MongoDB integration and corresponding and source code. For more detailed questions, use the [forum](http://forum.springsource.org/forumdisplay.php?f=80). If you are new to Spring as well as to Spring Data, look for information about [Spring projects](http://www.springsource.org/projects). 

Quick Start
-----------

## Redis

For those in a hurry:


* Download the jar through Maven:

      <dependency>
        <groupId>org.springframework.data</groupId>
        <artifactId>spring-data-document</artifactId>
        <version>1.0.0-BUILD-SNAPSHOT</version>
      </dependency> 


      <repository>
        <id>spring-maven-snapshot</id>
        <snapshots><enabled>true</enabled></snapshots>
        <name>Springframework Maven SNAPSHOT Repository</name>
        <url>http://maven.springframework.org/snapshot</url>
      </repository> 


* The MongoTemplate is the central support class for Mongo database operations.  It supports
** Basic POJO mapping support to and from BSON
** Connection Affinity callback
** Exception translation into Spring's 

Future plans are to support optional logging and/or exception throwing based on WriteResult return value, common map-reduce operations, GridFS operations.  A simple API for partial document updates is also planned.

* To simplify the creation of Data Repositories a generic Repository interface and base class is provided.  Furthermore, Spring will automatically create a Repository implementation for you that add implementations of finder methods you specify on an interface.  For example the Repository interface is

    public interface Repository<T, ID extends Serializable> {

        T save(T entity);

	List<T> save(Iterable<? extends T> entities);

	T findById(ID id);

	boolean exists(ID id);

	List<T> findAll();

	Long count();

	void delete(T entity);

	void delete(Iterable<? extends T> entities);

	void deleteAll();
    }

and there is a placeholder interface called MongoRepository that will in future add more Mongo specific methods.

    public interface MongoRepository<T, ID extends Serializable> extends
        Repository<T, ID> {
    }

You can use the provided implementation class SimpleMongoRepository for basic data access.  You can also extend the MongoRepository interface and supply your own finder methods that follow simple naming conventions so they can be converted into queries.  For example, given a Person class with first and last name properties

    public interface PersonRepository extends MongoRepository<Person, Long> {

      List<Person> findByLastname(String lastname);

      List<Person> findByFirstnameLike(String firstname);
    }

You can have Spring automatically generate the implemention as shown below

        <bean id="template" class="org.springframework.data.document.mongodb.MongoTemplate">
                <constructor-arg>
                        <bean class="com.mongodb.Mongo">
                                <constructor-arg value="localhost" />
                                <constructor-arg value="27017" />
                        </bean>
                </constructor-arg>
                <constructor-arg value="database" />
                <property name="defaultCollectionName" value="springdata" />
        </bean>

        <bean class="org.springframework.data.document.mongodb.repository.MongoRepositoryFactoryBean">
                <property name="template" ref="template" />
                <property name="repositoryInterface" value="org.springframework.data.document.mongodb.repository.PersonRepository" />
        </bean>

This will register an object in the container named PersonRepository.  You can use it as shown below

     @Service
     public class MyService {

        @Autowired
        PersonRepository repository;


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


## CouchDB

TBD


Contributing to Spring Data
---------------------------

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?f=80) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/DATADOC) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.

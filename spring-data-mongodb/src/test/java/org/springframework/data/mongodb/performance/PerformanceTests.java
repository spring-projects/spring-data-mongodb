/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.performance;

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.Constants;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactoryBean;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

/**
 * Test class to execute performance tests for plain MongoDB driver usage, {@link MongoTemplate} and the repositories
 * abstraction.
 * 
 * @author Oliver Gierke
 */
public class PerformanceTests {

	private static final String DATABASE_NAME = "performance";
	private static final int NUMBER_OF_PERSONS = 30000;
	private static final StopWatch watch = new StopWatch();
	private static final Collection<String> IGNORED_WRITE_CONCERNS = Arrays.asList("MAJORITY", "REPLICAS_SAFE",
			"FSYNC_SAFE", "JOURNAL_SAFE");
	private static final int COLLECTION_SIZE = 1024 * 1024 * 256; // 256 MB
	private static final Collection<String> COLLECTION_NAMES = Arrays.asList("template", "driver", "person");

	Mongo mongo;
	MongoTemplate operations;
	PersonRepository repository;

	@Before
	public void setUp() throws Exception {

		this.mongo = new Mongo();
		this.operations = new MongoTemplate(new SimpleMongoDbFactory(this.mongo, DATABASE_NAME));

		MongoRepositoryFactoryBean<PersonRepository, Person, ObjectId> factory = new MongoRepositoryFactoryBean<PersonRepository, Person, ObjectId>();
		factory.setMongoOperations(operations);
		factory.setRepositoryInterface(PersonRepository.class);
		factory.afterPropertiesSet();

		repository = factory.getObject();
	}

	@Test
	public void writeWithWriteConcerns() {
		executeWithWriteConcerns(new WriteConcernCallback() {
			public void doWithWriteConcern(String constantName, WriteConcern concern) {
				writeHeadline("WriteConcern: " + constantName);
				writingObjectsUsingPlainDriver("Writing %s objects using plain driver");
				writingObjectsUsingMongoTemplate("Writing %s objects using template");
				writingObjectsUsingRepositories("Writing %s objects using repository");
				writeFooter();
			}
		});
	}

	@Test
	public void writeAndRead() {

		mongo.setWriteConcern(WriteConcern.SAFE);

		for (int i = 3; i > 0; i--) {

			setupCollections();

			writeHeadline("Plain driver");
			writingObjectsUsingPlainDriver("Writing %s objects using plain driver");
			readingUsingPlainDriver("Reading all objects using plain driver");
			queryUsingPlainDriver("Executing query using plain driver");
			writeFooter();

			writeHeadline("Template");
			writingObjectsUsingMongoTemplate("Writing %s objects using template");
			readingUsingTemplate("Reading all objects using template");
			queryUsingTemplate("Executing query using template");
			writeFooter();

			writeHeadline("Repositories");
			writingObjectsUsingRepositories("Writing %s objects using repository");
			readingUsingRepository("Reading all objects using repository");
			queryUsingRepository("Executing query using repository");
			writeFooter();

			writeFooter();
		}
	}

	private void writeHeadline(String headline) {
		System.out.println(headline);
		System.out.println("---------------------------------".substring(0, headline.length()));
	}

	private void writeFooter() {
		System.out.println();
	}

	private void queryUsingTemplate(String template) {
		executeWatchedWithTimeAndResultSize(template, new WatchCallback<List<Person>>() {
			public List<Person> doInWatch() {
				Query query = query(where("addresses.zipCode").regex(".*1.*"));
				return operations.find(query, Person.class, "template");
			}
		});
	}

	private void queryUsingRepository(String template) {
		executeWatchedWithTimeAndResultSize(template, new WatchCallback<List<Person>>() {
			public List<Person> doInWatch() {
				return repository.findByAddressesZipCodeContaining("1");
			}
		});
	}

	private void executeWithWriteConcerns(WriteConcernCallback callback) {

		Constants constants = new Constants(WriteConcern.class);

		for (String constantName : constants.getNames(null)) {

			if (IGNORED_WRITE_CONCERNS.contains(constantName)) {
				continue;
			}

			WriteConcern writeConcern = (WriteConcern) constants.asObject(constantName);
			mongo.setWriteConcern(writeConcern);

			setupCollections();

			callback.doWithWriteConcern(constantName, writeConcern);
		}
	}

	private void setupCollections() {

		DB db = this.mongo.getDB(DATABASE_NAME);

		for (String collectionName : COLLECTION_NAMES) {
			DBCollection collection = db.getCollection(collectionName);
			collection.drop();
			db.command(getCreateCollectionCommand(collectionName));
			collection.ensureIndex(new BasicDBObject("firstname", -1));
			collection.ensureIndex(new BasicDBObject("lastname", -1));
		}
	}

	private DBObject getCreateCollectionCommand(String name) {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("createCollection", name);
		dbObject.put("capped", false);
		dbObject.put("size", COLLECTION_SIZE);
		return dbObject;
	}

	private void writingObjectsUsingPlainDriver(String template) {

		final DBCollection collection = mongo.getDB(DATABASE_NAME).getCollection("driver");
		final List<DBObject> persons = getPersonDBObjects();

		executeWatchedWithTime(template, new WatchCallback<Void>() {
			public Void doInWatch() {
				for (DBObject person : persons) {
					collection.save(person);
				}
				return null;
			}
		});
	}

	private void writingObjectsUsingRepositories(String template) {

		final List<Person> persons = getPersonObjects();

		executeWatchedWithTime(template, new WatchCallback<Void>() {
			public Void doInWatch() {
				repository.save(persons);
				return null;
			}
		});
	}

	private void writingObjectsUsingMongoTemplate(String template) {

		final List<Person> persons = getPersonObjects();

		executeWatchedWithTime(template, new WatchCallback<Void>() {
			public Void doInWatch() {
				for (Person person : persons) {
					operations.save(person, "template");
				}
				return null;
			}
		});
	}

	private void readingUsingPlainDriver(String template) {

		final DBCollection collection = mongo.getDB(DATABASE_NAME).getCollection("driver");

		executeWatchedWithTimeAndResultSize(String.format(template, NUMBER_OF_PERSONS), new WatchCallback<List<Person>>() {
			public List<Person> doInWatch() {
				return toPersons(collection.find());
			}
		});
	}

	private void readingUsingTemplate(String template) {
		executeWatchedWithTimeAndResultSize(String.format(template, NUMBER_OF_PERSONS), new WatchCallback<List<Person>>() {
			public List<Person> doInWatch() {
				return operations.findAll(Person.class, "template");
			}
		});
	}

	private void readingUsingRepository(String template) {
		executeWatchedWithTimeAndResultSize(String.format(template, NUMBER_OF_PERSONS), new WatchCallback<List<Person>>() {
			public List<Person> doInWatch() {
				return repository.findAll();
			}
		});
	}

	private void queryUsingPlainDriver(String template) {

		final DBCollection collection = mongo.getDB(DATABASE_NAME).getCollection("driver");

		executeWatchedWithTimeAndResultSize(template, new WatchCallback<List<Person>>() {
			public List<Person> doInWatch() {
				DBObject regex = new BasicDBObject("$regex", Pattern.compile(".*1.*"));
				DBObject query = new BasicDBObject("addresses.zipCode", regex);
				return toPersons(collection.find(query));
			}
		});
	}

	private List<DBObject> getPersonDBObjects() {

		List<DBObject> result = new ArrayList<DBObject>(NUMBER_OF_PERSONS);

		for (Person person : getPersonObjects()) {
			result.add(person.toDBObject());
		}

		return result;
	}

	private List<Person> getPersonObjects() {

		List<Person> result = new ArrayList<Person>(NUMBER_OF_PERSONS);

		watch.start("Created " + NUMBER_OF_PERSONS + " Persons");

		for (int i = 0; i < NUMBER_OF_PERSONS; i++) {

			Address address = new Address("zip" + i, "city" + i);
			Person person = new Person("Firstname" + i, "Lastname" + i, Arrays.asList(address));
			person.orders.add(new Order(LineItem.generate()));
			person.orders.add(new Order(LineItem.generate()));
			result.add(person);
		}

		watch.stop();

		return result;
	}

	private <T> T executeWatched(String template, WatchCallback<T> callback) {

		watch.start(String.format(template, NUMBER_OF_PERSONS));

		try {
			return callback.doInWatch();
		} finally {
			watch.stop();
		}
	}

	private <T> void executeWatchedWithTime(String template, WatchCallback<?> callback) {
		executeWatched(template, callback);
		printStatistics(null);
	}

	private <T> void executeWatchedWithTimeAndResultSize(String template, WatchCallback<List<T>> callback) {
		printStatistics(executeWatched(template, callback));
	}

	private void printStatistics(Collection<?> result) {

		long time = watch.getLastTaskTimeMillis();
		StringBuilder builder = new StringBuilder(watch.getLastTaskName());

		if (result != null) {
			builder.append(" returned ").append(result.size()).append(" results and");
		}

		builder.append(" took ").append(time).append(" milliseconds");
		System.out.println(builder);
	}

	private static List<Person> toPersons(DBCursor cursor) {

		List<Person> persons = new ArrayList<Person>();

		while (cursor.hasNext()) {
			persons.add(Person.from(cursor.next()));
		}

		return persons;
	}

	static class Person {

		ObjectId id;
		@Indexed
		final String firstname, lastname;
		final List<Address> addresses;
		final Set<Order> orders;

		public Person(String firstname, String lastname, List<Address> addresses) {
			this.firstname = firstname;
			this.lastname = lastname;
			this.addresses = addresses;
			this.orders = new HashSet<Order>();
		}

		public static Person from(DBObject source) {

			BasicDBList addressesSource = (BasicDBList) source.get("addresses");
			List<Address> addresses = new ArrayList<Address>(addressesSource.size());
			for (Object addressSource : addressesSource) {
				addresses.add(Address.from((DBObject) addressSource));
			}

			BasicDBList ordersSource = (BasicDBList) source.get("orders");
			Set<Order> orders = new HashSet<Order>(ordersSource.size());
			for (Object orderSource : ordersSource) {
				orders.add(Order.from((DBObject) orderSource));
			}

			Person person = new Person((String) source.get("firstname"), (String) source.get("lastname"), addresses);
			person.orders.addAll(orders);
			return person;
		}

		public DBObject toDBObject() {

			DBObject dbObject = new BasicDBObject();
			dbObject.put("firstname", firstname);
			dbObject.put("lastname", lastname);
			dbObject.put("addresses", writeAll(addresses));
			dbObject.put("orders", writeAll(orders));
			return dbObject;
		}
	}

	static class Address implements Convertible {

		final String zipCode;
		final String city;
		final Set<AddressType> types;

		public Address(String zipCode, String city) {
			this(zipCode, city, new HashSet<AddressType>(pickRandomNumerOfItemsFrom(Arrays.asList(AddressType.values()))));
		}

		@PersistenceConstructor
		public Address(String zipCode, String city, Set<AddressType> types) {
			this.zipCode = zipCode;
			this.city = city;
			this.types = types;
		}

		public static Address from(DBObject source) {
			String zipCode = (String) source.get("zipCode");
			String city = (String) source.get("city");
			BasicDBList types = (BasicDBList) source.get("types");

			return new Address(zipCode, city, new HashSet<AddressType>(readFromBasicDBList(types, AddressType.class)));
		}

		public DBObject toDBObject() {
			BasicDBObject dbObject = new BasicDBObject();
			dbObject.put("zipCode", zipCode);
			dbObject.put("city", city);
			dbObject.put("types", toBasicDBList(types));
			return dbObject;
		}
	}

	private static <T extends Enum<T>> List<T> readFromBasicDBList(BasicDBList source, Class<T> type) {

		List<T> result = new ArrayList<T>(source.size());
		for (Object object : source) {
			result.add(Enum.valueOf(type, object.toString()));
		}
		return result;
	}

	private static <T extends Enum<T>> BasicDBList toBasicDBList(Collection<T> enums) {
		BasicDBList result = new BasicDBList();
		for (T element : enums) {
			result.add(element.toString());
		}

		return result;
	}

	static class Order implements Convertible {

		static enum Status {
			ORDERED, PAYED, SHIPPED;
		}

		Date createdAt;
		List<LineItem> lineItems;
		Status status;

		public Order(List<LineItem> lineItems, Date createdAt) {
			this.lineItems = lineItems;
			this.createdAt = createdAt;
			this.status = Status.ORDERED;
		}

		@PersistenceConstructor
		public Order(List<LineItem> lineItems, Date createdAt, Status status) {
			this.lineItems = lineItems;
			this.createdAt = createdAt;
			this.status = status;
		}

		public static Order from(DBObject source) {

			BasicDBList lineItemsSource = (BasicDBList) source.get("lineItems");
			List<LineItem> lineItems = new ArrayList<PerformanceTests.LineItem>(lineItemsSource.size());
			for (Object lineItemSource : lineItemsSource) {
				lineItems.add(LineItem.from((DBObject) lineItemSource));
			}

			Date date = (Date) source.get("createdAt");
			Status status = Status.valueOf((String) source.get("status"));
			return new Order(lineItems, date, status);
		}

		public Order(List<LineItem> lineItems) {
			this(lineItems, new Date());
		}

		public DBObject toDBObject() {
			DBObject result = new BasicDBObject();
			result.put("createdAt", createdAt);
			result.put("lineItems", writeAll(lineItems));
			result.put("status", status.toString());
			return result;
		}
	}

	static class LineItem implements Convertible {

		String description;
		double price;
		int amount;

		public LineItem(String description, int amount, double price) {
			this.description = description;
			this.amount = amount;
			this.price = price;
		}

		public static List<LineItem> generate() {

			LineItem iPad = new LineItem("iPad", 1, 649);
			LineItem iPhone = new LineItem("iPhone", 1, 499);
			LineItem macBook = new LineItem("MacBook", 2, 1299);

			return pickRandomNumerOfItemsFrom(Arrays.asList(iPad, iPhone, macBook));
		}

		public static LineItem from(DBObject source) {

			String description = (String) source.get("description");
			double price = (Double) source.get("price");
			int amount = (Integer) source.get("amount");

			return new LineItem(description, amount, price);
		}

		public DBObject toDBObject() {

			BasicDBObject dbObject = new BasicDBObject();
			dbObject.put("description", description);
			dbObject.put("price", price);
			dbObject.put("amount", amount);
			return dbObject;
		}
	}

	private static <T> List<T> pickRandomNumerOfItemsFrom(List<T> source) {

		Assert.isTrue(!source.isEmpty());

		Random random = new Random();
		int numberOfItems = random.nextInt(source.size());
		numberOfItems = numberOfItems == 0 ? 1 : numberOfItems;

		List<T> result = new ArrayList<T>(numberOfItems);
		while (result.size() < numberOfItems) {
			int index = random.nextInt(source.size());
			T candidate = source.get(index);
			if (!result.contains(candidate)) {
				result.add(candidate);
			}
		}

		return result;
	}

	static enum AddressType {
		SHIPPING, BILLING;
	}

	private interface WriteConcernCallback {
		void doWithWriteConcern(String constantName, WriteConcern concern);
	}

	private interface WatchCallback<T> {
		T doInWatch();
	}

	private interface PersonRepository extends MongoRepository<Person, ObjectId> {

		List<Person> findByAddressesZipCodeContaining(String parameter);
	}

	private interface Convertible {

		DBObject toDBObject();
	}

	private static List<DBObject> writeAll(Collection<? extends Convertible> convertibles) {
		List<DBObject> result = new ArrayList<DBObject>();
		for (Convertible convertible : convertibles) {
			result.add(convertible.toDBObject());
		}
		return result;
	}
}

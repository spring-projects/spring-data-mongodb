/*
 * Copyright 2012-2017 the original author or authors.
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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.Constants;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactoryBean;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

/**
 * Test class to execute performance tests for plain MongoDB driver usage, {@link MongoTemplate} and the repositories
 * abstraction.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class PerformanceTests {

	private static final String DATABASE_NAME = "performance";
	private static final int NUMBER_OF_PERSONS = 300;
	private static final int ITERATIONS = 50;
	private static final StopWatch watch = new StopWatch();
	private static final Collection<String> IGNORED_WRITE_CONCERNS = Arrays.asList("MAJORITY", "REPLICAS_SAFE",
			"FSYNC_SAFE", "FSYNCED", "JOURNAL_SAFE", "JOURNALED", "REPLICA_ACKNOWLEDGED", "W2", "W3");
	private static final int COLLECTION_SIZE = 1024 * 1024 * 256; // 256 MB
	private static final Collection<String> COLLECTION_NAMES = Arrays.asList("template", "driver", "person");

	MongoClient mongo;
	MongoTemplate operations;
	PersonRepository repository;
	MongoConverter converter;

	@Before
	public void setUp() throws Exception {

		this.mongo = new MongoClient();

		SimpleMongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(this.mongo, DATABASE_NAME);

		MongoMappingContext context = new MongoMappingContext();
		context.setInitialEntitySet(Collections.singleton(Person.class));
		context.afterPropertiesSet();

		this.converter = new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), context);
		this.operations = new MongoTemplate(new SimpleMongoDbFactory(this.mongo, DATABASE_NAME), converter);

		MongoRepositoryFactoryBean<PersonRepository, Person, ObjectId> factory = new MongoRepositoryFactoryBean<PersonRepository, Person, ObjectId>(
				PersonRepository.class);
		factory.setMongoOperations(operations);
		factory.afterPropertiesSet();

		this.repository = factory.getObject();

	}

	@Test
	public void writeWithWriteConcerns() {
		executeWithWriteConcerns(new WriteConcernCallback() {
			public void doWithWriteConcern(String constantName, WriteConcern concern) {
				writeHeadline("WriteConcern: " + constantName);
				System.out.println(String.format("Writing %s objects using plain driver took %sms", NUMBER_OF_PERSONS,
						writingObjectsUsingPlainDriver(NUMBER_OF_PERSONS)));
				System.out.println(String.format("Writing %s objects using template took %sms", NUMBER_OF_PERSONS,
						writingObjectsUsingMongoTemplate(NUMBER_OF_PERSONS)));
				System.out.println(String.format("Writing %s objects using repository took %sms", NUMBER_OF_PERSONS,
						writingObjectsUsingRepositories(NUMBER_OF_PERSONS)));
				writeFooter();
			}
		});
	}

	@Test
	public void plainConversion() throws InterruptedException {

		Statistics statistics = new Statistics(
				"Plain conversion of " + NUMBER_OF_PERSONS * 100 + " persons - After %s iterations");

		List<Document> documents = getPersonDocuments(NUMBER_OF_PERSONS * 100);

		for (int i = 0; i < ITERATIONS; i++) {
			statistics.registerTime(Api.DIRECT, Mode.READ, convertDirectly(documents));
			statistics.registerTime(Api.CONVERTER, Mode.READ, convertUsingConverter(documents));
		}

		statistics.printResults(ITERATIONS);
	}

	private long convertDirectly(final List<Document> documents) {

		executeWatched(new WatchCallback<List<Person>>() {

			@Override
			public List<Person> doInWatch() {

				List<Person> persons = new ArrayList<PerformanceTests.Person>();

				for (Document document : documents) {
					persons.add(Person.from(new BasicDBObject(document)));
				}

				return persons;
			}
		});

		return watch.getLastTaskTimeMillis();
	}

	private long convertUsingConverter(final List<Document> documents) {

		executeWatched(new WatchCallback<List<Person>>() {

			@Override
			public List<Person> doInWatch() {

				List<Person> persons = new ArrayList<PerformanceTests.Person>();

				for (Document document : documents) {
					persons.add(converter.read(Person.class, document));
				}

				return persons;
			}
		});

		return watch.getLastTaskTimeMillis();
	}

	@Test
	public void writeAndRead() throws Exception {

		mongo.setWriteConcern(WriteConcern.SAFE);

		readsAndWrites(NUMBER_OF_PERSONS, ITERATIONS);
	}

	private void readsAndWrites(int numberOfPersons, int iterations) {

		Statistics statistics = new Statistics("Reading " + numberOfPersons + " - After %s iterations");

		for (int i = 0; i < iterations; i++) {

			setupCollections();

			statistics.registerTime(Api.DRIVER, Mode.WRITE, writingObjectsUsingPlainDriver(numberOfPersons));
			statistics.registerTime(Api.TEMPLATE, Mode.WRITE, writingObjectsUsingMongoTemplate(numberOfPersons));
			statistics.registerTime(Api.REPOSITORY, Mode.WRITE, writingObjectsUsingRepositories(numberOfPersons));

			statistics.registerTime(Api.DRIVER, Mode.READ, readingUsingPlainDriver());
			statistics.registerTime(Api.TEMPLATE, Mode.READ, readingUsingTemplate());
			statistics.registerTime(Api.REPOSITORY, Mode.READ, readingUsingRepository());

			statistics.registerTime(Api.DRIVER, Mode.QUERY, queryUsingPlainDriver());
			statistics.registerTime(Api.TEMPLATE, Mode.QUERY, queryUsingTemplate());
			statistics.registerTime(Api.REPOSITORY, Mode.QUERY, queryUsingRepository());

			if (i > 0 && i % (iterations / 10) == 0) {
				statistics.printResults(i);
			}
		}

		statistics.printResults(iterations);
	}

	private void writeHeadline(String headline) {
		System.out.println(headline);
		System.out.println(createUnderline(headline));
	}

	private void writeFooter() {
		System.out.println();
	}

	private long queryUsingTemplate() {
		executeWatched(() -> operations.find(query(where("addresses.zipCode").regex(".*1.*")), Person.class, "template"));

		return watch.getLastTaskTimeMillis();
	}

	private long queryUsingRepository() {
		executeWatched(() -> repository.findByAddressesZipCodeContaining("1"));

		return watch.getLastTaskTimeMillis();
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
			collection.getDB().command(getCreateCollectionCommand(collectionName));
			collection.createIndex(new BasicDBObject("firstname", -1));
			collection.createIndex(new BasicDBObject("lastname", -1));
		}
	}

	private DBObject getCreateCollectionCommand(String name) {
		DBObject document = new BasicDBObject();
		document.put("createCollection", name);
		document.put("capped", false);
		document.put("size", COLLECTION_SIZE);
		return document;
	}

	private long writingObjectsUsingPlainDriver(int numberOfPersons) {

		DBCollection collection = mongo.getDB(DATABASE_NAME).getCollection("driver");
		List<Person> persons = getPersonObjects(numberOfPersons);

		executeWatched(() -> persons.stream().map(it -> collection.save(new BasicDBObject(it.toDocument()))));

		return watch.getLastTaskTimeMillis();
	}

	private long writingObjectsUsingRepositories(int numberOfPersons) {

		List<Person> persons = getPersonObjects(numberOfPersons);

		executeWatched(() -> repository.saveAll(persons));

		return watch.getLastTaskTimeMillis();
	}

	private long writingObjectsUsingMongoTemplate(int numberOfPersons) {

		List<Person> persons = getPersonObjects(numberOfPersons);

		executeWatched(() -> persons.stream()//
				.peek(it -> operations.save(it, "template"))//
				.collect(Collectors.toList()));

		return watch.getLastTaskTimeMillis();
	}

	private long readingUsingPlainDriver() {

		executeWatched(() -> toPersons(mongo.getDB(DATABASE_NAME).getCollection("driver").find()));

		return watch.getLastTaskTimeMillis();
	}

	private long readingUsingTemplate() {
		executeWatched(() -> operations.findAll(Person.class, "template"));

		return watch.getLastTaskTimeMillis();
	}

	private long readingUsingRepository() {
		executeWatched(repository::findAll);

		return watch.getLastTaskTimeMillis();
	}

	private long queryUsingPlainDriver() {

		executeWatched(() -> {

			DBCollection collection = mongo.getDB(DATABASE_NAME).getCollection("driver");

			BasicDBObject regex = new BasicDBObject("$regex", Pattern.compile(".*1.*"));
			BasicDBObject query = new BasicDBObject("addresses.zipCode", regex);
			return toPersons(collection.find(query));
		});

		return watch.getLastTaskTimeMillis();
	}

	private List<Person> getPersonObjects(int numberOfPersons) {

		List<Person> result = new ArrayList<Person>();

		for (int i = 0; i < numberOfPersons; i++) {

			List<Address> addresses = new ArrayList<Address>();

			for (int a = 0; a < 5; a++) {
				addresses.add(new Address("zip" + a, "city" + a));
			}

			Person person = new Person("Firstname" + i, "Lastname" + i, addresses);

			for (int o = 0; o < 10; o++) {
				person.orders.add(new Order(LineItem.generate()));
			}

			result.add(person);
		}

		return result;
	}

	private List<Document> getPersonDocuments(int numberOfPersons) {

		List<Document> documents = new ArrayList<Document>(numberOfPersons);

		for (Person person : getPersonObjects(numberOfPersons)) {
			documents.add(person.toDocument());
		}

		return documents;
	}

	private <T> T executeWatched(WatchCallback<T> callback) {

		watch.start();

		try {
			return callback.doInWatch();
		} finally {
			watch.stop();
		}
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
		String firstname, lastname;
		List<Address> addresses;
		Set<Order> orders;

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
				addresses.add(Address.from((Document) addressSource));
			}

			BasicDBList ordersSource = (BasicDBList) source.get("orders");
			Set<Order> orders = new HashSet<Order>(ordersSource.size());
			for (Object orderSource : ordersSource) {
				orders.add(Order.from((Document) orderSource));
			}

			Person person = new Person((String) source.get("firstname"), (String) source.get("lastname"), addresses);
			person.orders.addAll(orders);
			return person;
		}

		public Document toDocument() {

			Document document = new Document();
			document.put("firstname", firstname);
			document.put("lastname", lastname);
			document.put("addresses", writeAll(addresses));
			document.put("orders", writeAll(orders));
			return document;
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

		public static Address from(Document source) {
			String zipCode = (String) source.get("zipCode");
			String city = (String) source.get("city");
			BasicDBList types = (BasicDBList) source.get("types");

			return new Address(zipCode, city, new HashSet<AddressType>(readFromBasicDBList(types, AddressType.class)));
		}

		public Document toDocument() {
			Document document = new Document();
			document.put("zipCode", zipCode);
			document.put("city", city);
			document.put("types", toBasicDBList(types));
			return document;
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

		public static Order from(Document source) {

			BasicDBList lineItemsSource = (BasicDBList) source.get("lineItems");
			List<LineItem> lineItems = new ArrayList<PerformanceTests.LineItem>(lineItemsSource.size());
			for (Object lineItemSource : lineItemsSource) {
				lineItems.add(LineItem.from((Document) lineItemSource));
			}

			Date date = (Date) source.get("createdAt");
			Status status = Status.valueOf((String) source.get("status"));
			return new Order(lineItems, date, status);
		}

		public Order(List<LineItem> lineItems) {
			this(lineItems, new Date());
		}

		public Document toDocument() {
			Document result = new Document();
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

		public static LineItem from(Document source) {

			String description = (String) source.get("description");
			double price = (Double) source.get("price");
			int amount = (Integer) source.get("amount");

			return new LineItem(description, amount, price);
		}

		public Document toDocument() {

			Document document = new Document();
			document.put("description", description);
			document.put("price", price);
			document.put("amount", amount);
			return document;
		}
	}

	private static <T> List<T> pickRandomNumerOfItemsFrom(List<T> source) {

		Assert.isTrue(!source.isEmpty(), "Source must not be empty!");

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

		Document toDocument();
	}

	private static BasicDBList writeAll(Collection<? extends Convertible> convertibles) {
		BasicDBList result = new BasicDBList();
		for (Convertible convertible : convertibles) {
			result.add(convertible.toDocument());
		}
		return result;
	}

	static enum Api {
		DRIVER, TEMPLATE, REPOSITORY, DIRECT, CONVERTER;
	}

	static enum Mode {
		WRITE, READ, QUERY;
	}

	private static class Statistics {

		private final String headline;
		private final Map<Mode, ModeTimes> times;

		public Statistics(String headline) {

			this.headline = headline;
			this.times = new HashMap<Mode, ModeTimes>();

			for (Mode mode : Mode.values()) {
				times.put(mode, new ModeTimes(mode));
			}
		}

		public void registerTime(Api api, Mode mode, double time) {
			times.get(mode).add(api, time);
		}

		public void printResults(int iterations) {

			String title = String.format(headline, iterations);

			System.out.println(title);
			System.out.println(createUnderline(title));

			StringBuilder builder = new StringBuilder();
			for (Mode mode : Mode.values()) {
				String print = times.get(mode).print();
				if (!print.isEmpty()) {
					builder.append(print).append('\n');
				}
			}

			System.out.println(builder.toString());
		}

		@Override
		public String toString() {

			StringBuilder builder = new StringBuilder(times.size());

			for (ModeTimes times : this.times.values()) {
				builder.append(times.toString());
			}

			return builder.toString();
		}
	}

	private static String createUnderline(String input) {

		StringBuilder builder = new StringBuilder(input.length());

		for (int i = 0; i < input.length(); i++) {
			builder.append("-");
		}

		return builder.toString();
	}

	static class ApiTimes {

		private static final String TIME_TEMPLATE = "%s %s time -\tAverage: %sms%s,%sMedian: %sms%s";

		private static final DecimalFormat TIME_FORMAT;
		private static final DecimalFormat DEVIATION_FORMAT;

		static {

			TIME_FORMAT = new DecimalFormat("0.00");

			DEVIATION_FORMAT = new DecimalFormat("0.00");
			DEVIATION_FORMAT.setPositivePrefix("+");
		}

		private final Api api;
		private final Mode mode;
		private final List<Double> times;

		public ApiTimes(Api api, Mode mode) {
			this.api = api;
			this.mode = mode;
			this.times = new ArrayList<Double>();
		}

		public void add(double time) {
			this.times.add(time);
		}

		public boolean hasTimes() {
			return !times.isEmpty();
		}

		public double getAverage() {

			double result = 0;

			for (Double time : times) {
				result += time;
			}

			return result == 0.0 ? 0.0 : result / times.size();
		}

		public double getMedian() {

			if (times.isEmpty()) {
				return 0.0;
			}

			ArrayList<Double> list = new ArrayList<Double>(times);
			Collections.sort(list);

			int size = list.size();

			if (size % 2 == 0) {
				return (list.get(size / 2 - 1) + list.get(size / 2)) / 2;
			} else {
				return list.get(size / 2);
			}
		}

		private double getDeviationFrom(double otherAverage) {

			double average = getAverage();
			return average * 100 / otherAverage - 100;
		}

		private double getMediaDeviationFrom(double otherMedian) {
			double median = getMedian();
			return median * 100 / otherMedian - 100;
		}

		public String print() {

			if (times.isEmpty()) {
				return "";
			}

			return basicPrint("", "\t\t", "") + '\n';
		}

		private String basicPrint(String extension, String middle, String foo) {
			return String.format(TIME_TEMPLATE, api, mode, TIME_FORMAT.format(getAverage()), extension, middle,
					TIME_FORMAT.format(getMedian()), foo);
		}

		public String print(double referenceAverage, double referenceMedian) {

			if (times.isEmpty()) {
				return "";
			}

			return basicPrint(String.format(" %s%%", DEVIATION_FORMAT.format(getDeviationFrom(referenceAverage))), "\t",
					String.format(" %s%%", DEVIATION_FORMAT.format(getMediaDeviationFrom(referenceMedian)))) + '\n';
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return times.isEmpty() ? ""
					: String.format("%s, %s: %s", api, mode, StringUtils.collectionToCommaDelimitedString(times)) + '\n';
		}
	}

	static class ModeTimes {

		private final Map<Api, ApiTimes> times;

		public ModeTimes(Mode mode) {

			this.times = new HashMap<Api, ApiTimes>();

			for (Api api : Api.values()) {
				this.times.put(api, new ApiTimes(api, mode));
			}
		}

		public void add(Api api, double time) {
			times.get(api).add(time);
		}

		@SuppressWarnings("null")
		public String print() {

			if (times.isEmpty()) {
				return "";
			}

			Double previousTime = null;
			Double previousMedian = null;
			StringBuilder builder = new StringBuilder();

			for (Api api : Api.values()) {

				ApiTimes apiTimes = times.get(api);

				if (!apiTimes.hasTimes()) {
					continue;
				}

				if (previousTime == null) {
					builder.append(apiTimes.print());
					previousTime = apiTimes.getAverage();
					previousMedian = apiTimes.getMedian();
				} else {
					builder.append(apiTimes.print(previousTime, previousMedian));
				}
			}

			return builder.toString();
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {

			StringBuilder builder = new StringBuilder(times.size());

			for (ApiTimes times : this.times.values()) {
				builder.append(times.toString());
			}

			return builder.toString();
		}
	}
}

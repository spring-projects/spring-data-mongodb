/*
 * Copyright 2016-2017 the original author or authors.
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

import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
import java.util.Optional;
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
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DbRefProxyHandler;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DbRefResolverCallback;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.support.ReactiveMongoRepositoryFactory;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Test class to execute performance tests for plain Reactive Streams MongoDB driver usage,
 * {@link ReactiveMongoOperations} and the repositories abstraction.
 *
 * @author Mark Paluch
 */
public class ReactivePerformanceTests {

	private static final String DATABASE_NAME = "performance";
	private static final int NUMBER_OF_PERSONS = 300;
	private static final int ITERATIONS = 50;
	private static final StopWatch watch = new StopWatch();
	private static final Collection<String> IGNORED_WRITE_CONCERNS = Arrays.asList("MAJORITY", "REPLICAS_SAFE",
			"FSYNC_SAFE", "FSYNCED", "JOURNAL_SAFE", "JOURNALED", "REPLICA_ACKNOWLEDGED");
	private static final int COLLECTION_SIZE = 1024 * 1024 * 256; // 256 MB
	private static final Collection<String> COLLECTION_NAMES = Arrays.asList("template", "driver", "person");

	MongoClient mongo;
	ReactiveMongoTemplate operations;
	ReactivePersonRepository repository;
	MongoConverter converter;

	@Before
	public void setUp() throws Exception {

		mongo = MongoClients.create();

		SimpleReactiveMongoDatabaseFactory mongoDbFactory = new SimpleReactiveMongoDatabaseFactory(this.mongo,
				DATABASE_NAME);

		MongoMappingContext context = new MongoMappingContext();
		context.setInitialEntitySet(Collections.singleton(Person.class));
		context.afterPropertiesSet();

		converter = new MappingMongoConverter(new DbRefResolver() {
			@Override
			public Optional<Object> resolveDbRef(MongoPersistentProperty property, DBRef dbref, DbRefResolverCallback callback,
												 DbRefProxyHandler proxyHandler) {
				return Optional.empty();
			}

			@Override
			public DBRef createDbRef(org.springframework.data.mongodb.core.mapping.DBRef annotation,
					MongoPersistentEntity<?> entity, Object id) {
				return null;
			}

			@Override
			public Document fetch(DBRef dbRef) {
				return null;
			}

			@Override
			public List<Document> bulkFetch(List<DBRef> dbRefs) {
				return null;
			}
		}, context);
		operations = new ReactiveMongoTemplate(mongoDbFactory, converter);

		ReactiveMongoRepositoryFactory factory = new ReactiveMongoRepositoryFactory(operations);
		repository = factory.getRepository(ReactivePersonRepository.class);
	}

	@Test // DATAMONGO-1444
	public void writeWithWriteConcerns() {
		executeWithWriteConcerns((constantName, concern) -> {

			writeHeadline("WriteConcern: " + constantName);
			System.out.println(String.format("Writing %s objects using plain driver took %sms", NUMBER_OF_PERSONS,
					writingObjectsUsingPlainDriver(NUMBER_OF_PERSONS, concern)));
			System.out.println(String.format("Writing %s objects using template took %sms", NUMBER_OF_PERSONS,
					writingObjectsUsingMongoTemplate(NUMBER_OF_PERSONS, concern)));
			System.out.println(String.format("Writing %s objects using repository took %sms", NUMBER_OF_PERSONS,
					writingObjectsUsingRepositories(NUMBER_OF_PERSONS, concern)));

			System.out.println(String.format("Writing %s objects async using plain driver took %sms", NUMBER_OF_PERSONS,
					writingAsyncObjectsUsingPlainDriver(NUMBER_OF_PERSONS, concern)));
			System.out.println(String.format("Writing %s objects async using template took %sms", NUMBER_OF_PERSONS,
					writingAsyncObjectsUsingMongoTemplate(NUMBER_OF_PERSONS, concern)));
			System.out.println(String.format("Writing %s objects async using repository took %sms", NUMBER_OF_PERSONS,
					writingAsyncObjectsUsingRepositories(NUMBER_OF_PERSONS, concern)));
			writeFooter();
		});
	}

	@Test
	public void plainConversion() throws InterruptedException {

		Statistics statistics = new Statistics(
				"Plain conversion of " + NUMBER_OF_PERSONS * 100 + " persons - After %s iterations");

		List<Document> dbObjects = getPersonDocuments(NUMBER_OF_PERSONS * 100);

		for (int i = 0; i < ITERATIONS; i++) {
			statistics.registerTime(Api.DIRECT, Mode.READ, convertDirectly(dbObjects));
			statistics.registerTime(Api.CONVERTER, Mode.READ, convertUsingConverter(dbObjects));
		}

		statistics.printResults(ITERATIONS);
	}

	private long convertDirectly(final List<Document> dbObjects) {

		executeWatched(() -> {

			List<Person> persons = new ArrayList<Person>();

			for (Document dbObject : dbObjects) {
				persons.add(Person.from(new Document(dbObject)));
			}

			return persons;
		});

		return watch.getLastTaskTimeMillis();
	}

	private long convertUsingConverter(final List<Document> dbObjects) {

		executeWatched(() -> {

			List<Person> persons = new ArrayList<Person>();

			for (Document dbObject : dbObjects) {
				persons.add(converter.read(Person.class, dbObject));
			}

			return persons;
		});

		return watch.getLastTaskTimeMillis();
	}

	@Test // DATAMONGO-1444
	public void writeAndRead() throws Exception {

		readsAndWrites(NUMBER_OF_PERSONS, ITERATIONS, WriteConcern.SAFE);
	}

	private void readsAndWrites(int numberOfPersons, int iterations, WriteConcern concern) {

		Statistics statistics = new Statistics("Reading " + numberOfPersons + " - After %s iterations");

		for (int i = 0; i < iterations; i++) {

			setupCollections();

			statistics.registerTime(Api.DRIVER, Mode.WRITE, writingObjectsUsingPlainDriver(numberOfPersons, concern));
			statistics.registerTime(Api.TEMPLATE, Mode.WRITE, writingObjectsUsingMongoTemplate(numberOfPersons, concern));
			statistics.registerTime(Api.REPOSITORY, Mode.WRITE, writingObjectsUsingRepositories(numberOfPersons, concern));

			statistics.registerTime(Api.DRIVER, Mode.WRITE_ASYNC,
					writingAsyncObjectsUsingPlainDriver(numberOfPersons, concern));
			statistics.registerTime(Api.TEMPLATE, Mode.WRITE_ASYNC,
					writingAsyncObjectsUsingMongoTemplate(numberOfPersons, concern));
			statistics.registerTime(Api.REPOSITORY, Mode.WRITE_ASYNC,
					writingAsyncObjectsUsingRepositories(numberOfPersons, concern));

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
		executeWatched(() -> {
			Query query = query(where("addresses.zipCode").regex(".*1.*"));
			return operations.find(query, Person.class, "template").collectList().block();
		});

		return watch.getLastTaskTimeMillis();
	}

	private long queryUsingRepository() {
		executeWatched(() -> repository.findByAddressesZipCodeContaining("1").collectList().block());

		return watch.getLastTaskTimeMillis();
	}

	private void executeWithWriteConcerns(WriteConcernCallback callback) {

		Constants constants = new Constants(WriteConcern.class);

		for (String constantName : constants.getNames(null)) {

			if (IGNORED_WRITE_CONCERNS.contains(constantName)) {
				continue;
			}

			WriteConcern writeConcern = (WriteConcern) constants.asObject(constantName);

			setupCollections();

			callback.doWithWriteConcern(constantName, writeConcern);
		}
	}

	private void setupCollections() {

		MongoDatabase db = this.mongo.getDatabase(DATABASE_NAME);

		for (String collectionName : COLLECTION_NAMES) {
			MongoCollection<Document> collection = db.getCollection(collectionName);
			Mono.from(collection.drop()).block();
			Mono.from(db.createCollection(collectionName, getCreateCollectionOptions())).block();
			collection.createIndex(new BasicDBObject("firstname", -1));
			collection.createIndex(new BasicDBObject("lastname", -1));
		}
	}

	private CreateCollectionOptions getCreateCollectionOptions() {
		CreateCollectionOptions options = new CreateCollectionOptions();
		return options.sizeInBytes(COLLECTION_SIZE).capped(false);
	}

	private long writingObjectsUsingPlainDriver(int numberOfPersons, WriteConcern concern) {

		final MongoCollection<Document> collection = mongo.getDatabase(DATABASE_NAME).getCollection("driver")
				.withWriteConcern(concern);
		final List<Person> persons = getPersonObjects(numberOfPersons);

		executeWatched(new WatchCallback<Void>() {
			public Void doInWatch() {
				for (Person person : persons) {
					Mono.from(collection.insertOne(new Document(person.toDocument()))).block();
				}
				return null;
			}
		});

		return watch.getLastTaskTimeMillis();
	}

	private long writingObjectsUsingRepositories(int numberOfPersons, WriteConcern concern) {

		final List<Person> persons = getPersonObjects(numberOfPersons);
		operations.setWriteConcern(concern);
		executeWatched(new WatchCallback<Void>() {
			public Void doInWatch() {
				for (Person person : persons) {
					repository.save(person).block();
				}
				return null;
			}
		});

		return watch.getLastTaskTimeMillis();
	}

	private long writingObjectsUsingMongoTemplate(int numberOfPersons, WriteConcern concern) {

		final List<Person> persons = getPersonObjects(numberOfPersons);

		executeWatched(new WatchCallback<Void>() {
			public Void doInWatch() {
				operations.setWriteConcern(concern);
				for (Person person : persons) {
					Mono.from(operations.save(person, "template")).block();
				}
				return null;
			}
		});

		return watch.getLastTaskTimeMillis();
	}

	private long writingAsyncObjectsUsingPlainDriver(int numberOfPersons, WriteConcern concern) {

		final MongoCollection<Document> collection = mongo.getDatabase(DATABASE_NAME).getCollection("driver")
				.withWriteConcern(concern);
		final List<Person> persons = getPersonObjects(numberOfPersons);

		executeWatched(new WatchCallback<Void>() {
			public Void doInWatch() {

				Flux.from(collection
						.insertMany(persons.stream().map(person -> new Document(person.toDocument())).collect(Collectors.toList())))
						.then().block();
				return null;
			}
		});

		return watch.getLastTaskTimeMillis();
	}

	private long writingAsyncObjectsUsingRepositories(int numberOfPersons, WriteConcern concern) {

		final List<Person> persons = getPersonObjects(numberOfPersons);
		operations.setWriteConcern(concern);
		executeWatched(new WatchCallback<Void>() {
			public Void doInWatch() {
				repository.save(persons).then().block();
				return null;
			}
		});

		return watch.getLastTaskTimeMillis();
	}

	private long writingAsyncObjectsUsingMongoTemplate(int numberOfPersons, WriteConcern concern) {

		final List<Person> persons = getPersonObjects(numberOfPersons);

		executeWatched(new WatchCallback<Void>() {
			public Void doInWatch() {
				operations.setWriteConcern(concern);
				Flux.from(operations.insertAll(persons)).then().block();
				return null;
			}
		});

		return watch.getLastTaskTimeMillis();
	}

	private long readingUsingPlainDriver() {

		executeWatched(() -> Flux.from(mongo.getDatabase(DATABASE_NAME).getCollection("driver").find()).map(Person::from)
				.collectList().block());

		return watch.getLastTaskTimeMillis();
	}

	private long readingUsingTemplate() {
		executeWatched(() -> operations.findAll(Person.class, "template").collectList().block());

		return watch.getLastTaskTimeMillis();
	}

	private long readingUsingRepository() {
		executeWatched(() -> repository.findAll().collectList().block());

		return watch.getLastTaskTimeMillis();
	}

	private long queryUsingPlainDriver() {

		executeWatched(() -> {

			MongoCollection<Document> collection = mongo.getDatabase(DATABASE_NAME).getCollection("driver");

			Document regex = new Document("$regex", Pattern.compile(".*1.*"));
			Document query = new Document("addresses.zipCode", regex);
			return Flux.from(collection.find(query)).map(Person::from).collectList().block();
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

		List<Document> dbObjects = new ArrayList<Document>(numberOfPersons);

		for (Person person : getPersonObjects(numberOfPersons)) {
			dbObjects.add(person.toDocument());
		}

		return dbObjects;
	}

	private <T> T executeWatched(WatchCallback<T> callback) {

		watch.start();

		try {
			return callback.doInWatch();
		} finally {
			watch.stop();
		}
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

		public static Person from(Document source) {

			List<Document> addressesSource = (List<Document>) source.get("addresses");
			List<Address> addresses = new ArrayList<Address>(addressesSource.size());
			for (Object addressSource : addressesSource) {
				addresses.add(Address.from((Document) addressSource));
			}

			List<Document> ordersSource = (List<Document>) source.get("orders");
			Set<Order> orders = new HashSet<Order>(ordersSource.size());
			for (Object orderSource : ordersSource) {
				orders.add(Order.from((Document) orderSource));
			}

			Person person = new Person((String) source.get("firstname"), (String) source.get("lastname"), addresses);
			person.orders.addAll(orders);
			return person;
		}

		public Document toDocument() {

			Document dbObject = new Document();
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

		public static Address from(Document source) {
			String zipCode = (String) source.get("zipCode");
			String city = (String) source.get("city");
			List types = (List) source.get("types");

			return new Address(zipCode, city, new HashSet<AddressType>(fromList(types, AddressType.class)));
		}

		public Document toDocument() {
			Document dbObject = new Document();
			dbObject.put("zipCode", zipCode);
			dbObject.put("city", city);
			dbObject.put("types", toList(types));
			return dbObject;
		}
	}

	private static <T extends Enum<T>> List<T> fromList(List source, Class<T> type) {

		List<T> result = new ArrayList<T>(source.size());
		for (Object object : source) {
			result.add(Enum.valueOf(type, object.toString()));
		}
		return result;
	}

	private static <T extends Enum<T>> List toList(Collection<T> enums) {
		List<String> result = new ArrayList<>();
		for (T element : enums) {
			result.add(element.toString());
		}

		return result;
	}

	static class Order implements Convertible {

		enum Status {
			ORDERED, PAYED, SHIPPED
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

			List lineItemsSource = (List) source.get("lineItems");
			List<LineItem> lineItems = new ArrayList<ReactivePerformanceTests.LineItem>(lineItemsSource.size());
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

			Document dbObject = new Document();
			dbObject.put("description", description);
			dbObject.put("price", price);
			dbObject.put("amount", amount);
			return dbObject;
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

	enum AddressType {
		SHIPPING, BILLING
	}

	private interface WriteConcernCallback {
		void doWithWriteConcern(String constantName, WriteConcern concern);
	}

	private interface WatchCallback<T> {
		T doInWatch();
	}

	private interface ReactivePersonRepository extends ReactiveMongoRepository<Person, ObjectId> {

		Flux<Person> findByAddressesZipCodeContaining(String parameter);
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

	enum Api {
		DRIVER, TEMPLATE, REPOSITORY, DIRECT, CONVERTER
	}

	enum Mode {
		WRITE, READ, QUERY, WRITE_ASYNC
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

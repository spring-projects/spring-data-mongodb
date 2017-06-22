/*
 * Copyright 2017 the original author or authors.
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
package spring.data.mongodb.core.convert;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import spring.data.microbenchmark.AbstractMicrobenchmark;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/**
 * @author Christoph Strobl
 */
@State(Scope.Benchmark)
public class MappingMongoConverterBenchmark extends AbstractMicrobenchmark {

	private MongoClient client;
	private MongoMappingContext mappingContext;
	private MappingMongoConverter converter;
	private Document plainSource, sourceWithAddress;
	private Customer customer;

	private Document complexSource;
	private SlightlyMoreComplexObject complexObject;

	@Setup
	public void setUp() throws Exception {

		client = new MongoClient(new ServerAddress());

		this.mappingContext = new MongoMappingContext();
		this.mappingContext.setInitialEntitySet(Collections.singleton(Customer.class));
		this.mappingContext.afterPropertiesSet();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(new SimpleMongoDbFactory(client, "benchmark"));

		this.converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		this.converter.setCustomConversions(new CustomConversions(Collections.emptyList()));
		this.converter.afterPropertiesSet();

		this.plainSource = new Document("firstname", "Dave").append("lastname", "Matthews");

		Document address = new Document("zipCode", "ABCDE").append("city", "Some Place");

		this.sourceWithAddress = new Document("firstname", "Dave").//
				append("lastname", "Matthews").//
				append("address", address);

		this.customer = new Customer("Dave", "Matthews", new Address("zipCode", "City"));

		complexObject = new SlightlyMoreComplexObject();
		complexObject.id = UUID.randomUUID().toString();
		complexObject.addressList = Arrays.asList(new Address("zip-1", "city-1"), new Address("zip-2", "city-2"));
		complexObject.customer = customer;
		complexObject.customerMap = new LinkedHashMap<>();
		complexObject.customerMap.put("dave", customer);
		complexObject.customerMap.put("deborah", new Customer("Deborah Anne", "Dyer", new Address("?", "london")));
		complexObject.customerMap.put("eddie", new Customer("Eddie", "Vedder", new Address("??", "Seattle")));
		complexObject.intOne = Integer.MIN_VALUE;
		complexObject.intTwo = Integer.MAX_VALUE;
		complexObject.location = new Point(-33.865143, 151.209900);
		complexObject.renamedField = "supercalifragilisticexpialidocious";
		complexObject.stringOne = "¯\\_(ツ)_/¯";
		complexObject.stringTwo = " (╯°□°）╯︵ ┻━┻";

		complexSource = Document.parse(
				"{ \"_id\" : \"517f6aee-e9e0-44f0-88ed-f3694a019f27\", \"intOne\" : -2147483648, \"intTwo\" : 2147483647, \"stringOne\" : \"¯\\\\_(ツ)_/¯\", \"stringTwo\" : \" (╯°□°）╯︵ ┻━┻\", \"explicit-field-name\" : \"supercalifragilisticexpialidocious\", \"location\" : { \"x\" : -33.865143, \"y\" : 151.2099 }, \"customer\" : { \"firstname\" : \"Dave\", \"lastname\" : \"Matthews\", \"address\" : { \"zipCode\" : \"zipCode\", \"city\" : \"City\" } }, \"addressList\" : [{ \"zipCode\" : \"zip-1\", \"city\" : \"city-1\" }, { \"zipCode\" : \"zip-2\", \"city\" : \"city-2\" }], \"customerMap\" : { \"dave\" : { \"firstname\" : \"Dave\", \"lastname\" : \"Matthews\", \"address\" : { \"zipCode\" : \"zipCode\", \"city\" : \"City\" } }, \"deborah\" : { \"firstname\" : \"Deborah Anne\", \"lastname\" : \"Dyer\", \"address\" : { \"zipCode\" : \"?\", \"city\" : \"london\" } }, \"eddie\" : { \"firstname\" : \"Eddie\", \"lastname\" : \"Vedder\", \"address\" : { \"zipCode\" : \"??\", \"city\" : \"Seattle\" } } }, \"_class\" : \"spring.data.mongodb.core.convert.MappingMongoConverterBenchmark$SlightlyMoreComplexObject\" }");

	}

	@TearDown
	public void tearDown() {
		client.close();
	}

	@Benchmark
	public Customer readObject() {
		return converter.read(Customer.class, plainSource);
	}

	@Benchmark
	public Customer readObjectWithNested() {
		return converter.read(Customer.class, sourceWithAddress);
	}

	@Benchmark
	public Document writeObject() {

		Document sink = new Document();
		converter.write(customer, sink);
		return sink;
	}

	@Benchmark
	public Object complexRead() {
		return converter.read(SlightlyMoreComplexObject.class, complexSource);
	}

	@Benchmark
	public Object complexWrite() {

		Document sink = new Document();
		converter.write(complexObject, sink);
		return sink;
	}

	@Getter
	@RequiredArgsConstructor
	static class Customer {

		private @Id ObjectId id;
		private final String firstname, lastname;
		private final Address address;
	}

	@Getter
	@AllArgsConstructor
	static class Address {
		private String zipCode, city;
	}

	@Data
	static class SlightlyMoreComplexObject {

		@Id String id;
		int intOne, intTwo;
		String stringOne, stringTwo;
		@Field("explicit-field-name") String renamedField;
		Point location;
		Customer customer;
		List<Address> addressList;
		Map<String, Customer> customerMap;
	}
}

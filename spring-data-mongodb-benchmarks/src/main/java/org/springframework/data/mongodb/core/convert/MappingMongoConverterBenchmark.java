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
package org.springframework.data.mongodb.core.convert;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.microbenchmark.AbstractMicrobenchmark;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

/**
 * @author Christoph Strobl
 */
@State(Scope.Benchmark)
public class MappingMongoConverterBenchmark extends AbstractMicrobenchmark {

	private static final String DB_NAME = "mapping-mongo-converter-benchmark";

	private MongoClient client;
	private MongoMappingContext mappingContext;
	private MappingMongoConverter converter;
	private BasicDBObject documentWith2Properties, documentWith2PropertiesAnd1Nested;
	private Customer objectWith2PropertiesAnd1Nested;

	private BasicDBObject documentWithFlatAndComplexPropertiesPlusListAndMap;
	private SlightlyMoreComplexObject objectWithFlatAndComplexPropertiesPlusListAndMap;

	@Setup
	public void setUp() throws Exception {

		client = new MongoClient(new ServerAddress());

		this.mappingContext = new MongoMappingContext();
		this.mappingContext.setInitialEntitySet(Collections.singleton(Customer.class));
		this.mappingContext.afterPropertiesSet();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(new SimpleMongoDbFactory(client, DB_NAME));

		this.converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		this.converter.setCustomConversions(new CustomConversions(Collections.emptyList()));
		this.converter.afterPropertiesSet();

		// just a flat document
		this.documentWith2Properties = new BasicDBObject("firstname", "Dave").append("lastname", "Matthews");

		// document with a nested one
		BasicDBObject address = new BasicDBObject("zipCode", "ABCDE").append("city", "Some Place");
		this.documentWith2PropertiesAnd1Nested = new BasicDBObject("firstname", "Dave").//
				append("lastname", "Matthews").//
				append("address", address);

		// object equivalent of documentWith2PropertiesAnd1Nested
		this.objectWith2PropertiesAnd1Nested = new Customer("Dave", "Matthews", new Address("zipCode", "City"));

		// a bit more challenging object with list & map conversion.
		objectWithFlatAndComplexPropertiesPlusListAndMap = new SlightlyMoreComplexObject();
		objectWithFlatAndComplexPropertiesPlusListAndMap.id = UUID.randomUUID().toString();
		objectWithFlatAndComplexPropertiesPlusListAndMap.addressList = Arrays.asList(new Address("zip-1", "city-1"),
				new Address("zip-2", "city-2"));
		objectWithFlatAndComplexPropertiesPlusListAndMap.customer = objectWith2PropertiesAnd1Nested;
		objectWithFlatAndComplexPropertiesPlusListAndMap.customerMap = new LinkedHashMap<String, Customer>();
		objectWithFlatAndComplexPropertiesPlusListAndMap.customerMap.put("dave", objectWith2PropertiesAnd1Nested);
		objectWithFlatAndComplexPropertiesPlusListAndMap.customerMap.put("deborah",
				new Customer("Deborah Anne", "Dyer", new Address("?", "london")));
		objectWithFlatAndComplexPropertiesPlusListAndMap.customerMap.put("eddie",
				new Customer("Eddie", "Vedder", new Address("??", "Seattle")));
		objectWithFlatAndComplexPropertiesPlusListAndMap.intOne = Integer.MIN_VALUE;
		objectWithFlatAndComplexPropertiesPlusListAndMap.intTwo = Integer.MAX_VALUE;
		objectWithFlatAndComplexPropertiesPlusListAndMap.location = new Point(-33.865143, 151.209900);
		objectWithFlatAndComplexPropertiesPlusListAndMap.renamedField = "supercalifragilisticexpialidocious";
		objectWithFlatAndComplexPropertiesPlusListAndMap.stringOne = "¯\\_(ツ)_/¯";
		objectWithFlatAndComplexPropertiesPlusListAndMap.stringTwo = " (╯°□°）╯︵ ┻━┻";

		// JSON equivalent of objectWithFlatAndComplexPropertiesPlusListAndMap
		documentWithFlatAndComplexPropertiesPlusListAndMap = (BasicDBObject) JSON.parse(
				"{ \"_id\" : \"517f6aee-e9e0-44f0-88ed-f3694a019f27\", \"intOne\" : -2147483648, \"intTwo\" : 2147483647, \"stringOne\" : \"¯\\\\_(ツ)_/¯\", \"stringTwo\" : \" (╯°□°）╯︵ ┻━┻\", \"explicit-field-name\" : \"supercalifragilisticexpialidocious\", \"location\" : { \"x\" : -33.865143, \"y\" : 151.2099 }, \"objectWith2PropertiesAnd1Nested\" : { \"firstname\" : \"Dave\", \"lastname\" : \"Matthews\", \"address\" : { \"zipCode\" : \"zipCode\", \"city\" : \"City\" } }, \"addressList\" : [{ \"zipCode\" : \"zip-1\", \"city\" : \"city-1\" }, { \"zipCode\" : \"zip-2\", \"city\" : \"city-2\" }], \"customerMap\" : { \"dave\" : { \"firstname\" : \"Dave\", \"lastname\" : \"Matthews\", \"address\" : { \"zipCode\" : \"zipCode\", \"city\" : \"City\" } }, \"deborah\" : { \"firstname\" : \"Deborah Anne\", \"lastname\" : \"Dyer\", \"address\" : { \"zipCode\" : \"?\", \"city\" : \"london\" } }, \"eddie\" : { \"firstname\" : \"Eddie\", \"lastname\" : \"Vedder\", \"address\" : { \"zipCode\" : \"??\", \"city\" : \"Seattle\" } } }, \"_class\" : \"org.springframework.data.mongodb.core.convert.MappingMongoConverterBenchmark$SlightlyMoreComplexObject\" }");

	}

	@TearDown
	public void tearDown() {

		client.dropDatabase(DB_NAME);
		client.close();
	}

	@Benchmark // DATAMONGO-1720
	public Customer readObjectWith2Properties() {
		return converter.read(Customer.class, documentWith2Properties);
	}

	@Benchmark // DATAMONGO-1720
	public Customer readObjectWith2PropertiesAnd1NestedObject() {
		return converter.read(Customer.class, documentWith2PropertiesAnd1Nested);
	}

	@Benchmark // DATAMONGO-1720
	public BasicDBObject writeObjectWith2PropertiesAnd1NestedObject() {

		BasicDBObject sink = new BasicDBObject();
		converter.write(objectWith2PropertiesAnd1Nested, sink);
		return sink;
	}

	@Benchmark // DATAMONGO-1720
	public Object readObjectWithListAndMapsOfComplexType() {
		return converter.read(SlightlyMoreComplexObject.class, documentWithFlatAndComplexPropertiesPlusListAndMap);
	}

	@Benchmark // DATAMONGO-1720
	public Object writeObjectWithListAndMapsOfComplexType() {

		BasicDBObject sink = new BasicDBObject();
		converter.write(objectWithFlatAndComplexPropertiesPlusListAndMap, sink);
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

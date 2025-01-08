/*
 * Copyright 2017-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.convert;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.microbenchmark.AbstractMicrobenchmark;
import org.springframework.util.ObjectUtils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * @author Christoph Strobl
 */
@State(Scope.Benchmark)
@Testable
public class MappingMongoConverterBenchmark extends AbstractMicrobenchmark {

	private static final String DB_NAME = "mapping-mongo-converter-benchmark";

	private MongoClient client;
	private MongoMappingContext mappingContext;
	private MappingMongoConverter converter;
	private Document documentWith2Properties, documentWith2PropertiesAnd1Nested;
	private Customer objectWith2PropertiesAnd1Nested;

	private Document documentWithFlatAndComplexPropertiesPlusListAndMap;
	private SlightlyMoreComplexObject objectWithFlatAndComplexPropertiesPlusListAndMap;

	@Setup
	public void setUp() throws Exception {

		client = MongoClients.create();

		this.mappingContext = new MongoMappingContext();
		this.mappingContext.setInitialEntitySet(Collections.singleton(Customer.class));
		this.mappingContext.afterPropertiesSet();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(new SimpleMongoClientDatabaseFactory(client, DB_NAME));

		this.converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		this.converter.setCustomConversions(new MongoCustomConversions(Collections.emptyList()));
		this.converter.afterPropertiesSet();

		// just a flat document
		this.documentWith2Properties = new Document("firstname", "Dave").append("lastname", "Matthews");

		// document with a nested one
		Document address = new Document("zipCode", "ABCDE").append("city", "Some Place");
		this.documentWith2PropertiesAnd1Nested = new Document("firstname", "Dave").//
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
		objectWithFlatAndComplexPropertiesPlusListAndMap.customerMap = new LinkedHashMap<>();
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
		documentWithFlatAndComplexPropertiesPlusListAndMap = Document.parse(
				"{ \"_id\" : \"517f6aee-e9e0-44f0-88ed-f3694a019f27\", \"intOne\" : -2147483648, \"intTwo\" : 2147483647, \"stringOne\" : \"¯\\\\_(ツ)_/¯\", \"stringTwo\" : \" (╯°□°）╯︵ ┻━┻\", \"explicit-field-name\" : \"supercalifragilisticexpialidocious\", \"location\" : { \"x\" : -33.865143, \"y\" : 151.2099 }, \"objectWith2PropertiesAnd1Nested\" : { \"firstname\" : \"Dave\", \"lastname\" : \"Matthews\", \"address\" : { \"zipCode\" : \"zipCode\", \"city\" : \"City\" } }, \"addressList\" : [{ \"zipCode\" : \"zip-1\", \"city\" : \"city-1\" }, { \"zipCode\" : \"zip-2\", \"city\" : \"city-2\" }], \"customerMap\" : { \"dave\" : { \"firstname\" : \"Dave\", \"lastname\" : \"Matthews\", \"address\" : { \"zipCode\" : \"zipCode\", \"city\" : \"City\" } }, \"deborah\" : { \"firstname\" : \"Deborah Anne\", \"lastname\" : \"Dyer\", \"address\" : { \"zipCode\" : \"?\", \"city\" : \"london\" } }, \"eddie\" : { \"firstname\" : \"Eddie\", \"lastname\" : \"Vedder\", \"address\" : { \"zipCode\" : \"??\", \"city\" : \"Seattle\" } } }, \"_class\" : \"org.springframework.data.mongodb.core.convert.MappingMongoConverterBenchmark$SlightlyMoreComplexObject\" }");

	}

	@TearDown
	public void tearDown() {

		client.getDatabase(DB_NAME).drop();
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
	public Document writeObjectWith2PropertiesAnd1NestedObject() {

		Document sink = new Document();
		converter.write(objectWith2PropertiesAnd1Nested, sink);
		return sink;
	}

	@Benchmark // DATAMONGO-1720
	public Object readObjectWithListAndMapsOfComplexType() {
		return converter.read(SlightlyMoreComplexObject.class, documentWithFlatAndComplexPropertiesPlusListAndMap);
	}

	@Benchmark // DATAMONGO-1720
	public Object writeObjectWithListAndMapsOfComplexType() {

		Document sink = new Document();
		converter.write(objectWithFlatAndComplexPropertiesPlusListAndMap, sink);
		return sink;
	}

	static class Customer {

		private @Id ObjectId id;
		private final String firstname, lastname;
		private final Address address;

		public Customer(String firstname, String lastname, Address address) {
			this.firstname = firstname;
			this.lastname = lastname;
			this.address = address;
		}
	}

	static class Address {
		private String zipCode, city;

		public Address(String zipCode, String city) {
			this.zipCode = zipCode;
			this.city = city;
		}

		public String getZipCode() {
			return zipCode;
		}

		public String getCity() {
			return city;
		}
	}

	static class SlightlyMoreComplexObject {

		@Id String id;
		int intOne, intTwo;
		String stringOne, stringTwo;
		@Field("explicit-field-name") String renamedField;
		Point location;
		Customer customer;
		List<Address> addressList;
		Map<String, Customer> customerMap;

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof SlightlyMoreComplexObject)) {
				return false;
			}
			SlightlyMoreComplexObject that = (SlightlyMoreComplexObject) o;
			if (intOne != that.intOne) {
				return false;
			}
			if (intTwo != that.intTwo) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(id, that.id)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(stringOne, that.stringOne)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(stringTwo, that.stringTwo)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(renamedField, that.renamedField)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(location, that.location)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(customer, that.customer)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(addressList, that.addressList)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(customerMap, that.customerMap);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(id);
			result = 31 * result + intOne;
			result = 31 * result + intTwo;
			result = 31 * result + ObjectUtils.nullSafeHashCode(stringOne);
			result = 31 * result + ObjectUtils.nullSafeHashCode(stringTwo);
			result = 31 * result + ObjectUtils.nullSafeHashCode(renamedField);
			result = 31 * result + ObjectUtils.nullSafeHashCode(location);
			result = 31 * result + ObjectUtils.nullSafeHashCode(customer);
			result = 31 * result + ObjectUtils.nullSafeHashCode(addressList);
			result = 31 * result + ObjectUtils.nullSafeHashCode(customerMap);
			return result;
		}
	}
}

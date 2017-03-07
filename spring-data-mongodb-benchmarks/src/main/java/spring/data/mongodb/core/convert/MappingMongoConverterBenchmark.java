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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import spring.data.microbenchmark.AbstractMicrobenchmark;

import java.util.Collections;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
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

	@Setup
	public void setUp() throws Exception {

		client = new MongoClient(new ServerAddress());

		this.mappingContext = new MongoMappingContext();
		this.mappingContext.setInitialEntitySet(Collections.singleton(Customer.class));
		this.mappingContext.afterPropertiesSet();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(new SimpleMongoDbFactory(client, "benchmark"));

		this.converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		this.plainSource = new Document("firstname", "Dave").append("lastname", "Matthews");

		Document address = new Document("zipCode", "ABCDE").append("city", "Some Place");

		this.sourceWithAddress = new Document("firstname", "Dave").//
				append("lastname", "Matthews").//
				append("address", address);

		this.customer = new Customer("Dave", "Matthews", new Address("zipCode", "City"));
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
}

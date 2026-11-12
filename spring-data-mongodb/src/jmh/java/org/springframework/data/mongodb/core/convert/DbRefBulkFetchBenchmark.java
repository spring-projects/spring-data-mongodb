/*
 * Copyright 2026-present the original author or authors.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.microbenchmark.AbstractMicrobenchmark;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

/**
 * Benchmark for {@link DefaultDbRefResolver#bulkFetch(List)} without database interaction.
 *
 * @author Christoph Strobl
 */
@State(Scope.Benchmark)
@Testable
public class DbRefBulkFetchBenchmark extends AbstractMicrobenchmark {

	@Param({ "1", "10", "100", "1000" }) int size;

	private DefaultDbRefResolver resolver;
	private List<DBRef> references;

	@Setup
	@SuppressWarnings("unchecked")
	public void setUp() {

		references = new ArrayList<>(size);
		List<Document> documents = new ArrayList<>(size);

		for (int i = 0; i < size; i++) {

			ObjectId id = new ObjectId();
			references.add(new DBRef("items", id));
			documents.add(new Document("_id", id).append("name", "name-" + i));
		}

		Collections.shuffle(documents, new Random(42));

		FindIterable<Document> cursor = proxy(FindIterable.class, (proxy, method, args) -> {

			if (method.getName().equals("into")) {

				Collection<Document> target = (Collection<Document>) args[0];
				target.addAll(documents);
				return target;
			}
			throw new UnsupportedOperationException(method.getName());
		});

		MongoCollection<Document> collection = proxy(MongoCollection.class, (proxy, method, args) -> {

			if (method.getName().equals("find")) {
				return cursor;
			}
			throw new UnsupportedOperationException(method.getName());
		});

		MongoDatabaseFactory factory = proxy(MongoDatabaseFactory.class, (proxy, method, args) -> {

			if (method.getName().equals("getExceptionTranslator")) {
				return MongoExceptionTranslator.DEFAULT_EXCEPTION_TRANSLATOR;
			}
			throw new UnsupportedOperationException(method.getName());
		});

		resolver = new DefaultDbRefResolver(factory) {

			@Override
			protected MongoCollection<Document> getCollection(DBRef reference) {
				return collection;
			}
		};
	}

	@Benchmark
	public List<Document> bulkFetch() {
		return resolver.bulkFetch(references);
	}

	@SuppressWarnings("unchecked")
	private static <T> T proxy(Class<?> type, InvocationHandler handler) {
		return (T) Proxy.newProxyInstance(DbRefBulkFetchBenchmark.class.getClassLoader(), new Class<?>[] { type }, handler);
	}
}

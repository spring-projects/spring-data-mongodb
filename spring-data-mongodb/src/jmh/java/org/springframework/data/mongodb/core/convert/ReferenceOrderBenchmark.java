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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import org.springframework.data.mongodb.core.convert.ReferenceLookupDelegate.ListDocumentReferenceQuery;
import org.springframework.data.mongodb.core.convert.ReferenceLookupDelegate.MapDocumentReferenceQuery;
import org.springframework.data.mongodb.microbenchmark.AbstractMicrobenchmark;

/**
 * Benchmark for restoring the order of {@link org.springframework.data.mongodb.core.mapping.DocumentReference} lookup
 * results.
 *
 * @author Christoph Strobl
 */
@Testable
public class ReferenceOrderBenchmark extends AbstractMicrobenchmark {

	@State(Scope.Benchmark)
	public static class Shuffled {

		@Param({ "2", "10", "100", "1000" }) int size;

		@Param({ "ID", "COMPOUND" }) String lookup;

		ListDocumentReferenceQuery query;
		List<Document> results;

		@Setup
		public void setUp() {

			List<Document> documents = documents(size);
			this.query = query(documents, lookup);
			this.results = shuffle(documents);
		}
	}

	@State(Scope.Benchmark)
	public static class TwoDocuments {

		@Param({ "ID", "COMPOUND" }) String lookup;

		@Param({ "true", "false" }) boolean swap;

		ListDocumentReferenceQuery query;
		List<Document> results;

		@Setup
		public void setUp() {

			List<Document> documents = documents(2);
			this.query = query(documents, lookup);
			this.results = swap ? List.of(documents.get(1), documents.get(0)) : documents;
		}
	}

	@State(Scope.Benchmark)
	public static class Sparse {

		@Param({ "1000", "10000" }) int references;

		@Param({ "ID", "COMPOUND" }) String lookup;

		ListDocumentReferenceQuery query;
		List<Document> results;

		@Setup
		public void setUp() {

			List<Document> documents = documents(references);
			this.query = query(documents, lookup);
			this.results = shuffle(
					List.of(documents.get(references / 4), documents.get(references / 2), documents.get(references - 1)));
		}
	}

	@State(Scope.Benchmark)
	public static class Sequential {

		@Param({ "100", "1000" }) int size;

		@Param({ "ID", "COMPOUND" }) String lookup;

		ListDocumentReferenceQuery query;
		List<Document> results;

		@Setup
		public void setUp() {

			List<Document> documents = documents(size);
			this.query = query(documents, lookup);
			this.results = new LinkedList<>(shuffle(documents));
		}
	}

	@State(Scope.Benchmark)
	public static class MapReferences {

		@Param({ "10", "100", "1000" }) int size;

		@Param({ "ID", "COMPOUND" }) String lookup;

		MapDocumentReferenceQuery query;
		List<Document> results;

		@Setup
		public void setUp() {

			List<Document> documents = documents(size);
			Map<Object, Document> filters = new LinkedHashMap<>(size);
			for (int i = 0; i < size; i++) {
				filters.put("key-" + i, reference(documents.get(i), lookup));
			}
			this.query = new MapDocumentReferenceQuery(new Document("$or", filters.values()), new Document(), filters);
			this.results = shuffle(documents);
		}
	}

	@Benchmark
	public Iterable<Document> restoreOrder(Shuffled state) {
		return state.query.restoreOrder(state.results);
	}

	@Benchmark
	public Iterable<Document> restoreOrderOfTwoDocuments(TwoDocuments state) {
		return state.query.restoreOrder(state.results);
	}

	@Benchmark
	public Iterable<Document> restoreOrderOfSparseResults(Sparse state) {
		return state.query.restoreOrder(state.results);
	}

	@Benchmark
	public Iterable<Document> restoreOrderOfSequentialList(Sequential state) {
		return state.query.restoreOrder(state.results);
	}

	@Benchmark
	public Iterable<Document> restoreOrderOfMap(MapReferences state) {
		return state.query.restoreOrder(state.results);
	}

	private static List<Document> documents(int size) {

		List<Document> documents = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			documents.add(new Document("_id", new ObjectId()).append("tenant", "spring").append("code", "code-" + i)
					.append("name", "name-" + i));
		}
		return documents;
	}

	private static ListDocumentReferenceQuery query(List<Document> documents, String lookup) {

		List<Document> references = new ArrayList<>(documents.size());
		for (Document document : documents) {
			references.add(reference(document, lookup));
		}
		return new ListDocumentReferenceQuery(new Document("$or", references), new Document());
	}

	private static Document reference(Document document, String lookup) {
		return "ID".equals(lookup) ? new Document("_id", document.get("_id"))
				: new Document("tenant", "spring").append("code", document.get("code"));
	}

	private static List<Document> shuffle(List<Document> documents) {

		List<Document> shuffled = new ArrayList<>(documents);
		Collections.shuffle(shuffled, new Random(42));
		return shuffled;
	}
}

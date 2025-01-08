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

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.microbenchmark.AbstractMicrobenchmark;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * @author Christoph Strobl
 */
@State(Scope.Benchmark)
public class DbRefMappingBenchmark extends AbstractMicrobenchmark {

	private static final String DB_NAME = "dbref-loading-benchmark";

	private MongoClient client;
	private MongoTemplate template;

	private Query queryObjectWithDBRef;
	private Query queryObjectWithDBRefList;

	@Setup
	public void setUp() throws Exception {

		client = MongoClients.create();
		template = new MongoTemplate(client, DB_NAME);

		List<RefObject> refObjects = new ArrayList<>();
		for (int i = 0; i < 1; i++) {
			RefObject o = new RefObject();
			template.save(o);
			refObjects.add(o);
		}

		ObjectWithDBRef singleDBRef = new ObjectWithDBRef();
		singleDBRef.ref = refObjects.iterator().next();
		template.save(singleDBRef);

		ObjectWithDBRef multipleDBRefs = new ObjectWithDBRef();
		multipleDBRefs.refList = refObjects;
		template.save(multipleDBRefs);

		queryObjectWithDBRef = query(where("id").is(singleDBRef.id));
		queryObjectWithDBRefList = query(where("id").is(multipleDBRefs.id));
	}

	@TearDown
	public void tearDown() {

		client.getDatabase(DB_NAME).drop();
		client.close();
	}

	@Benchmark // DATAMONGO-1720
	public ObjectWithDBRef readSingleDbRef() {
		return template.findOne(queryObjectWithDBRef, ObjectWithDBRef.class);
	}

	@Benchmark // DATAMONGO-1720
	public ObjectWithDBRef readMultipleDbRefs() {
		return template.findOne(queryObjectWithDBRefList, ObjectWithDBRef.class);
	}

	static class ObjectWithDBRef {

		private @Id ObjectId id;
		private @DBRef RefObject ref;
		private @DBRef List<RefObject> refList;

		public ObjectId getId() {
			return id;
		}

		public void setId(ObjectId id) {
			this.id = id;
		}

		public RefObject getRef() {
			return ref;
		}

		public void setRef(RefObject ref) {
			this.ref = ref;
		}

		public List<RefObject> getRefList() {
			return refList;
		}

		public void setRefList(List<RefObject> refList) {
			this.refList = refList;
		}
	}

	static class RefObject {

		private @Id String id;
		private String someValue;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getSomeValue() {
			return someValue;
		}

		public void setSomeValue(String someValue) {
			this.someValue = someValue;
		}
	}
}

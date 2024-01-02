/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.CollectionInfo;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for Views.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
public class MongoTemplateViewTests {

	static @Client MongoClient client;
	static final String DB_NAME = "mongo-template-view-tests";

	private MongoTemplate template;

	Student alex = new Student(22001L, "Alex", 1, 4.0D);
	Student bernie = new Student(21001L, "bernie", 2, 3.7D);
	Student chris = new Student(20010L, "Chris", 3, 2.5D);
	Student drew = new Student(22021L, "Drew", 1, 3.2D);
	Student harley1 = new Student(17301L, "harley", 6, 3.1D);
	Student farmer = new Student(21022L, "Farmer", 1, 2.2D);
	Student george = new Student(20020L, "george", 3, 2.8D);
	Student harley2 = new Student(18020, "Harley", 5, 2.8D);

	List<Student> students = Arrays.asList(alex, bernie, chris, drew, harley1, farmer, george, harley2);

	@BeforeEach
	void beforeEach() {
		template = new MongoTemplate(client, DB_NAME);
	}

	@AfterEach
	void afterEach() {
		client.getDatabase(DB_NAME).drop();
	}

	@Test // GH-2594
	void createsViewFromPipeline() {

		template.insertAll(students);

		template.createView("firstYears", Student.class, match(where("year").is(1)));

		CollectionInfo collectionInfo = MongoTestUtils.readCollectionInfo(client.getDatabase(DB_NAME), "firstYears");
		assertThat(collectionInfo.isView()).isTrue();
		assertThat(collectionInfo.getViewTarget()).isEqualTo("student");
		assertThat(collectionInfo.getViewPipeline()).containsExactly(new Document("$match", new Document("year", 1)));
	}

	@Test // GH-2594
	void mapsPipelineAgainstDomainObject() {

		template.insertAll(students);

		template.createView("fakeStudents", Student.class, match(where("studentID").gte("22")));

		CollectionInfo collectionInfo = MongoTestUtils.readCollectionInfo(client.getDatabase(DB_NAME), "fakeStudents");
		assertThat(collectionInfo.isView()).isTrue();
		assertThat(collectionInfo.getViewPipeline())
				.containsExactly(new Document("$match", new Document("sID", new Document("$gte", "22"))));
	}

	@Test // GH-2594
	void takesPipelineAsIsIfNoTypeDefined() {

		template.insertAll(students);

		template.createView("fakeStudents", "student", AggregationPipeline.of(match(where("studentID").gte("22"))),
				ViewOptions.none());

		CollectionInfo collectionInfo = MongoTestUtils.readCollectionInfo(client.getDatabase(DB_NAME), "fakeStudents");
		assertThat(collectionInfo.isView()).isTrue();
		assertThat(collectionInfo.getViewPipeline())
				.containsExactly(new Document("$match", new Document("studentID", new Document("$gte", "22"))));
	}

	@Test // GH-2594
	void readsFromView() {

		template.insertAll(students);
		client.getDatabase(DB_NAME).createView("firstYears", "student",
				Arrays.asList(new Document("$match", new Document("year", 1))));

		assertThat(template.query(Student.class).inCollection("firstYears").all()).containsExactlyInAnyOrder(alex, drew,
				farmer);
	}

	@Test // GH-2594
	void appliesCollationToView() {

		template.insertAll(students);

		template.createView("firstYears", Student.class, AggregationPipeline.of(match(where("year").is(1))),
				new ViewOptions().collation(Collation.of("en_US")));

		CollectionInfo collectionInfo = MongoTestUtils.readCollectionInfo(client.getDatabase(DB_NAME), "firstYears");
		assertThat(collectionInfo.isView()).isTrue();
		assertThat(collectionInfo.getCollation().getLocale()).isEqualTo("en_US");
	}

	@Data
	@NoArgsConstructor
	private static class Student {

		@Field("sID") Long studentID;

		int year;

		double score;

		String name;

		public Student(long studentID, String name, int year, double score) {
			this.studentID = studentID;
			this.name = name;
			this.year = year;
			this.score = score;
		}
	}
}

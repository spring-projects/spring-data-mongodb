/*
 * Copyright 2011-2024 the original author or authors.
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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;

/**
 * Integration test for {@link MongoTemplate}.
 *
 * @author Oliver Gierke
 * @author Thomas Risberg
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:template-mapping.xml")
public class MongoTemplateMappingTests {

	@Autowired @Qualifier("mongoTemplate1") MongoTemplate template1;

	@Autowired @Qualifier("mongoTemplate2") MongoTemplate template2;

	@Before
	public void setUp() {
		template1.dropCollection(template1.getCollectionName(Person.class));
	}

	@Test
	public void insertsEntityCorrectly1() {

		addAndRetrievePerson(template1);
		checkPersonPersisted(template1);
	}

	@Test
	public void insertsEntityCorrectly2() {

		addAndRetrievePerson(template2);
		checkPersonPersisted(template2);
	}

	@Test // DATAMONGO-2357
	public void writesAndReadsEntityWithNativeMongoGeoJsonTypesCorrectly() {

		WithMongoGeoJson source = new WithMongoGeoJson();
		source.id = "id-2";
		source.multiPolygon = new MultiPolygon(Arrays.asList(new PolygonCoordinates(Arrays.asList(new Position(0, 0),
				new Position(0, 1), new Position(1, 1), new Position(1, 0), new Position(0, 0)))));

		template1.save(source);

		assertThat(template1.findOne(query(where("id").is(source.id)), WithMongoGeoJson.class)).isEqualTo(source);
	}

	@Test // DATAMONGO-2357
	public void writesAndReadsEntityWithOpenNativeMongoGeoJsonTypesCorrectly() {

		WithOpenMongoGeoJson source = new WithOpenMongoGeoJson();
		source.id = "id-2";
		source.geometry = new MultiPolygon(Arrays.asList(new PolygonCoordinates(Arrays.asList(new Position(0, 0),
				new Position(0, 1), new Position(1, 1), new Position(1, 0), new Position(0, 0)))));

		template1.save(source);

		assertThat(template1.findOne(query(where("id").is(source.id)), WithOpenMongoGeoJson.class)).isEqualTo(source);
	}

	@Data
	static class WithMongoGeoJson {

		@Id String id;
		MultiPolygon multiPolygon;
	}

	@Data
	static class WithOpenMongoGeoJson {

		@Id String id;
		Geometry geometry;
	}

	private void addAndRetrievePerson(MongoTemplate template) {
		Person person = new Person("Oliver");
		person.setAge(25);
		template.insert(person);

		Person result = template.findById(person.getId(), Person.class);
		assertThat(result.getFirstName()).isEqualTo("Oliver");
		assertThat(result.getAge()).isEqualTo(25);
	}

	private void checkPersonPersisted(MongoTemplate template) {
		template.execute(Person.class, new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				Document document = collection.find(new Document()).first();
				assertThat((String) document.get("name")).isEqualTo("Oliver");
				return null;
			}
		});
	}
}

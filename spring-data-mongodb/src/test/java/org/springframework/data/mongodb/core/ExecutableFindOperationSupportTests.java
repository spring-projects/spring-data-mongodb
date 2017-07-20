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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ExecutableFindOperation.TerminatingFindOperation;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.NearQuery;

import com.mongodb.MongoClient;

/**
 * Integration tests for {@link ExecutableFindOperationSupport}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ExecutableFindOperationSupportTests {

	private static final String STAR_WARS = "star-wars";
	private static final String STAR_WARS_PLANETS = "star-wars-universe";
	MongoTemplate template;

	Person han;
	Person luke;

	Planet alderan;
	Planet dantooine;

	@Before
	public void setUp() {

		template = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "ExecutableFindOperationSupportTests"));
		template.dropCollection(STAR_WARS);
		template.dropCollection(STAR_WARS_PLANETS);

		template.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		initPersons();
		initPlanets();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1563
	public void domainTypeIsRequired() {
		template.query(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1563
	public void returnTypeIsRequiredOnSet() {
		template.query(Person.class).as(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1563
	public void collectionIsRequiredOnSet() {
		template.query(Person.class).inCollection(null);
	}

	@Test // DATAMONGO-1563
	public void findAll() {
		assertThat(template.query(Person.class).all()).containsExactlyInAnyOrder(han, luke);
	}

	@Test // DATAMONGO-1563
	public void findAllWithCollection() {
		assertThat(template.query(Human.class).inCollection(STAR_WARS).all()).hasSize(2);
	}

	@Test // DATAMONGO-1563
	public void findAllWithProjection() {
		assertThat(template.query(Person.class).as(Jedi.class).all()).hasOnlyElementsOfType(Jedi.class).hasSize(2);
	}

	@Test // DATAMONGO-1733
	public void findByReturningAllValuesAsClosedInterfaceProjection() {

		assertThat(template.query(Person.class).as(PersonProjection.class).all())
				.hasOnlyElementsOfTypes(PersonProjection.class);
	}

	@Test // DATAMONGO-1563
	public void findAllBy() {

		assertThat(template.query(Person.class).matching(query(where("firstname").is("luke"))).all())
				.containsExactlyInAnyOrder(luke);
	}

	@Test // DATAMONGO-1563
	public void findAllByWithCollectionUsingMappingInformation() {

		assertThat(template.query(Jedi.class).inCollection(STAR_WARS).matching(query(where("name").is("luke"))).all())
				.hasSize(1).hasOnlyElementsOfType(Jedi.class);
	}

	@Test // DATAMONGO-1563
	public void findAllByWithCollection() {
		assertThat(template.query(Human.class).inCollection(STAR_WARS).matching(query(where("firstname").is("luke"))).all())
				.hasSize(1);
	}

	@Test // DATAMONGO-1563
	public void findAllByWithProjection() {

		assertThat(template.query(Person.class).as(Jedi.class).matching(query(where("firstname").is("luke"))).all())
				.hasOnlyElementsOfType(Jedi.class).hasSize(1);
	}

	@Test // DATAMONGO-1563
	public void findBy() {
		assertThat(template.query(Person.class).matching(query(where("firstname").is("luke"))).one()).contains(luke);
	}

	@Test // DATAMONGO-1563
	public void findByNoMatch() {
		assertThat(template.query(Person.class).matching(query(where("firstname").is("spock"))).one()).isEmpty();
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class) // DATAMONGO-1563
	public void findByTooManyResults() {
		template.query(Person.class).matching(query(where("firstname").in("han", "luke"))).one();
	}

	@Test // DATAMONGO-1726
	public void findByReturningOneValue() {
		assertThat(template.query(Person.class).matching(query(where("firstname").is("luke"))).oneValue()).isEqualTo(luke);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class) // DATAMONGO-1726
	public void findByReturningOneValueButTooManyResults() {
		template.query(Person.class).matching(query(where("firstname").in("han", "luke"))).oneValue();
	}

	@Test // DATAMONGO-1726
	public void findByReturningFirstValue() {

		assertThat(template.query(Person.class).matching(query(where("firstname").is("luke"))).firstValue())
				.isEqualTo(luke);
	}

	@Test // DATAMONGO-1726
	public void findByReturningFirstValueForManyResults() {

		assertThat(template.query(Person.class).matching(query(where("firstname").in("han", "luke"))).firstValue())
				.isIn(han, luke);
	}

	@Test // DATAMONGO-1733
	public void findByReturningFirstValueAsClosedInterfaceProjection() {

		PersonProjection result = template.query(Person.class).as(PersonProjection.class)
				.matching(query(where("firstname").is("han"))).firstValue();

		assertThat(result).isInstanceOf(PersonProjection.class);
		assertThat(result.getFirstname()).isEqualTo("han");
	}

	@Test // DATAMONGO-1733
	public void findByReturningFirstValueAsOpenInterfaceProjection() {

		PersonSpELProjection result = template.query(Person.class).as(PersonSpELProjection.class)
				.matching(query(where("firstname").is("han"))).firstValue();

		assertThat(result).isInstanceOf(PersonSpELProjection.class);
		assertThat(result.getName()).isEqualTo("han");
	}

	@Test // DATAMONGO-1563
	public void streamAll() {

		try (Stream<Person> stream = template.query(Person.class).stream()) {
			assertThat(stream).containsExactlyInAnyOrder(han, luke);
		}
	}

	@Test // DATAMONGO-1563
	public void streamAllWithCollection() {

		try (Stream<Human> stream = template.query(Human.class).inCollection(STAR_WARS).stream()) {
			assertThat(stream).hasSize(2);
		}
	}

	@Test // DATAMONGO-1563
	public void streamAllWithProjection() {

		try (Stream<Jedi> stream = template.query(Person.class).as(Jedi.class).stream()) {
			assertThat(stream).hasOnlyElementsOfType(Jedi.class).hasSize(2);
		}
	}

	@Test // DATAMONGO-1733
	public void streamAllReturningResultsAsClosedInterfaceProjection() {

		TerminatingFindOperation<PersonProjection> operation = template.query(Person.class).as(PersonProjection.class);

		assertThat(operation.stream()) //
				.hasSize(2) //
				.allSatisfy(it -> {
					assertThat(it).isInstanceOf(PersonProjection.class);
					assertThat(it.getFirstname()).isNotBlank();
				});
	}

	@Test // DATAMONGO-1733
	public void streamAllReturningResultsAsOpenInterfaceProjection() {

		TerminatingFindOperation<PersonSpELProjection> operation = template.query(Person.class)
				.as(PersonSpELProjection.class);

		assertThat(operation.stream()) //
				.hasSize(2) //
				.allSatisfy(it -> {
					assertThat(it).isInstanceOf(PersonSpELProjection.class);
					assertThat(it.getName()).isNotBlank();
				});
	}

	@Test // DATAMONGO-1563
	public void streamAllBy() {

		try (Stream<Person> stream = template.query(Person.class).matching(query(where("firstname").is("luke"))).stream()) {
			assertThat(stream).containsExactlyInAnyOrder(luke);
		}
	}

	@Test // DATAMONGO-1563
	public void findAllNearBy() {

		GeoResults<Planet> results = template.query(Planet.class).near(NearQuery.near(-73.9667, 40.78).spherical(true))
				.all();
		assertThat(results.getContent()).hasSize(2);
		assertThat(results.getContent().get(0).getDistance()).isNotNull();
	}

	@Test // DATAMONGO-1563
	public void findAllNearByWithCollectionAndProjection() {

		GeoResults<Human> results = template.query(Object.class).inCollection(STAR_WARS_PLANETS).as(Human.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all();

		assertThat(results.getContent()).hasSize(2);
		assertThat(results.getContent().get(0).getDistance()).isNotNull();
		assertThat(results.getContent().get(0).getContent()).isInstanceOf(Human.class);
		assertThat(results.getContent().get(0).getContent().getId()).isEqualTo("alderan");
	}

	@Test // DATAMONGO-1733
	public void findAllNearByReturningGeoResultContentAsClosedInterfaceProjection() {

		GeoResults<PlanetProjection> results = template.query(Planet.class).as(PlanetProjection.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all();

		assertThat(results.getContent()).allSatisfy(it -> {

			assertThat(it.getContent()).isInstanceOf(PlanetProjection.class);
			assertThat(it.getContent().getName()).isNotBlank();
		});
	}

	@Test // DATAMONGO-1733
	public void findAllNearByReturningGeoResultContentAsOpenInterfaceProjection() {

		GeoResults<PlanetSpELProjection> results = template.query(Planet.class).as(PlanetSpELProjection.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all();

		assertThat(results.getContent()).allSatisfy(it -> {

			assertThat(it.getContent()).isInstanceOf(PlanetSpELProjection.class);
			assertThat(it.getContent().getId()).isNotBlank();
		});
	}

	@Test // DATAMONGO-1728
	public void firstShouldReturnFirstEntryInCollection() {
		assertThat(template.query(Person.class).first()).isNotEmpty();
	}

	@Test // DATAMONGO-1734
	public void countShouldReturnNrOfElementsInCollectionWhenNoQueryPresent() {
		assertThat(template.query(Person.class).count()).isEqualTo(2);
	}

	@Test // DATAMONGO-1734
	public void countShouldReturnNrOfElementsMatchingQuery() {

		assertThat(template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))).count())
				.isEqualTo(1);
	}

	@Test // DATAMONGO-1734
	public void existsShouldReturnTrueIfAtLeastOneElementExistsInCollection() {
		assertThat(template.query(Person.class).exists()).isTrue();
	}

	@Test // DATAMONGO-1734
	public void existsShouldReturnFalseIfNoElementExistsInCollection() {

		template.remove(new BasicQuery("{}"), STAR_WARS);

		assertThat(template.query(Person.class).exists()).isFalse();
	}

	@Test // DATAMONGO-1734
	public void existsShouldReturnTrueIfAtLeastOneElementMatchesQuery() {

		assertThat(template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))).exists())
				.isTrue();
	}

	@Test // DATAMONGO-1734
	public void existsShouldReturnFalseWhenNoElementMatchesQuery() {
		assertThat(template.query(Person.class).matching(query(where("firstname").is("spock"))).exists()).isFalse();
	}

	@Test // DATAMONGO-1734
	public void returnsTargetObjectDirectlyIfProjectionInterfaceIsImplemented() {
		assertThat(template.query(Person.class).as(Contact.class).all()).allMatch(it -> it instanceof Person);
	}

	interface Contact {}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person implements Contact {
		@Id String id;
		String firstname;
	}

	interface PersonProjection {
		String getFirstname();
	}

	public interface PersonSpELProjection {

		@Value("#{target.firstname}")
		String getName();
	}

	@Data
	static class Human {
		@Id String id;
	}

	@Data
	static class Jedi {

		@Field("firstname") String name;
	}

	@Data
	@AllArgsConstructor
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS_PLANETS)
	static class Planet {

		@Id String name;
		Point coordinates;
	}

	interface PlanetProjection {
		String getName();
	}

	interface PlanetSpELProjection {

		@Value("#{target.name}")
		String getId();
	}

	private void initPersons() {

		han = new Person();
		han.firstname = "han";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.id = "id-2";

		template.save(han);
		template.save(luke);
	}

	private void initPlanets() {

		alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		template.save(alderan);
		template.save(dantooine);
	}
}

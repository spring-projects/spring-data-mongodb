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
import lombok.NoArgsConstructor;
import reactor.test.StepVerifier;

import java.util.Date;
import java.util.function.Consumer;

import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.NearQuery;

import com.mongodb.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Integration tests for {@link ReactiveFindOperationSupport}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class ReactiveFindOperationSupportTests {

	private static final String STAR_WARS = "star-wars";
	MongoTemplate blocking;
	ReactiveMongoTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		blocking = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "ExecutableFindOperationSupportTests"));
		blocking.dropCollection(STAR_WARS);

		han = new Person();
		han.firstname = "han";
		han.lastname = "solo";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.lastname = "skywalker";
		luke.id = "id-2";

		blocking.save(han);
		blocking.save(luke);

		template = new ReactiveMongoTemplate(MongoClients.create(), "ExecutableFindOperationSupportTests");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void domainTypeIsRequired() {
		template.query(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void returnTypeIsRequiredOnSet() {
		template.query(Person.class).as(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void collectionIsRequiredOnSet() {
		template.query(Person.class).inCollection(null);
	}

	@Test // DATAMONGO-1719
	public void findAll() {

		StepVerifier.create(template.query(Person.class).all().collectList()).consumeNextWith(actual -> {
			assertThat(actual).containsExactlyInAnyOrder(han, luke);
		}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllWithCollection() {
		StepVerifier.create(template.query(Human.class).inCollection(STAR_WARS).all()).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllWithProjection() {

		StepVerifier.create(template.query(Person.class).as(Jedi.class).all().map(it -> it.getClass().getName()))
				.expectNext(Jedi.class.getName(), Jedi.class.getName()).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllBy() {

		StepVerifier.create(template.query(Person.class).matching(query(where("firstname").is("luke"))).all())
				.expectNext(luke).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithCollectionUsingMappingInformation() {

		StepVerifier
				.create(template.query(Jedi.class).inCollection(STAR_WARS).matching(query(where("name").is("luke"))).all())
				.consumeNextWith(it -> assertThat(it).isInstanceOf(Jedi.class)).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithCollection() {

		StepVerifier
				.create(
						template.query(Human.class).inCollection(STAR_WARS).matching(query(where("firstname").is("luke"))).all())
				.expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithProjection() {

		StepVerifier
				.create(template.query(Person.class).as(Jedi.class).matching(query(where("firstname").is("luke"))).all())
				.consumeNextWith(it -> assertThat(it).isInstanceOf(Jedi.class)).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithClosedInterfaceProjection() {

		StepVerifier.create(
				template.query(Person.class).as(PersonProjection.class).matching(query(where("firstname").is("luke"))).all())
				.consumeNextWith(it -> {

					assertThat(it).isInstanceOf(PersonProjection.class);
					assertThat(it.getFirstname()).isEqualTo("luke");
				}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithOpenInterfaceProjection() {

		StepVerifier.create(template.query(Person.class).as(PersonSpELProjection.class)
				.matching(query(where("firstname").is("luke"))).all()).consumeNextWith(it -> {

					assertThat(it).isInstanceOf(PersonSpELProjection.class);
					assertThat(it.getName()).isEqualTo("luke");
				}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findBy() {

		StepVerifier.create(template.query(Person.class).matching(query(where("firstname").is("luke"))).one())
				.expectNext(luke).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findByNoMatch() {

		StepVerifier.create(template.query(Person.class).matching(query(where("firstname").is("spock"))).one())
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findByTooManyResults() {

		StepVerifier.create(template.query(Person.class).matching(query(where("firstname").in("han", "luke"))).one())
				.expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATAMONGO-1719
	public void findAllNearBy() {

		blocking.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		Planet alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		Planet dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		blocking.save(alderan);
		blocking.save(dantooine);

		StepVerifier.create(template.query(Planet.class).near(NearQuery.near(-73.9667, 40.78).spherical(true)).all())
				.consumeNextWith(actual -> {
					assertThat(actual.getDistance()).isNotNull();
				}).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllNearByWithCollectionAndProjection() {

		blocking.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		Planet alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		Planet dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		blocking.save(alderan);
		blocking.save(dantooine);

		StepVerifier.create(template.query(Object.class).inCollection(STAR_WARS).as(Human.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all()).consumeNextWith(actual -> {
					assertThat(actual.getDistance()).isNotNull();
					assertThat(actual.getContent()).isInstanceOf(Human.class);
					assertThat(actual.getContent().getId()).isEqualTo("alderan");
				}).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllNearByReturningGeoResultContentAsClosedInterfaceProjection() {

		blocking.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		Planet alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		Planet dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		blocking.save(alderan);
		blocking.save(dantooine);

		StepVerifier.create(template.query(Planet.class).as(PlanetProjection.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all()).consumeNextWith(it -> {

					assertThat(it.getDistance()).isNotNull();
					assertThat(it.getContent()).isInstanceOf(PlanetProjection.class);
					assertThat(it.getContent().getName()).isEqualTo("alderan");
				}).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllNearByReturningGeoResultContentAsOpenInterfaceProjection() {

		blocking.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		Planet alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		Planet dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		blocking.save(alderan);
		blocking.save(dantooine);

		StepVerifier.create(template.query(Planet.class).as(PlanetSpELProjection.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all()).consumeNextWith(it -> {

					assertThat(it.getDistance()).isNotNull();
					assertThat(it.getContent()).isInstanceOf(PlanetSpELProjection.class);
					assertThat(it.getContent().getId()).isEqualTo("alderan");
				}).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void firstShouldReturnFirstEntryInCollection() {
		StepVerifier.create(template.query(Person.class).first()).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void countShouldReturnNrOfElementsInCollectionWhenNoQueryPresent() {
		StepVerifier.create(template.query(Person.class).count()).expectNext(2L).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void countShouldReturnNrOfElementsMatchingQuery() {

		StepVerifier
				.create(template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))).count())
				.expectNext(1L).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void existsShouldReturnTrueIfAtLeastOneElementExistsInCollection() {
		StepVerifier.create(template.query(Person.class).exists()).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void existsShouldReturnFalseIfNoElementExistsInCollection() {

		blocking.remove(new BasicQuery("{}"), STAR_WARS);

		StepVerifier.create(template.query(Person.class).exists()).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void existsShouldReturnTrueIfAtLeastOneElementMatchesQuery() {

		StepVerifier
				.create(template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))).exists())
				.expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void existsShouldReturnFalseWhenNoElementMatchesQuery() {

		StepVerifier.create(template.query(Person.class).matching(query(where("firstname").is("spock"))).exists())
				.expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsEmptyListIfNoMatchFound() {

		StepVerifier.create(template.query(Person.class).distinct("actually-not-property-in-use").as(String.class).all())
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsSimpleFieldValuesCorrectlyForCollectionHavingReturnTypeSpecifiedThatCanBeConvertedDirectlyByACodec() {

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.lastname = luke.lastname;

		blocking.save(anakin);

		StepVerifier.create(template.query(Person.class).distinct("lastname").as(String.class).all())
				.assertNext(in("solo", "skywalker")).assertNext(in("solo", "skywalker")).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsSimpleFieldValuesCorrectlyForCollectionHavingNoReturnTypeSpecified() {

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = "dark-lord";

		Person padme = new Person();
		padme.firstname = "padme";
		padme.ability = 42L;

		Person jaja = new Person();
		jaja.firstname = "jaja";
		jaja.ability = new Date();

		blocking.save(anakin);
		blocking.save(padme);
		blocking.save(jaja);

		Consumer<Object> containedInAbilities = in(anakin.ability, padme.ability, jaja.ability);

		StepVerifier.create(template.query(Person.class).distinct("ability").all()).assertNext(containedInAbilities)
				.assertNext(containedInAbilities).assertNext(containedInAbilities).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsComplexValuesCorrectlyForCollectionHavingNoReturnTypeSpecified() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		blocking.save(anakin);

		StepVerifier.create(template.query(Person.class).distinct("ability").all()).expectNext(anakin.ability)
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsComplexValuesCorrectlyForCollectionHavingReturnTypeSpecified() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		blocking.save(anakin);

		StepVerifier.create(template.query(Person.class).distinct("ability").as(Sith.class).all()).expectNext(sith)
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsComplexValuesCorrectlyForCollectionHavingReturnTypeDocumentSpecified() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		blocking.save(anakin);

		StepVerifier.create(template.query(Person.class).distinct("ability").as(Document.class).all())
				.expectNext(new Document("rank", "lord").append("_class", Sith.class.getName())).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctMapsFieldNameCorrectly() {

		StepVerifier.create(template.query(Jedi.class).inCollection(STAR_WARS).distinct("name").as(String.class).all())
				.assertNext(in("han", "luke")).assertNext(in("han", "luke")).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsRawValuesIfReturnTypeIsBsonValue() {

		Consumer<BsonValue> inValues = in(new BsonString("solo"), new BsonString("skywalker"));
		StepVerifier.create(template.query(Person.class).distinct("lastname").as(BsonValue.class).all())
				.assertNext(inValues).assertNext(inValues).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsValuesMappedToTheirJavaTypeEvenWhenNotExplicitlyDefinedByTheDomainType() {

		blocking.save(new Document("darth", "vader"), STAR_WARS);

		StepVerifier.create(template.query(Person.class).distinct("darth").all()).expectNext("vader").verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsMappedDomainTypeForProjections() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		blocking.save(luke);

		StepVerifier.create(template.query(Person.class).distinct("father").as(Jedi.class).all())
				.expectNext(new Jedi("anakin")).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctAlllowsQueryUsingObjectSourceType() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		blocking.save(luke);

		StepVerifier.create(template.query(Object.class).inCollection(STAR_WARS).distinct("father").as(Jedi.class).all())
				.expectNext(new Jedi("anakin")).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsMappedDomainTypeExtractedFromPropertyWhenNoExplicitTypePresent() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		blocking.save(luke);

		Person expected = new Person();
		expected.firstname = luke.father.firstname;

		StepVerifier.create(template.query(Person.class).distinct("father").all()).expectNext(expected).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctThrowsExceptionWhenExplicitMappingTypeCannotBeApplied() {
		StepVerifier.create(template.query(Person.class).distinct("firstname").as(Long.class).all())
				.expectError(InvalidDataAccessApiUsageException.class).verify();
	}

	interface Contact {}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person implements Contact {

		@Id String id;
		String firstname;
		String lastname;
		Object ability;
		Person father;
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
	@NoArgsConstructor
	@AllArgsConstructor
	static class Jedi {

		@Field("firstname") String name;
	}

	@Data
	static class Sith {

		String rank;
	}

	@Data
	@AllArgsConstructor
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
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

	static <T> Consumer<T> in(T... values) {
		return (val) -> {
			assertThat(values).contains(val);
		};
	}
}

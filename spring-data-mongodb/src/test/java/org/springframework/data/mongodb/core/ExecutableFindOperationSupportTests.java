/*
 * Copyright 2017-2023 the original author or authors.
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
import static org.springframework.data.mongodb.test.util.DirtiesStateExtension.*;

import java.util.Date;
import java.util.Objects;
import java.util.stream.Stream;

import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ExecutableFindOperation.TerminatingFind;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.test.util.DirtiesStateExtension;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * Integration tests for {@link ExecutableFindOperationSupport}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith({ MongoTemplateExtension.class, DirtiesStateExtension.class })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExecutableFindOperationSupportTests implements StateFunctions {

	private static final String STAR_WARS = "star-wars";
	private static final String STAR_WARS_PLANETS = "star-wars-universe";

	@Template(database = "executable-find-operation-support-tests", initialEntitySet = { Person.class, Planet.class }) //
	private static MongoTestTemplate template;

	private Person han;
	private Person luke;

	private Planet alderan;
	private Planet dantooine;

	@Override
	public void clear() {
		template.flush();
	}

	@Override
	public void setupState() {
		template.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		initPersons();
		initPlanets();
	}

	@Test // DATAMONGO-1563
	void domainTypeIsRequired() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.query(null));
	}

	@Test // DATAMONGO-1563
	void returnTypeIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.query(Person.class).as(null));
	}

	@Test // DATAMONGO-1563
	void collectionIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.query(Person.class).inCollection(null));
	}

	@Test // DATAMONGO-1563
	void findAll() {
		assertThat(template.query(Person.class).all()).containsExactlyInAnyOrder(han, luke);
	}

	@Test // DATAMONGO-1563
	void findAllWithCollection() {
		assertThat(template.query(Human.class).inCollection(STAR_WARS).all()).hasSize(2);
	}

	@Test // DATAMONGO-1563
	void findAllWithProjection() {
		assertThat(template.query(Person.class).as(Jedi.class).all()).hasOnlyElementsOfType(Jedi.class).hasSize(2);
	}

	@Test // DATAMONGO-2041
	@DirtiesState
	void findAllWithProjectionOnEmbeddedType() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		template.save(luke);

		assertThat(template.query(Person.class).as(PersonDtoProjection.class).matching(query(where("id").is(luke.id)))
				.firstValue()).hasFieldOrPropertyWithValue("father", luke.father);
	}

	@Test // DATAMONGO-1733
	void findByReturningAllValuesAsClosedInterfaceProjection() {

		assertThat(template.query(Person.class).as(PersonProjection.class).all())
				.hasOnlyElementsOfTypes(PersonProjection.class);
	}

	@Test // DATAMONGO-1563
	void findAllBy() {

		assertThat(template.query(Person.class).matching(query(where("firstname").is("luke"))).all())
				.containsExactlyInAnyOrder(luke);
	}

	@Test // DATAMONGO-1563
	void findAllByWithCollectionUsingMappingInformation() {

		assertThat(template.query(Jedi.class).inCollection(STAR_WARS).matching(query(where("name").is("luke"))).all())
				.hasSize(1).hasOnlyElementsOfType(Jedi.class);
	}

	@Test // DATAMONGO-1563
	void findAllByWithCollection() {
		assertThat(template.query(Human.class).inCollection(STAR_WARS).matching(query(where("firstname").is("luke"))).all())
				.hasSize(1);
	}

	@Test // DATAMONGO-2323
	void findAllAsDocument() {
		assertThat(
				template.query(Document.class).inCollection(STAR_WARS).matching(query(where("firstname").is("luke"))).all())
						.hasSize(1);
	}

	@Test // DATAMONGO-1563
	void findAllByWithProjection() {

		assertThat(template.query(Person.class).as(Jedi.class).matching(query(where("firstname").is("luke"))).all())
				.hasOnlyElementsOfType(Jedi.class).hasSize(1);
	}

	@Test // DATAMONGO-1563
	void findBy() {
		assertThat(template.query(Person.class).matching(query(where("firstname").is("luke"))).one()).contains(luke);
	}

	@Test // DATAMONGO-2416
	void findByCriteria() {
		assertThat(template.query(Person.class).matching(where("firstname").is("luke")).one()).contains(luke);
	}

	@Test // DATAMONGO-1563
	void findByNoMatch() {
		assertThat(template.query(Person.class).matching(query(where("firstname").is("spock"))).one()).isEmpty();
	}

	@Test // DATAMONGO-1563
	void findByTooManyResults() {
		assertThatExceptionOfType(IncorrectResultSizeDataAccessException.class)
				.isThrownBy(() -> template.query(Person.class).matching(query(where("firstname").in("han", "luke"))).one());
	}

	@Test // DATAMONGO-1726
	void findByReturningOneValue() {
		assertThat(template.query(Person.class).matching(query(where("firstname").is("luke"))).oneValue()).isEqualTo(luke);
	}

	@Test // DATAMONGO-1726
	void findByReturningOneValueButTooManyResults() {
		assertThatExceptionOfType(IncorrectResultSizeDataAccessException.class).isThrownBy(
				() -> template.query(Person.class).matching(query(where("firstname").in("han", "luke"))).oneValue());
	}

	@Test // DATAMONGO-1726
	void findByReturningFirstValue() {

		assertThat(template.query(Person.class).matching(query(where("firstname").is("luke"))).firstValue())
				.isEqualTo(luke);
	}

	@Test // DATAMONGO-1726
	void findByReturningFirstValueForManyResults() {

		assertThat(template.query(Person.class).matching(query(where("firstname").in("han", "luke"))).firstValue())
				.isIn(han, luke);
	}

	@Test // DATAMONGO-1733
	void findByReturningFirstValueAsClosedInterfaceProjection() {

		PersonProjection result = template.query(Person.class).as(PersonProjection.class)
				.matching(query(where("firstname").is("han"))).firstValue();

		assertThat(result).isInstanceOf(PersonProjection.class);
		assertThat(result.getFirstname()).isEqualTo("han");
	}

	@Test // DATAMONGO-1733
	void findByReturningFirstValueAsOpenInterfaceProjection() {

		PersonSpELProjection result = template.query(Person.class).as(PersonSpELProjection.class)
				.matching(query(where("firstname").is("han"))).firstValue();

		assertThat(result).isInstanceOf(PersonSpELProjection.class);
		assertThat(result.getName()).isEqualTo("han");
	}

	@Test // DATAMONGO-1563
	void streamAll() {

		try (Stream<Person> stream = template.query(Person.class).stream()) {
			assertThat(stream).containsExactlyInAnyOrder(han, luke);
		}
	}

	@Test // DATAMONGO-1563
	void streamAllWithCollection() {

		try (Stream<Human> stream = template.query(Human.class).inCollection(STAR_WARS).stream()) {
			assertThat(stream).hasSize(2);
		}
	}

	@Test // DATAMONGO-1563
	void streamAllWithProjection() {

		try (Stream<Jedi> stream = template.query(Person.class).as(Jedi.class).stream()) {
			assertThat(stream).hasOnlyElementsOfType(Jedi.class).hasSize(2);
		}
	}

	@Test // DATAMONGO-1733
	void streamAllReturningResultsAsClosedInterfaceProjection() {

		TerminatingFind<PersonProjection> operation = template.query(Person.class).as(PersonProjection.class);

		assertThat(operation.stream()) //
				.hasSize(2) //
				.allSatisfy(it -> {
					assertThat(it).isInstanceOf(PersonProjection.class);
					assertThat(it.getFirstname()).isNotBlank();
				});
	}

	@Test // DATAMONGO-1733
	void streamAllReturningResultsAsOpenInterfaceProjection() {

		TerminatingFind<PersonSpELProjection> operation = template.query(Person.class).as(PersonSpELProjection.class);

		assertThat(operation.stream()) //
				.hasSize(2) //
				.allSatisfy(it -> {
					assertThat(it).isInstanceOf(PersonSpELProjection.class);
					assertThat(it.getName()).isNotBlank();
				});
	}

	@Test // DATAMONGO-1563
	void streamAllBy() {

		try (Stream<Person> stream = template.query(Person.class).matching(query(where("firstname").is("luke"))).stream()) {
			assertThat(stream).containsExactlyInAnyOrder(luke);
		}
	}

	@Test // DATAMONGO-1563
	void findAllNearBy() {

		GeoResults<Planet> results = template.query(Planet.class).near(NearQuery.near(-73.9667, 40.78).spherical(true))
				.all();
		assertThat(results.getContent()).hasSize(2);
		assertThat(results.getContent().get(0).getDistance()).isNotNull();
	}

	@Test // DATAMONGO-1563
	void findAllNearByWithCollectionAndProjection() {

		GeoResults<Human> results = template.query(Object.class).inCollection(STAR_WARS_PLANETS).as(Human.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all();

		assertThat(results.getContent()).hasSize(2);
		assertThat(results.getContent().get(0).getDistance()).isNotNull();
		assertThat(results.getContent().get(0).getContent()).isInstanceOf(Human.class);
		assertThat(results.getContent().get(0).getContent().getId()).isEqualTo("alderan");
	}

	@Test // DATAMONGO-1733
	void findAllNearByReturningGeoResultContentAsClosedInterfaceProjection() {

		GeoResults<PlanetProjection> results = template.query(Planet.class).as(PlanetProjection.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all();

		assertThat(results.getContent()).allSatisfy(it -> {

			assertThat(it.getContent()).isInstanceOf(PlanetProjection.class);
			assertThat(it.getContent().getName()).isNotBlank();
		});
	}

	@Test // DATAMONGO-1733
	void findAllNearByReturningGeoResultContentAsOpenInterfaceProjection() {

		GeoResults<PlanetSpELProjection> results = template.query(Planet.class).as(PlanetSpELProjection.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all();

		assertThat(results.getContent()).allSatisfy(it -> {

			assertThat(it.getContent()).isInstanceOf(PlanetSpELProjection.class);
			assertThat(it.getContent().getId()).isNotBlank();
		});
	}

	@Test // DATAMONGO-1728
	void firstShouldReturnFirstEntryInCollection() {
		assertThat(template.query(Person.class).first()).isNotEmpty();
	}

	@Test // DATAMONGO-1734
	void countShouldReturnNrOfElementsInCollectionWhenNoQueryPresent() {
		assertThat(template.query(Person.class).count()).isEqualTo(2);
	}

	@Test // DATAMONGO-1734
	void countShouldReturnNrOfElementsMatchingQuery() {

		assertThat(template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))).count())
				.isEqualTo(1);
	}

	@Test // DATAMONGO-1734
	void existsShouldReturnTrueIfAtLeastOneElementExistsInCollection() {
		assertThat(template.query(Person.class).exists()).isTrue();
	}

	@Test // DATAMONGO-1734
	@DirtiesState
	void existsShouldReturnFalseIfNoElementExistsInCollection() {

		template.remove(new BasicQuery("{}"), STAR_WARS);

		assertThat(template.query(Person.class).exists()).isFalse();
	}

	@Test // DATAMONGO-1734
	void existsShouldReturnTrueIfAtLeastOneElementMatchesQuery() {

		assertThat(template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))).exists())
				.isTrue();
	}

	@Test // DATAMONGO-1734
	void existsShouldReturnFalseWhenNoElementMatchesQuery() {
		assertThat(template.query(Person.class).matching(query(where("firstname").is("spock"))).exists()).isFalse();
	}

	@Test // DATAMONGO-1734
	void returnsTargetObjectDirectlyIfProjectionInterfaceIsImplemented() {
		assertThat(template.query(Person.class).as(Contact.class).all()).allMatch(it -> it instanceof Person);
	}

	@Test // DATAMONGO-1761
	void distinctReturnsEmptyListIfNoMatchFound() {
		assertThat(template.query(Person.class).distinct("actually-not-property-in-use").as(String.class).all()).isEmpty();
	}

	@Test // DATAMONGO-1761
	void distinctReturnsSimpleFieldValuesCorrectlyForCollectionHavingReturnTypeSpecifiedThatCanBeConvertedDirectlyByACodec() {

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.lastname = luke.lastname;

		template.save(anakin);

		assertThat(template.query(Person.class).distinct("lastname").as(String.class).all())
				.containsExactlyInAnyOrder("solo", "skywalker");
	}

	@Test // DATAMONGO-1761
	void distinctReturnsSimpleFieldValuesCorrectly() {

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = "dark-lord";

		Person padme = new Person();
		padme.firstname = "padme";
		padme.ability = 42L;

		Person jaja = new Person();
		jaja.firstname = "jaja";
		jaja.ability = new Date();

		template.save(anakin);
		template.save(padme);
		template.save(jaja);

		assertThat(template.query(Person.class).distinct("ability").all()).containsExactlyInAnyOrder(anakin.ability,
				padme.ability, jaja.ability);
	}

	@Test // DATAMONGO-1761
	void distinctReturnsComplexValuesCorrectly() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		template.save(anakin);

		assertThat(template.query(Person.class).distinct("ability").all()).containsExactlyInAnyOrder(anakin.ability);
	}

	@Test // DATAMONGO-1761
	void distinctReturnsComplexValuesCorrectlyHavingReturnTypeSpecified() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		template.save(anakin);

		assertThat(template.query(Person.class).distinct("ability").as(Sith.class).all())
				.containsExactlyInAnyOrder((Sith) anakin.ability);
	}

	@Test // DATAMONGO-1761
	void distinctReturnsComplexValuesCorrectlyHavingReturnTypeDocumentSpecified() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		template.save(anakin);

		assertThat(template.query(Person.class).distinct("ability").as(Document.class).all())
				.containsExactlyInAnyOrder(new Document("rank", "lord").append("_class", Sith.class.getName()));
	}

	@Test // DATAMONGO-1761
	void distinctMapsFieldNameCorrectly() {

		assertThat(template.query(Jedi.class).inCollection(STAR_WARS).distinct("name").as(String.class).all())
				.containsExactlyInAnyOrder("han", "luke");
	}

	@Test // DATAMONGO-1761
	void distinctReturnsRawValuesIfReturnTypeIsBsonValue() {

		assertThat(template.query(Person.class).distinct("lastname").as(BsonValue.class).all())
				.containsExactlyInAnyOrder(new BsonString("solo"), new BsonString("skywalker"));
	}

	@Test // DATAMONGO-1761
	@DirtiesState
	void distinctReturnsValuesMappedToTheirJavaTypeEvenWhenNotExplicitlyDefinedByTheDomainType() {

		template.save(new Document("darth", "vader"), STAR_WARS);

		assertThat(template.query(Person.class).distinct("darth").all()).containsExactlyInAnyOrder("vader");
	}

	@Test // DATAMONGO-1761
	@DirtiesState
	void distinctReturnsMappedDomainTypeForProjections() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		template.save(luke);

		assertThat(template.query(Person.class).distinct("father").as(Jedi.class).all())
				.containsExactlyInAnyOrder(new Jedi("anakin"));
	}

	@Test // DATAMONGO-1761
	@DirtiesState
	void distinctAlllowsQueryUsingObjectSourceType() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		template.save(luke);

		assertThat(template.query(Object.class).inCollection(STAR_WARS).distinct("father").as(Jedi.class).all())
				.containsExactlyInAnyOrder(new Jedi("anakin"));
	}

	@Test // DATAMONGO-1761
	@DirtiesState
	void distinctReturnsMappedDomainTypeExtractedFromPropertyWhenNoExplicitTypePresent() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		template.save(luke);

		Person expected = new Person();
		expected.firstname = luke.father.firstname;

		assertThat(template.query(Person.class).distinct("father").all()).containsExactlyInAnyOrder(expected);
	}

	@Test // DATAMONGO-1761
	void distinctThrowsExceptionWhenExplicitMappingTypeCannotBeApplied() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> template.query(Person.class).distinct("firstname").as(Long.class).all());
	}

	@Test // DATAMONGO-2507
	void distinctAppliesFilterQuery() {

		assertThat(template.query(Person.class).inCollection(STAR_WARS).distinct("firstname") //
				.matching(where("lastname").is(luke.lastname)) //
				.as(String.class) //
				.all() //
		).containsExactlyInAnyOrder("luke");
	}

	@Test // GH-2860
	void projectionOnDbRef() {

		WithRefs source = new WithRefs();
		source.id = "id-1";
		source.noRef = "value";
		source.planetDbRef = alderan;

		template.save(source);

		WithDbRefProjection target = template.query(WithRefs.class).as(WithDbRefProjection.class)
				.matching(where("id").is(source.id)).oneValue();

		assertThat(target.getPlanetDbRef()).isEqualTo(alderan);
	}

	@Test // GH-2860
	@Disabled("GH-3913")
	@DirtiesState
	void propertyProjectionOnDbRef() {

		WithRefs source = new WithRefs();
		source.id = "id-1";
		source.noRef = "value";
		source.planetDbRef = alderan;

		template.save(source);

		WithDbRefPropertyProjection target = template.query(WithRefs.class).as(WithDbRefPropertyProjection.class)
				.matching(where("id").is(source.id)).oneValue();

		assertThat(target.getPlanetDbRef().getName()).isEqualTo(alderan.getName());
	}

	@Test // GH-2860
	@DirtiesState
	void projectionOnDocRef() {

		WithRefs source = new WithRefs();
		source.id = "id-1";
		source.noRef = "value";
		source.planetDocRef = alderan;

		template.save(source);

		WithDocumentRefProjection target = template.query(WithRefs.class).as(WithDocumentRefProjection.class)
				.matching(where("id").is(source.id)).oneValue();

		assertThat(target.getPlanetDocRef()).isEqualTo(alderan);
	}

	@Test // GH-2860
	@DirtiesState
	void propertyProjectionOnDocRef() {

		WithRefs source = new WithRefs();
		source.id = "id-1";
		source.noRef = "value";
		source.planetDocRef = alderan;

		template.save(source);

		WithDocRefPropertyProjection target = template.query(WithRefs.class).as(WithDocRefPropertyProjection.class)
				.matching(where("id").is(source.id)).oneValue();

		assertThat(target.getPlanetDocRef().getName()).isEqualTo(alderan.getName());
	}

	interface Contact {}

	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person implements Contact {

		@Id String id;
		String firstname;
		String lastname;
		Object ability;
		Person father;

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public String getLastname() {
			return this.lastname;
		}

		public Object getAbility() {
			return this.ability;
		}

		public Person getFather() {
			return this.father;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public void setAbility(Object ability) {
			this.ability = ability;
		}

		public void setFather(Person father) {
			this.father = father;
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals(id, person.id) && Objects.equals(firstname, person.firstname)
					&& Objects.equals(lastname, person.lastname) && Objects.equals(ability, person.ability)
					&& Objects.equals(father, person.father);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, firstname, lastname, ability, father);
		}

		public String toString() {
			return "ExecutableFindOperationSupportTests.Person(id=" + this.getId() + ", firstname=" + this.getFirstname()
					+ ", lastname=" + this.getLastname() + ", ability=" + this.getAbility() + ", father=" + this.getFather()
					+ ")";
		}
	}

	interface PersonProjection {
		String getFirstname();
	}

	public interface PersonSpELProjection {

		@Value("#{target.firstname}")
		String getName();
	}

	// TODO: Without getters/setters, not identified as projection/properties
	static class PersonDtoProjection {

		@Field("firstname") String name;
		Person father;

		public String getName() {
			return this.name;
		}

		public Person getFather() {
			return this.father;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setFather(Person father) {
			this.father = father;
		}
	}

	static class Human {

		@Id String id;

		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String toString() {
			return "ExecutableFindOperationSupportTests.Human(id=" + this.getId() + ")";
		}
	}

	static class Jedi {

		@Field("firstname") String name;

		public Jedi(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Jedi jedi = (Jedi) o;
			return Objects.equals(name, jedi.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name);
		}

		public String toString() {
			return "ExecutableFindOperationSupportTests.Jedi(name=" + this.getName() + ")";
		}
	}

	static class Sith {

		String rank;

		public String getRank() {
			return this.rank;
		}

		public void setRank(String rank) {
			this.rank = rank;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Sith sith = (Sith) o;
			return Objects.equals(rank, sith.rank);
		}

		@Override
		public int hashCode() {
			return Objects.hash(rank);
		}

		public String toString() {
			return "ExecutableFindOperationSupportTests.Sith(rank=" + this.getRank() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS_PLANETS)
	static class Planet {

		@Id String name;
		Point coordinates;

		public Planet(String name, Point coordinates) {
			this.name = name;
			this.coordinates = coordinates;
		}

		public String getName() {
			return this.name;
		}

		public Point getCoordinates() {
			return this.coordinates;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setCoordinates(Point coordinates) {
			this.coordinates = coordinates;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Planet planet = (Planet) o;
			return Objects.equals(name, planet.name) && Objects.equals(coordinates, planet.coordinates);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, coordinates);
		}

		public String toString() {
			return "ExecutableFindOperationSupportTests.Planet(name=" + this.getName() + ", coordinates="
					+ this.getCoordinates() + ")";
		}
	}

	interface PlanetProjection {
		String getName();
	}

	interface PlanetSpELProjection {

		@Value("#{target.name}")
		String getId();
	}

	static class WithRefs {

		@Id String id;

		String noRef;

		@DBRef Planet planetDbRef;

		@DocumentReference Planet planetDocRef;

		public String getId() {
			return this.id;
		}

		public String getNoRef() {
			return this.noRef;
		}

		public Planet getPlanetDbRef() {
			return this.planetDbRef;
		}

		public Planet getPlanetDocRef() {
			return this.planetDocRef;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setNoRef(String noRef) {
			this.noRef = noRef;
		}

		public void setPlanetDbRef(Planet planetDbRef) {
			this.planetDbRef = planetDbRef;
		}

		public void setPlanetDocRef(Planet planetDocRef) {
			this.planetDocRef = planetDocRef;
		}

		public String toString() {
			return "ExecutableFindOperationSupportTests.WithRefs(id=" + this.getId() + ", noRef=" + this.getNoRef()
					+ ", planetDbRef=" + this.getPlanetDbRef() + ", planetDocRef=" + this.getPlanetDocRef() + ")";
		}
	}

	interface WithDbRefProjection {
		Planet getPlanetDbRef();
	}

	interface WithDocumentRefProjection {
		Planet getPlanetDocRef();
	}

	interface WithDbRefPropertyProjection {
		PlanetProjection getPlanetDbRef();
	}

	interface WithDocRefPropertyProjection {
		PlanetProjection getPlanetDocRef();
	}

	private void initPersons() {

		han = new Person();
		han.firstname = "han";
		han.lastname = "solo";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.lastname = "skywalker";
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

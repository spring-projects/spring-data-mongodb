/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static java.util.Arrays.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.geo.Metrics.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.SampleEvaluationContextExtension.SampleSecurityContextHolder;
import org.springframework.data.querydsl.QSort;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Base class for tests for {@link PersonRepository}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Fırat KÜÇÜK
 * @author Edward Prentice
 */
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractPersonRepositoryIntegrationTests {

	public @Rule ExpectedException expectedException = ExpectedException.none();

	@Autowired protected PersonRepository repository;

	@Autowired MongoOperations operations;

	Person dave, oliver, carter, boyd, stefan, leroi, alicia;
	QPerson person;

	List<Person> all;

	@Before
	public void setUp() throws InterruptedException {

		repository.deleteAll();

		dave = new Person("Dave", "Matthews", 42);
		oliver = new Person("Oliver August", "Matthews", 4);
		carter = new Person("Carter", "Beauford", 49);
		carter.setSkills(Arrays.asList("Drums", "percussion", "vocals"));
		Thread.sleep(10);
		boyd = new Person("Boyd", "Tinsley", 45);
		boyd.setSkills(Arrays.asList("Violin", "Electric Violin", "Viola", "Mandolin", "Vocals", "Guitar"));
		stefan = new Person("Stefan", "Lessard", 34);
		leroi = new Person("Leroi", "Moore", 41);

		alicia = new Person("Alicia", "Keys", 30, Sex.FEMALE);

		person = new QPerson("person");

		all = repository.saveAll(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia));
	}

	@Test
	public void findsPersonById() throws Exception {

		assertThat(repository.findById(dave.getId().toString()), is(Optional.of(dave)));
	}

	@Test
	public void findsAllMusicians() throws Exception {
		List<Person> result = repository.findAll();
		assertThat(result.size(), is(all.size()));
		assertThat(result.containsAll(all), is(true));
	}

	@Test
	public void findsAllWithGivenIds() {

		Iterable<Person> result = repository.findAllById(Arrays.asList(dave.id, boyd.id));
		assertThat(result, hasItems(dave, boyd));
		assertThat(result, not(hasItems(oliver, carter, stefan, leroi, alicia)));
	}

	@Test
	public void deletesPersonCorrectly() throws Exception {

		repository.delete(dave);

		List<Person> result = repository.findAll();

		assertThat(result.size(), is(all.size() - 1));
		assertThat(result, not(hasItem(dave)));
	}

	@Test
	public void deletesPersonByIdCorrectly() {

		repository.deleteById(dave.getId().toString());

		List<Person> result = repository.findAll();

		assertThat(result.size(), is(all.size() - 1));
		assertThat(result, not(hasItem(dave)));
	}

	@Test
	public void findsPersonsByLastname() throws Exception {

		List<Person> result = repository.findByLastname("Beauford");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(carter));
	}

	@Test
	public void findsPersonsByFirstname() {

		List<Person> result = repository.findByThePersonsFirstname("Leroi");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(leroi));
		assertThat(result.get(0).getAge(), is(nullValue()));
	}

	@Test
	public void findsPersonsByFirstnameLike() throws Exception {

		List<Person> result = repository.findByFirstnameLike("Bo*");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(boyd));
	}

	@Test // DATAMONGO-1608
	public void findByFirstnameLikeWithNull() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("property 'firstname'");

		repository.findByFirstnameLike(null);
	}

	@Test
	public void findsPagedPersons() throws Exception {

		Page<Person> result = repository.findAll(PageRequest.of(1, 2, Direction.ASC, "lastname", "firstname"));
		assertThat(result.isFirst(), is(false));
		assertThat(result.isLast(), is(false));
		assertThat(result, hasItems(dave, stefan));
	}

	@Test
	public void executesPagedFinderCorrectly() throws Exception {

		Page<Person> page = repository.findByLastnameLike("*a*",
				PageRequest.of(0, 2, Direction.ASC, "lastname", "firstname"));
		assertThat(page.isFirst(), is(true));
		assertThat(page.isLast(), is(false));
		assertThat(page.getNumberOfElements(), is(2));
		assertThat(page, hasItems(carter, stefan));
	}

	@Test
	public void executesPagedFinderWithAnnotatedQueryCorrectly() throws Exception {

		Page<Person> page = repository.findByLastnameLikeWithPageable(".*a.*",
				PageRequest.of(0, 2, Direction.ASC, "lastname", "firstname"));
		assertThat(page.isFirst(), is(true));
		assertThat(page.isLast(), is(false));
		assertThat(page.getNumberOfElements(), is(2));
		assertThat(page, hasItems(carter, stefan));
	}

	@Test
	public void findsPersonInAgeRangeCorrectly() throws Exception {

		List<Person> result = repository.findByAgeBetween(40, 45);
		assertThat(result.size(), is(2));
		assertThat(result, hasItems(dave, leroi));
	}

	@Test
	public void findsPersonByShippingAddressesCorrectly() throws Exception {

		Address address = new Address("Foo Street 1", "C0123", "Bar");
		dave.setShippingAddresses(new HashSet<Address>(asList(address)));

		repository.save(dave);
		assertThat(repository.findByShippingAddresses(address), is(dave));
	}

	@Test
	public void findsPersonByAddressCorrectly() throws Exception {

		Address address = new Address("Foo Street 1", "C0123", "Bar");
		dave.setAddress(address);
		repository.save(dave);

		List<Person> result = repository.findByAddress(address);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test
	public void findsPeopleByZipCode() throws Exception {

		Address address = new Address("Foo Street 1", "C0123", "Bar");
		dave.setAddress(address);
		repository.save(dave);

		List<Person> result = repository.findByAddressZipCode(address.getZipCode());
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test
	public void findsPeopleByQueryDslLastnameSpec() throws Exception {

		Iterable<Person> result = repository.findAll(person.lastname.eq("Matthews"));
		assertThat(result, hasItem(dave));
		assertThat(result, not(hasItems(carter, boyd, stefan, leroi, alicia)));
	}

	@Test
	public void findsPeopleByzipCodePredicate() throws Exception {

		Address address = new Address("Foo Street 1", "C0123", "Bar");
		dave.setAddress(address);
		repository.save(dave);

		Iterable<Person> result = repository.findAll(person.address.zipCode.eq("C0123"));
		assertThat(result, hasItem(dave));
		assertThat(result, not(hasItems(carter, boyd, stefan, leroi, alicia)));
	}

	@Test
	public void findsPeopleByLocationNear() {
		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		List<Person> result = repository.findByLocationNear(point);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test // DATAMONGO-1588
	public void findsPeopleByLocationNearUsingGeoJsonType() {

		GeoJsonPoint point = new GeoJsonPoint(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		List<Person> result = repository.findByLocationNear(point);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test
	public void findsPeopleByLocationWithinCircle() {
		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		List<Person> result = repository.findByLocationWithin(new Circle(-78.99171, 45.738868, 170));
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test
	public void findsPeopleByLocationWithinBox() {
		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		Box box = new Box(new Point(-78.99171, 35.738868), new Point(-68.99171, 45.738868));

		List<Person> result = repository.findByLocationWithin(box);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test
	public void findsPeopleByLocationWithinPolygon() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		Point first = new Point(-78.99171, 35.738868);
		Point second = new Point(-78.99171, 45.738868);
		Point third = new Point(-68.99171, 45.738868);
		Point fourth = new Point(-68.99171, 35.738868);

		List<Person> result = repository.findByLocationWithin(new Polygon(first, second, third, fourth));
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test
	public void findsPagedPeopleByPredicate() throws Exception {

		Page<Person> page = repository.findAll(person.lastname.contains("a"),
				PageRequest.of(0, 2, Direction.ASC, "lastname"));
		assertThat(page.isFirst(), is(true));
		assertThat(page.isLast(), is(false));
		assertThat(page.getNumberOfElements(), is(2));
		assertThat(page.getTotalElements(), is(4L));
		assertThat(page, hasItems(carter, stefan));
	}

	@Test // DATADOC-136
	public void findsPeopleBySexCorrectly() {

		List<Person> females = repository.findBySex(Sex.FEMALE);
		assertThat(females.size(), is(1));
		assertThat(females.get(0), is(alicia));
	}

	@Test // DATAMONGO-446
	public void findsPeopleBySexPaginated() {

		List<Person> males = repository.findBySex(Sex.MALE, PageRequest.of(0, 2));
		assertThat(males.size(), is(2));
	}

	@Test
	public void findsPeopleByNamedQuery() {
		List<Person> result = repository.findByNamedQuery("Dave");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test // DATADOC-190
	public void existsWorksCorrectly() {
		assertThat(repository.existsById(dave.getId()), is(true));
	}

	@Test(expected = DuplicateKeyException.class)
	public void rejectsDuplicateEmailAddressOnSave() {

		assertThat(dave.getEmail(), is("dave@dmband.com"));

		Person daveSyer = new Person("Dave", "Syer");
		assertThat(daveSyer.getEmail(), is("dave@dmband.com"));

		repository.save(daveSyer);
	}

	@Test // DATADOC-236
	public void findsPeopleByLastnameAndOrdersCorrectly() {
		List<Person> result = repository.findByLastnameOrderByFirstnameAsc("Matthews");
		assertThat(result.size(), is(2));
		assertThat(result.get(0), is(dave));
		assertThat(result.get(1), is(oliver));
	}

	@Test // DATADOC-236
	public void appliesStaticAndDynamicSorting() {
		List<Person> result = repository.findByFirstnameLikeOrderByLastnameAsc("*e*", Sort.by("age"));
		assertThat(result.size(), is(5));
		assertThat(result.get(0), is(carter));
		assertThat(result.get(1), is(stefan));
		assertThat(result.get(2), is(oliver));
		assertThat(result.get(3), is(dave));
		assertThat(result.get(4), is(leroi));
	}

	@Test
	public void executesGeoNearQueryForResultsCorrectly() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		GeoResults<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73),
				new Distance(2000, Metrics.KILOMETERS));
		assertThat(results.getContent().isEmpty(), is(false));
	}

	@Test
	public void executesGeoPageQueryForResultsCorrectly() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73),
				new Distance(2000, Metrics.KILOMETERS), PageRequest.of(0, 20));
		assertThat(results.getContent().isEmpty(), is(false));

		// DATAMONGO-607
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	@Test // DATAMONGO-323
	public void considersSortForAnnotatedQuery() {

		List<Person> result = repository.findByAgeLessThan(60, Sort.by("firstname"));

		assertThat(result.size(), is(7));
		assertThat(result.get(0), is(alicia));
		assertThat(result.get(1), is(boyd));
		assertThat(result.get(2), is(carter));
		assertThat(result.get(3), is(dave));
		assertThat(result.get(4), is(leroi));
		assertThat(result.get(5), is(oliver));
		assertThat(result.get(6), is(stefan));
	}

	@Test // DATAMONGO-347
	public void executesQueryWithDBRefReferenceCorrectly() {

		operations.remove(new org.springframework.data.mongodb.core.query.Query(), User.class);

		User user = new User();
		user.username = "Oliver";

		operations.save(user);

		dave.creator = user;
		repository.save(dave);

		List<Person> result = repository.findByCreator(user);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test // DATAMONGO-425
	public void bindsDateParameterForLessThanPredicateCorrectly() {

		List<Person> result = repository.findByCreatedAtLessThan(boyd.createdAt);
		assertThat(result.size(), is(3));
		assertThat(result, hasItems(dave, oliver, carter));
	}

	@Test // DATAMONGO-425
	public void bindsDateParameterForGreaterThanPredicateCorrectly() {

		List<Person> result = repository.findByCreatedAtGreaterThan(carter.createdAt);
		assertThat(result.size(), is(4));
		assertThat(result, hasItems(boyd, stefan, leroi, alicia));
	}

	@Test // DATAMONGO-427
	public void bindsDateParameterToBeforePredicateCorrectly() {

		List<Person> result = repository.findByCreatedAtBefore(boyd.createdAt);
		assertThat(result.size(), is(3));
		assertThat(result, hasItems(dave, oliver, carter));
	}

	@Test // DATAMONGO-427
	public void bindsDateParameterForAfterPredicateCorrectly() {

		List<Person> result = repository.findByCreatedAtAfter(carter.createdAt);
		assertThat(result.size(), is(4));
		assertThat(result, hasItems(boyd, stefan, leroi, alicia));
	}

	@Test // DATAMONGO-425
	public void bindsDateParameterForManuallyDefinedQueryCorrectly() {

		List<Person> result = repository.findByCreatedAtLessThanManually(boyd.createdAt);
		assertThat(result.isEmpty(), is(false));
	}

	@Test // DATAMONGO-472
	public void findsPeopleUsingNotPredicate() {

		List<Person> result = repository.findByLastnameNot("Matthews");
		assertThat(result, not(hasItem(dave)));
		assertThat(result, hasSize(5));
	}

	@Test // DATAMONGO-521
	public void executesAndQueryCorrectly() {

		List<Person> result = repository.findByFirstnameAndLastname("Dave", "Matthews");

		assertThat(result, hasSize(1));
		assertThat(result, hasItem(dave));

		result = repository.findByFirstnameAndLastname("Oliver August", "Matthews");

		assertThat(result, hasSize(1));
		assertThat(result, hasItem(oliver));
	}

	@Test // DATAMONGO-600
	public void readsDocumentsWithNestedPolymorphismCorrectly() {

		UsernameAndPassword usernameAndPassword = new UsernameAndPassword();
		usernameAndPassword.username = "dave";
		usernameAndPassword.password = "btcs";

		dave.credentials = usernameAndPassword;

		repository.save(dave);

		List<Person> result = repository.findByCredentials(usernameAndPassword);
		assertThat(result, hasSize(1));
		assertThat(result, hasItem(dave));
	}

	@Test // DATAMONGO-636
	public void executesDerivedCountProjection() {
		assertThat(repository.countByLastname("Matthews"), is(2L));
	}

	@Test // DATAMONGO-636
	public void executesDerivedCountProjectionToInt() {
		assertThat(repository.countByFirstname("Oliver August"), is(1));
	}

	@Test // DATAMONGO-636
	public void executesAnnotatedCountProjection() {
		assertThat(repository.someCountQuery("Matthews"), is(2L));
	}

	@Test // DATAMONGO-1454
	public void executesDerivedExistsProjectionToBoolean() {

		assertThat(repository.existsByFirstname("Oliver August"), is(true));
		assertThat(repository.existsByFirstname("Hans Peter"), is(false));
	}

	@Test // DATAMONGO-1454
	public void executesAnnotatedExistProjection() {
		assertThat(repository.someExistQuery("Matthews"), is(true));
	}

	@Test // DATAMONGO-701
	public void executesDerivedStartsWithQueryCorrectly() {

		List<Person> result = repository.findByLastnameStartsWith("Matt");
		assertThat(result, hasSize(2));
		assertThat(result, hasItems(dave, oliver));
	}

	@Test // DATAMONGO-701
	public void executesDerivedEndsWithQueryCorrectly() {

		List<Person> result = repository.findByLastnameEndsWith("thews");
		assertThat(result, hasSize(2));
		assertThat(result, hasItems(dave, oliver));
	}

	@Test // DATAMONGO-445
	public void executesGeoPageQueryForWithPageRequestForPageInBetween() {

		Point farAway = new Point(-73.9, 40.7);
		Point here = new Point(-73.99, 40.73);

		dave.setLocation(farAway);
		oliver.setLocation(here);
		carter.setLocation(here);
		boyd.setLocation(here);
		leroi.setLocation(here);

		repository.saveAll(Arrays.asList(dave, oliver, carter, boyd, leroi));

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73),
				new Distance(2000, Metrics.KILOMETERS), PageRequest.of(1, 2));

		assertThat(results.getContent().isEmpty(), is(false));
		assertThat(results.getNumberOfElements(), is(2));
		assertThat(results.isFirst(), is(false));
		assertThat(results.isLast(), is(false));
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
		assertThat(results.getAverageDistance().getNormalizedValue(), is(0.0));
	}

	@Test // DATAMONGO-445
	public void executesGeoPageQueryForWithPageRequestForPageAtTheEnd() {

		Point point = new Point(-73.99171, 40.738868);

		dave.setLocation(point);
		oliver.setLocation(point);
		carter.setLocation(point);

		repository.saveAll(Arrays.asList(dave, oliver, carter));

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73),
				new Distance(2000, Metrics.KILOMETERS), PageRequest.of(1, 2));
		assertThat(results.getContent().isEmpty(), is(false));
		assertThat(results.getNumberOfElements(), is(1));
		assertThat(results.isFirst(), is(false));
		assertThat(results.isLast(), is(true));
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	@Test // DATAMONGO-445
	public void executesGeoPageQueryForWithPageRequestForJustOneElement() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73),
				new Distance(2000, Metrics.KILOMETERS), PageRequest.of(0, 2));

		assertThat(results.getContent().isEmpty(), is(false));
		assertThat(results.getNumberOfElements(), is(1));
		assertThat(results.isFirst(), is(true));
		assertThat(results.isLast(), is(true));
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	@Test // DATAMONGO-445
	public void executesGeoPageQueryForWithPageRequestForJustOneElementEmptyPage() {

		dave.setLocation(new Point(-73.99171, 40.738868));
		repository.save(dave);

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73),
				new Distance(2000, Metrics.KILOMETERS), PageRequest.of(1, 2));

		assertThat(results.getContent().isEmpty(), is(true));
		assertThat(results.getNumberOfElements(), is(0));
		assertThat(results.isFirst(), is(false));
		assertThat(results.isLast(), is(true));
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	@Test // DATAMONGO-1608
	public void findByFirstNameIgnoreCaseWithNull() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("property 'firstname'");

		repository.findByFirstnameIgnoreCase(null);
	}

	@Test // DATAMONGO-770
	public void findByFirstNameIgnoreCase() {

		List<Person> result = repository.findByFirstnameIgnoreCase("dave");

		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(dave));
	}

	@Test // DATAMONGO-770
	public void findByFirstnameNotIgnoreCase() {

		List<Person> result = repository.findByFirstnameNotIgnoreCase("dave");

		assertThat(result.size(), is(6));
		assertThat(result, not(hasItem(dave)));
	}

	@Test // DATAMONGO-770
	public void findByFirstnameStartingWithIgnoreCase() {

		List<Person> result = repository.findByFirstnameStartingWithIgnoreCase("da");
		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(dave));
	}

	@Test // DATAMONGO-770
	public void findByFirstnameEndingWithIgnoreCase() {

		List<Person> result = repository.findByFirstnameEndingWithIgnoreCase("VE");
		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(dave));
	}

	@Test // DATAMONGO-770
	public void findByFirstnameContainingIgnoreCase() {

		List<Person> result = repository.findByFirstnameContainingIgnoreCase("AV");
		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(dave));
	}

	@Test // DATAMONGO-870
	public void findsSliceOfPersons() {

		Slice<Person> result = repository.findByAgeGreaterThan(40, PageRequest.of(0, 2, Direction.DESC, "firstname"));

		assertThat(result.hasNext(), is(true));
	}

	@Test // DATAMONGO-871
	public void findsPersonsByFirstnameAsArray() {

		Person[] result = repository.findByThePersonsFirstnameAsArray("Leroi");

		assertThat(result, is(arrayWithSize(1)));
		assertThat(result, is(arrayContaining(leroi)));
	}

	@Test // DATAMONGO-821
	public void findUsingAnnotatedQueryOnDBRef() {

		operations.remove(new org.springframework.data.mongodb.core.query.Query(), User.class);

		User user = new User();
		user.username = "Terria";
		operations.save(user);

		alicia.creator = user;
		repository.save(alicia);

		Page<Person> result = repository.findByHavingCreator(PageRequest.of(0, 100));

		assertThat(result.getNumberOfElements(), is(1));
		assertThat(result.getContent().get(0), is(alicia));
	}

	@Test // DATAMONGO-566
	public void deleteByShouldReturnListOfDeletedElementsWhenRetunTypeIsCollectionLike() {

		List<Person> result = repository.deleteByLastname("Beauford");

		assertThat(result, hasItem(carter));
		assertThat(result, hasSize(1));
	}

	@Test // DATAMONGO-566
	public void deleteByShouldRemoveElementsMatchingDerivedQuery() {

		repository.deleteByLastname("Beauford");

		assertThat(operations.count(new BasicQuery("{'lastname':'Beauford'}"), Person.class), is(0L));
	}

	@Test // DATAMONGO-566
	public void deleteByShouldReturnNumberOfDocumentsRemovedIfReturnTypeIsLong() {
		assertThat(repository.deletePersonByLastname("Beauford"), is(1L));
	}

	@Test // DATAMONGO-566
	public void deleteByShouldReturnZeroInCaseNoDocumentHasBeenRemovedAndReturnTypeIsNumber() {
		assertThat(repository.deletePersonByLastname("dorfuaeB"), is(0L));
	}

	@Test // DATAMONGO-566
	public void deleteByShouldReturnEmptyListInCaseNoDocumentHasBeenRemovedAndReturnTypeIsCollectionLike() {
		assertThat(repository.deleteByLastname("dorfuaeB"), empty());
	}

	@Test // DATAMONGO-566
	public void deleteByUsingAnnotatedQueryShouldReturnListOfDeletedElementsWhenRetunTypeIsCollectionLike() {

		List<Person> result = repository.removeByLastnameUsingAnnotatedQuery("Beauford");

		assertThat(result, hasItem(carter));
		assertThat(result, hasSize(1));
	}

	@Test // DATAMONGO-566
	public void deleteByUsingAnnotatedQueryShouldRemoveElementsMatchingDerivedQuery() {

		repository.removeByLastnameUsingAnnotatedQuery("Beauford");

		assertThat(operations.count(new BasicQuery("{'lastname':'Beauford'}"), Person.class), is(0L));
	}

	@Test // DATAMONGO-566
	public void deleteByUsingAnnotatedQueryShouldReturnNumberOfDocumentsRemovedIfReturnTypeIsLong() {
		assertThat(repository.removePersonByLastnameUsingAnnotatedQuery("Beauford"), is(1L));
	}

	@Test // DATAMONGO-893
	public void findByNestedPropertyInCollectionShouldFindMatchingDocuments() {

		Person p = new Person("Mary", "Poppins");
		Address adr = new Address("some", "2", "where");
		p.setAddress(adr);

		repository.save(p);

		Page<Person> result = repository.findByAddressIn(Arrays.asList(adr), PageRequest.of(0, 10));

		assertThat(result.getContent(), hasSize(1));
	}

	@Test // DATAMONGO-745
	public void findByCustomQueryFirstnamesInListAndLastname() {

		repository.save(new Person("foo", "bar"));
		repository.save(new Person("bar", "bar"));
		repository.save(new Person("fuu", "bar"));
		repository.save(new Person("notfound", "bar"));

		Page<Person> result = repository.findByCustomQueryFirstnamesAndLastname(Arrays.asList("bar", "foo", "fuu"), "bar",
				PageRequest.of(0, 2));

		assertThat(result.getContent(), hasSize(2));
		assertThat(result.getTotalPages(), is(2));
		assertThat(result.getTotalElements(), is(3L));
	}

	@Test // DATAMONGO-745
	public void findByCustomQueryLastnameAndStreetInList() {

		repository.save(new Person("foo", "bar").withAddress(new Address("street1", "1", "SB")));
		repository.save(new Person("bar", "bar").withAddress(new Address("street2", "1", "SB")));
		repository.save(new Person("fuu", "bar").withAddress(new Address("street1", "2", "RGB")));
		repository.save(new Person("notfound", "notfound"));

		Page<Person> result = repository.findByCustomQueryLastnameAndAddressStreetInList("bar",
				Arrays.asList("street1", "street2"), PageRequest.of(0, 2));

		assertThat(result.getContent(), hasSize(2));
		assertThat(result.getTotalPages(), is(2));
		assertThat(result.getTotalElements(), is(3L));

	}

	@Test // DATAMONGO-950
	public void shouldLimitCollectionQueryToMaxResultsWhenPresent() {

		repository.saveAll(Arrays.asList(new Person("Bob-1", "Dylan"), new Person("Bob-2", "Dylan"),
				new Person("Bob-3", "Dylan"), new Person("Bob-4", "Dylan"), new Person("Bob-5", "Dylan")));
		List<Person> result = repository.findTop3ByLastnameStartingWith("Dylan");
		assertThat(result.size(), is(3));
	}

	@Test // DATAMONGO-950, DATAMONGO-1464
	public void shouldNotLimitPagedQueryWhenPageRequestWithinBounds() {

		repository.saveAll(Arrays.asList(new Person("Bob-1", "Dylan"), new Person("Bob-2", "Dylan"),
				new Person("Bob-3", "Dylan"), new Person("Bob-4", "Dylan"), new Person("Bob-5", "Dylan")));
		Page<Person> result = repository.findTop3ByLastnameStartingWith("Dylan", PageRequest.of(0, 2));
		assertThat(result.getContent().size(), is(2));
		assertThat(result.getTotalElements(), is(3L));
	}

	@Test // DATAMONGO-950
	public void shouldLimitPagedQueryWhenPageRequestExceedsUpperBoundary() {

		repository.saveAll(Arrays.asList(new Person("Bob-1", "Dylan"), new Person("Bob-2", "Dylan"),
				new Person("Bob-3", "Dylan"), new Person("Bob-4", "Dylan"), new Person("Bob-5", "Dylan")));
		Page<Person> result = repository.findTop3ByLastnameStartingWith("Dylan", PageRequest.of(1, 2));
		assertThat(result.getContent().size(), is(1));
	}

	@Test // DATAMONGO-950, DATAMONGO-1464
	public void shouldReturnEmptyWhenPageRequestedPageIsTotallyOutOfScopeForLimit() {

		repository.saveAll(Arrays.asList(new Person("Bob-1", "Dylan"), new Person("Bob-2", "Dylan"),
				new Person("Bob-3", "Dylan"), new Person("Bob-4", "Dylan"), new Person("Bob-5", "Dylan")));
		Page<Person> result = repository.findTop3ByLastnameStartingWith("Dylan", PageRequest.of(100, 2));
		assertThat(result.getContent().size(), is(0));
		assertThat(result.getTotalElements(), is(3L));
	}

	@Test // DATAMONGO-996, DATAMONGO-950, DATAMONGO-1464
	public void gettingNonFirstPageWorksWithoutLimitBeingSet() {

		Page<Person> slice = repository.findByLastnameLike("Matthews", PageRequest.of(1, 1));

		assertThat(slice.getContent(), hasSize(1));
		assertThat(slice.hasPrevious(), is(true));
		assertThat(slice.hasNext(), is(false));
		assertThat(slice.getTotalElements(), is(2L));
	}

	@Test // DATAMONGO-972
	public void shouldExecuteFindOnDbRefCorrectly() {

		operations.remove(new org.springframework.data.mongodb.core.query.Query(), User.class);

		User user = new User();
		user.setUsername("Valerie Matthews");

		operations.save(user);

		dave.setCreator(user);
		operations.save(dave);

		assertThat(repository.findOne(QPerson.person.creator.eq(user)), is(dave));
	}

	@Test // DATAMONGO-969
	public void shouldFindPersonsWhenUsingQueryDslPerdicatedOnIdProperty() {
		assertThat(repository.findAll(person.id.in(Arrays.asList(dave.id, carter.id))), containsInAnyOrder(dave, carter));
	}

	@Test // DATAMONGO-1030
	public void executesSingleEntityQueryWithProjectionCorrectly() {

		PersonSummary result = repository.findSummaryByLastname("Beauford");

		assertThat(result, is(notNullValue()));
		assertThat(result.firstname, is("Carter"));
		assertThat(result.lastname, is("Beauford"));
	}

	@Test // DATAMONGO-1057
	public void sliceShouldTraverseElementsWithoutSkippingOnes() {

		repository.deleteAll();

		List<Person> persons = new ArrayList<Person>(100);
		for (int i = 0; i < 100; i++) {
			// format firstname to assert sorting retains proper order
			persons.add(new Person(String.format("%03d", i), "ln" + 1, 100));
		}

		repository.saveAll(persons);

		Slice<Person> slice = repository.findByAgeGreaterThan(50, PageRequest.of(0, 20, Direction.ASC, "firstname"));
		assertThat(slice, contains(persons.subList(0, 20).toArray()));

		slice = repository.findByAgeGreaterThan(50, slice.nextPageable());
		assertThat(slice, contains(persons.subList(20, 40).toArray()));
	}

	@Test // DATAMONGO-1072
	public void shouldBindPlaceholdersUsedAsKeysCorrectly() {

		List<Person> persons = repository.findByKeyValue("firstname", alicia.getFirstname());

		assertThat(persons, hasSize(1));
		assertThat(persons, hasItem(alicia));
	}

	@Test // DATAMONGO-1105
	public void returnsOrderedResultsForQuerydslOrderSpecifier() {

		Iterable<Person> result = repository.findAll(person.firstname.asc());

		assertThat(result, contains(alicia, boyd, carter, dave, leroi, oliver, stefan));
	}

	@Test // DATAMONGO-1085
	public void shouldSupportSortingByQueryDslOrderSpecifier() {

		repository.deleteAll();

		List<Person> persons = new ArrayList<Person>();

		for (int i = 0; i < 3; i++) {
			Person person = new Person(String.format("Siggi %s", i), "Bar", 30);
			person.setAddress(new Address(String.format("Street %s", i), "12345", "SinCity"));
			persons.add(person);
		}

		repository.saveAll(persons);

		QPerson person = QPerson.person;

		Iterable<Person> result = repository.findAll(person.firstname.isNotNull(), person.address.street.desc());

		assertThat(result, is(Matchers.<Person> iterableWithSize(persons.size())));
		assertThat(result.iterator().next().getFirstname(), is(persons.get(2).getFirstname()));
	}

	@Test // DATAMONGO-1085
	public void shouldSupportSortingWithQSortByQueryDslOrderSpecifier() throws Exception {

		repository.deleteAll();

		List<Person> persons = new ArrayList<Person>();

		for (int i = 0; i < 3; i++) {
			Person person = new Person(String.format("Siggi %s", i), "Bar", 30);
			person.setAddress(new Address(String.format("Street %s", i), "12345", "SinCity"));
			persons.add(person);
		}

		repository.saveAll(persons);

		PageRequest pageRequest = PageRequest.of(0, 2, new QSort(person.address.street.desc()));
		Iterable<Person> result = repository.findAll(pageRequest);

		assertThat(result, is(Matchers.<Person> iterableWithSize(2)));
		assertThat(result.iterator().next().getFirstname(), is("Siggi 2"));
	}

	@Test // DATAMONGO-1085
	public void shouldSupportSortingWithQSort() throws Exception {

		repository.deleteAll();

		List<Person> persons = new ArrayList<Person>();

		for (int i = 0; i < 3; i++) {
			Person person = new Person(String.format("Siggi %s", i), "Bar", 30);
			person.setAddress(new Address(String.format("Street %s", i), "12345", "SinCity"));
			persons.add(person);
		}

		repository.saveAll(persons);

		Iterable<Person> result = repository.findAll(new QSort(person.address.street.desc()));

		assertThat(result, is(Matchers.<Person> iterableWithSize(persons.size())));
		assertThat(result.iterator().next().getFirstname(), is("Siggi 2"));
	}

	@Test // DATAMONGO-1165
	public void shouldAllowReturningJava8StreamInCustomQuery() throws Exception {

		Stream<Person> result = repository.findByCustomQueryWithStreamingCursorByFirstnames(Arrays.asList("Dave"));

		try {
			assertThat(result.collect(Collectors.<Person> toList()), hasItems(dave));
		} finally {
			result.close();
		}
	}

	@Test // DATAMONGO-1110
	public void executesGeoNearQueryForResultsCorrectlyWhenGivenMinAndMaxDistance() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		Range<Distance> range = Distance.between(new Distance(0.01, KILOMETERS), new Distance(2000, KILOMETERS));

		GeoResults<Person> results = repository.findPersonByLocationNear(new Point(-73.99, 40.73), range);
		assertThat(results.getContent().isEmpty(), is(false));
	}

	@Test // DATAMONGO-990
	public void shouldFindByFirstnameForSpELExpressionWithParameterIndexOnly() {

		List<Person> users = repository.findWithSpelByFirstnameForSpELExpressionWithParameterIndexOnly("Dave");

		assertThat(users, hasSize(1));
		assertThat(users.get(0), is(dave));
	}

	@Test // DATAMONGO-990
	public void shouldFindByFirstnameAndCurrentUserWithCustomQuery() {

		SampleSecurityContextHolder.getCurrent().setPrincipal(dave);
		List<Person> users = repository.findWithSpelByFirstnameAndCurrentUserWithCustomQuery("Dave");

		assertThat(users, hasSize(1));
		assertThat(users.get(0), is(dave));
	}

	@Test // DATAMONGO-990
	public void shouldFindByFirstnameForSpELExpressionWithParameterVariableOnly() {

		List<Person> users = repository.findWithSpelByFirstnameForSpELExpressionWithParameterVariableOnly("Dave");

		assertThat(users, hasSize(1));
		assertThat(users.get(0), is(dave));
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldResolveStuffCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");

		// needed to tweak stuff a bit since some field are automatically set - so we need to undo this
		ReflectionTestUtils.setField(sample, "id", null);
		ReflectionTestUtils.setField(sample, "createdAt", null);
		ReflectionTestUtils.setField(sample, "email", null);

		Page<Person> result = repository.findAll(Example.of(sample), PageRequest.of(0, 10));
		assertThat(result.getNumberOfElements(), is(2));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldResolveStuffCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");

		// needed to tweak stuff a bit since some field are automatically set - so we need to undo this
		ReflectionTestUtils.setField(sample, "id", null);
		ReflectionTestUtils.setField(sample, "createdAt", null);
		ReflectionTestUtils.setField(sample, "email", null);

		List<Person> result = repository.findAll(Example.of(sample));
		assertThat(result.size(), is(2));
	}

	@Test // DATAMONGO-1425
	public void findsPersonsByFirstnameNotContains() throws Exception {

		List<Person> result = repository.findByFirstnameNotContains("Boyd");
		assertThat(result.size(), is((int) (repository.count() - 1)));
		assertThat(result, not(hasItem(boyd)));
	}

	@Test // DATAMONGO-1425
	public void findBySkillsContains() throws Exception {

		List<Person> result = repository.findBySkillsContains(Arrays.asList("Drums"));
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(carter));
	}

	@Test // DATAMONGO-1425
	public void findBySkillsNotContains() throws Exception {

		List<Person> result = repository.findBySkillsNotContains(Arrays.asList("Drums"));
		assertThat(result.size(), is((int) (repository.count() - 1)));
		assertThat(result, not(hasItem(carter)));
	}

	@Test // DATAMONGO-1424
	public void findsPersonsByFirstnameNotLike() throws Exception {

		List<Person> result = repository.findByFirstnameNotLike("Bo*");
		assertThat(result.size(), is((int) (repository.count() - 1)));
		assertThat(result, not(hasItem(boyd)));
	}

	@Test // DATAMONGO-1539
	public void countsPersonsByFirstname() {
		assertThat(repository.countByThePersonsFirstname("Dave"), is(1L));
	}

	@Test // DATAMONGO-1539
	public void deletesPersonsByFirstname() {

		repository.deleteByThePersonsFirstname("Dave");

		assertThat(repository.countByThePersonsFirstname("Dave"), is(0L));
	}
}

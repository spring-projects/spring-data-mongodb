/*
 * Copyright 2011-2014 the original author or authors.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.geo.Box;
import org.springframework.data.mongodb.core.geo.Circle;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.GeoPage;
import org.springframework.data.mongodb.core.geo.GeoResults;
import org.springframework.data.mongodb.core.geo.Metric;
import org.springframework.data.mongodb.core.geo.Metrics;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.geo.Polygon;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Base class for tests for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractPersonRepositoryIntegrationTests {

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
		Thread.sleep(10);
		boyd = new Person("Boyd", "Tinsley", 45);
		stefan = new Person("Stefan", "Lessard", 34);
		leroi = new Person("Leroi", "Moore", 41);

		alicia = new Person("Alicia", "Keys", 30, Sex.FEMALE);

		person = new QPerson("person");

		all = repository.save(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia));
	}

	@Test
	public void findsPersonById() throws Exception {

		assertThat(repository.findOne(dave.getId().toString()), is(dave));
	}

	@Test
	public void findsAllMusicians() throws Exception {
		List<Person> result = repository.findAll();
		assertThat(result.size(), is(all.size()));
		assertThat(result.containsAll(all), is(true));
	}

	@Test
	public void findsAllWithGivenIds() {

		Iterable<Person> result = repository.findAll(Arrays.asList(dave.id, boyd.id));
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

		repository.delete(dave.getId().toString());

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

	@Test
	public void findsPagedPersons() throws Exception {

		Page<Person> result = repository.findAll(new PageRequest(1, 2, Direction.ASC, "lastname", "firstname"));
		assertThat(result.isFirstPage(), is(false));
		assertThat(result.isLastPage(), is(false));
		assertThat(result, hasItems(dave, stefan));
	}

	@Test
	public void executesPagedFinderCorrectly() throws Exception {

		Page<Person> page = repository.findByLastnameLike("*a*", new PageRequest(0, 2, Direction.ASC, "lastname",
				"firstname"));
		assertThat(page.isFirstPage(), is(true));
		assertThat(page.isLastPage(), is(false));
		assertThat(page.getNumberOfElements(), is(2));
		assertThat(page, hasItems(carter, stefan));
	}

	@Test
	public void executesPagedFinderWithAnnotatedQueryCorrectly() throws Exception {

		Page<Person> page = repository.findByLastnameLikeWithPageable(".*a.*", new PageRequest(0, 2, Direction.ASC,
				"lastname", "firstname"));
		assertThat(page.isFirstPage(), is(true));
		assertThat(page.isLastPage(), is(false));
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

		Page<Person> page = repository.findAll(person.lastname.contains("a"), new PageRequest(0, 2, Direction.ASC,
				"lastname"));
		assertThat(page.isFirstPage(), is(true));
		assertThat(page.isLastPage(), is(false));
		assertThat(page.getNumberOfElements(), is(2));
		assertThat(page, hasItems(carter, stefan));
	}

	/**
	 * @see DATADOC-136
	 */
	@Test
	public void findsPeopleBySexCorrectly() {

		List<Person> females = repository.findBySex(Sex.FEMALE);
		assertThat(females.size(), is(1));
		assertThat(females.get(0), is(alicia));
	}

	/**
	 * @see DATAMONGO-446
	 */
	@Test
	public void findsPeopleBySexPaginated() {

		List<Person> males = repository.findBySex(Sex.MALE, new PageRequest(0, 2));
		assertThat(males.size(), is(2));
	}

	@Test
	public void findsPeopleByNamedQuery() {
		List<Person> result = repository.findByNamedQuery("Dave");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	/**
	 * @see DATADOC-190
	 */
	@Test
	public void existsWorksCorrectly() {
		assertThat(repository.exists(dave.getId()), is(true));
	}

	@Test(expected = DuplicateKeyException.class)
	public void rejectsDuplicateEmailAddressOnSave() {

		assertThat(dave.getEmail(), is("dave@dmband.com"));

		Person daveSyer = new Person("Dave", "Syer");
		assertThat(daveSyer.getEmail(), is("dave@dmband.com"));

		repository.save(daveSyer);
	}

	/**
	 * @see DATADOC-236
	 */
	@Test
	public void findsPeopleByLastnameAndOrdersCorrectly() {
		List<Person> result = repository.findByLastnameOrderByFirstnameAsc("Matthews");
		assertThat(result.size(), is(2));
		assertThat(result.get(0), is(dave));
		assertThat(result.get(1), is(oliver));
	}

	/**
	 * @see DATADOC-236
	 */
	@Test
	public void appliesStaticAndDynamicSorting() {
		List<Person> result = repository.findByFirstnameLikeOrderByLastnameAsc("*e*", new Sort("age"));
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

		GeoResults<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73), new Distance(2000,
				Metrics.KILOMETERS));
		assertThat(results.getContent().isEmpty(), is(false));
	}

	@Test
	public void executesGeoPageQueryForResultsCorrectly() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73), new Distance(2000,
				Metrics.KILOMETERS), new PageRequest(0, 20));
		assertThat(results.getContent().isEmpty(), is(false));

		// DATAMONGO-607
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	/**
	 * @see DATAMONGO-323
	 */
	@Test
	public void considersSortForAnnotatedQuery() {

		List<Person> result = repository.findByAgeLessThan(60, new Sort("firstname"));

		assertThat(result.size(), is(7));
		assertThat(result.get(0), is(alicia));
		assertThat(result.get(1), is(boyd));
		assertThat(result.get(2), is(carter));
		assertThat(result.get(3), is(dave));
		assertThat(result.get(4), is(leroi));
		assertThat(result.get(5), is(oliver));
		assertThat(result.get(6), is(stefan));
	}

	/**
	 * @see DATAMONGO-347
	 */
	@Test
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

	/**
	 * @see DATAMONGO-425
	 */
	@Test
	public void bindsDateParameterForLessThanPredicateCorrectly() {

		List<Person> result = repository.findByCreatedAtLessThan(boyd.createdAt);
		assertThat(result.size(), is(3));
		assertThat(result, hasItems(dave, oliver, carter));
	}

	/**
	 * @see DATAMONGO-425
	 */
	@Test
	public void bindsDateParameterForGreaterThanPredicateCorrectly() {

		List<Person> result = repository.findByCreatedAtGreaterThan(carter.createdAt);
		assertThat(result.size(), is(4));
		assertThat(result, hasItems(boyd, stefan, leroi, alicia));
	}

	/**
	 * @see DATAMONGO-427
	 */
	@Test
	public void bindsDateParameterToBeforePredicateCorrectly() {

		List<Person> result = repository.findByCreatedAtBefore(boyd.createdAt);
		assertThat(result.size(), is(3));
		assertThat(result, hasItems(dave, oliver, carter));
	}

	/**
	 * @see DATAMONGO-427
	 */
	@Test
	public void bindsDateParameterForAfterPredicateCorrectly() {

		List<Person> result = repository.findByCreatedAtAfter(carter.createdAt);
		assertThat(result.size(), is(4));
		assertThat(result, hasItems(boyd, stefan, leroi, alicia));
	}

	/**
	 * @see DATAMONGO-425
	 */
	@Test
	public void bindsDateParameterForManuallyDefinedQueryCorrectly() {

		List<Person> result = repository.findByCreatedAtLessThanManually(boyd.createdAt);
		assertThat(result.isEmpty(), is(false));
	}

	/**
	 * @see DATAMONGO-472
	 */
	@Test
	public void findsPeopleUsingNotPredicate() {

		List<Person> result = repository.findByLastnameNot("Matthews");
		assertThat(result, not(hasItem(dave)));
		assertThat(result, hasSize(5));
	}

	/**
	 * @see DATAMONGO-521
	 */
	@Test
	public void executesAndQueryCorrectly() {

		List<Person> result = repository.findByFirstnameAndLastname("Dave", "Matthews");

		assertThat(result, hasSize(1));
		assertThat(result, hasItem(dave));

		result = repository.findByFirstnameAndLastname("Oliver August", "Matthews");

		assertThat(result, hasSize(1));
		assertThat(result, hasItem(oliver));
	}

	/**
	 * @see DATAMONGO-600
	 */
	@Test
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

	/**
	 * @see DATAMONGO-636
	 */
	@Test
	public void executesDerivedCountProjection() {
		assertThat(repository.countByLastname("Matthews"), is(2L));
	}

	/**
	 * @see DATAMONGO-636
	 */
	@Test
	public void executesDerivedCountProjectionToInt() {
		assertThat(repository.countByFirstname("Oliver August"), is(1));
	}

	/**
	 * @see DATAMONGO-636
	 */
	@Test
	public void executesAnnotatedCountProjection() {
		assertThat(repository.someCountQuery("Matthews"), is(2L));
	}

	/**
	 * @see DATAMONGO-701
	 */
	@Test
	public void executesDerivedStartsWithQueryCorrectly() {

		List<Person> result = repository.findByLastnameStartsWith("Matt");
		assertThat(result, hasSize(2));
		assertThat(result, hasItems(dave, oliver));
	}

	/**
	 * @see DATAMONGO-701
	 */
	@Test
	public void executesDerivedEndsWithQueryCorrectly() {

		List<Person> result = repository.findByLastnameEndsWith("thews");
		assertThat(result, hasSize(2));
		assertThat(result, hasItems(dave, oliver));
	}

	/**
	 * @see DATAMONGO-445
	 */
	@Test
	public void executesGeoPageQueryForWithPageRequestForPageInBetween() {

		Point farAway = new Point(-73.9, 40.7);
		Point here = new Point(-73.99, 40.73);

		dave.setLocation(farAway);
		oliver.setLocation(here);
		carter.setLocation(here);
		boyd.setLocation(here);
		leroi.setLocation(here);

		repository.save(Arrays.asList(dave, oliver, carter, boyd, leroi));

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73), new Distance(2000,
				Metrics.KILOMETERS), new PageRequest(1, 2));

		assertThat(results.getContent().isEmpty(), is(false));
		assertThat(results.getNumberOfElements(), is(2));
		assertThat(results.isFirstPage(), is(false));
		assertThat(results.isLastPage(), is(false));
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
		assertThat(results.getAverageDistance().getNormalizedValue(), is(0.0));
	}

	/**
	 * @see DATAMONGO-445
	 */
	@Test
	public void executesGeoPageQueryForWithPageRequestForPageAtTheEnd() {

		Point point = new Point(-73.99171, 40.738868);

		dave.setLocation(point);
		oliver.setLocation(point);
		carter.setLocation(point);

		repository.save(Arrays.asList(dave, oliver, carter));

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73), new Distance(2000,
				Metrics.KILOMETERS), new PageRequest(1, 2));
		assertThat(results.getContent().isEmpty(), is(false));
		assertThat(results.getNumberOfElements(), is(1));
		assertThat(results.isFirstPage(), is(false));
		assertThat(results.isLastPage(), is(true));
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	/**
	 * @see DATAMONGO-445
	 */
	@Test
	public void executesGeoPageQueryForWithPageRequestForJustOneElement() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave);

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73), new Distance(2000,
				Metrics.KILOMETERS), new PageRequest(0, 2));

		assertThat(results.getContent().isEmpty(), is(false));
		assertThat(results.getNumberOfElements(), is(1));
		assertThat(results.isFirstPage(), is(true));
		assertThat(results.isLastPage(), is(true));
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	/**
	 * @see DATAMONGO-445
	 */
	@Test
	public void executesGeoPageQueryForWithPageRequestForJustOneElementEmptyPage() {

		dave.setLocation(new Point(-73.99171, 40.738868));
		repository.save(dave);

		GeoPage<Person> results = repository.findByLocationNear(new Point(-73.99, 40.73), new Distance(2000,
				Metrics.KILOMETERS), new PageRequest(1, 2));

		assertThat(results.getContent().isEmpty(), is(true));
		assertThat(results.getNumberOfElements(), is(0));
		assertThat(results.isFirstPage(), is(false));
		assertThat(results.isLastPage(), is(true));
		assertThat(results.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void findByFirstNameIgnoreCase() {

		List<Person> result = repository.findByFirstnameIgnoreCase("dave");

		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(dave));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void findByFirstnameNotIgnoreCase() {

		List<Person> result = repository.findByFirstnameNotIgnoreCase("dave");

		assertThat(result.size(), is(6));
		assertThat(result, not(hasItem(dave)));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void findByFirstnameStartingWithIgnoreCase() {

		List<Person> result = repository.findByFirstnameStartingWithIgnoreCase("da");
		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(dave));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void findByFirstnameEndingWithIgnoreCase() {

		List<Person> result = repository.findByFirstnameEndingWithIgnoreCase("VE");
		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(dave));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void findByFirstnameContainingIgnoreCase() {

		List<Person> result = repository.findByFirstnameContainingIgnoreCase("AV");
		assertThat(result.size(), is(1));
		assertThat(result.get(0), is(dave));
	}
	
	/**
	 * @see DATAMONGO-871
	 */
	@Test
	public void findsPersonsByFirstnameAsArray() {

		Person[] result = repository.findByThePersonsFirstnameAsArray("Leroi");

		assertThat(result, is(arrayWithSize(1)));
		assertThat(result, is(arrayContaining(leroi)));
	}
	
	
	/**
	 * @see DATAMONGO-821
	 */
	@Test
	public void findUsingAnnotatedQueryOnDBRef() {

		operations.remove(new org.springframework.data.mongodb.core.query.Query(), User.class);

		User user = new User();
		user.username = "Terria";
		operations.save(user);

		alicia.creator = user;
		repository.save(alicia);

		Page<Person> result = repository.findByHavingCreator(new PageRequest(0, 100));
		assertThat(result.getNumberOfElements(), is(1));
		assertThat(result.getContent().get(0), is(alicia));
	}
}

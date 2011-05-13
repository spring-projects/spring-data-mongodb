package org.springframework.data.document.mongodb.repository;

import static java.util.Arrays.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.document.mongodb.geo.Box;
import org.springframework.data.document.mongodb.geo.Circle;
import org.springframework.data.document.mongodb.geo.Point;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Base class for tests for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractPersonRepositoryIntegrationTests {

	@Autowired
	protected PersonRepository repository;
	Person dave, carter, boyd, stefan, leroi;
	QPerson person;

	@Before
	public void setUp() {

		repository.deleteAll();

		dave = new Person("Dave", "Matthews", 42);
		carter = new Person("Carter", "Beauford", 49);
		boyd = new Person("Boyd", "Tinsley", 45);
		stefan = new Person("Stefan", "Lessard", 34);
		leroi = new Person("Leroi", "Moore", 41);

		person = new QPerson("person");

		repository.save(Arrays.asList(dave, carter, boyd, stefan, leroi));
	}

	@Test
	public void findsPersonById() throws Exception {

		assertThat(repository.findOne(dave.getId()), is(dave));
	}

	@Test
	public void findsAllMusicians() throws Exception {
		List<Person> result = repository.findAll();
		assertThat(result, hasItems(dave, carter, boyd, stefan, leroi));
		assertThat(result.size(), is(5));
	}

	@Test
	public void deletesPersonCorrectly() throws Exception {

		repository.delete(dave);

		List<Person> result = repository.findAll();

		assertThat(result.size(), is(4));
		assertThat(result, not(hasItem(dave)));
	}

	@Test
	public void deletesPersonByIdCorrectly() {

		repository.delete(dave.getId());

		List<Person> result = repository.findAll();

		assertThat(result.size(), is(4));
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

		Page<Person> result = repository.findAll(new PageRequest(1, 2, Direction.ASC, "lastname"));
		assertThat(result.isFirstPage(), is(false));
		assertThat(result.isLastPage(), is(false));
		assertThat(result, hasItems(dave, leroi));
	}

	@Test
	public void executesPagedFinderCorrectly() throws Exception {

		Page<Person> page = repository.findByLastnameLike("*a*", new PageRequest(0, 2, Direction.ASC, "lastname"));
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

		List<Person> result = repository.findAll(person.lastname.eq("Matthews"));
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
	}

	@Test
	public void findsPeopleByzipCodePredicate() throws Exception {

		Address address = new Address("Foo Street 1", "C0123", "Bar");
		dave.setAddress(address);
		repository.save(dave);

		List<Person> result = repository.findAll(person.address.zipCode.eq("C0123"));
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(dave));
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
	@Ignore
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
	public void findsPagedPeopleByPredicate() throws Exception {

		Page<Person> page = repository.findAll(person.lastname.contains("a"), new PageRequest(0, 2, Direction.ASC,
				"lastname"));
		assertThat(page.isFirstPage(), is(true));
		assertThat(page.isLastPage(), is(false));
		assertThat(page.getNumberOfElements(), is(2));
		assertThat(page, hasItems(carter, stefan));
	}
}
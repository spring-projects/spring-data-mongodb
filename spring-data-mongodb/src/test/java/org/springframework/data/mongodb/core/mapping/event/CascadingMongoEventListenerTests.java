package org.springframework.data.mongodb.core.mapping.event;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration tests for testing cascade operations on fields annotated with {@link org.springframework.data.mongodb.core.mapping.DBRef}
 * Contains two unit tests: save and delete for each option in {@link org.springframework.data.mongodb.core.mapping.CascadeType} enumeration
 *
 * @author Maciej Walkowiak
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CascadingMongoEventListenerTests {
	@Autowired
	private MongoOperations mongoOperations;

	/**
	 * Makes sure that collections used in tests are empty
	 */
	@Before
	public void cleanDatabase() {
		mongoOperations.dropCollection(Person.class);
		mongoOperations.dropCollection(Address.class);
	}

	/**
	 * Tests {@link MongoOperations#save(Object)} with {@link org.springframework.data.mongodb.core.mapping.CascadeType#SAVE}
	 */
	@Test
	public void testSavingWithCascadeSave() {
		// given
		Person person = createPersonWithCascadeSaveAddress();

		// when
		mongoOperations.save(person);

		// then
		List<Address> addresses = mongoOperations.findAll(Address.class);
		List<Person> persons = mongoOperations.findAll(Person.class);

		assertEquals(1, addresses.size());
		assertEquals(1, persons.size());

		assertEquals(persons.get(0).getAddressWithCascadeSave(), addresses.get(0));
	}

	/**
	 * Tests {@link MongoOperations#remove(Object)} with {@link org.springframework.data.mongodb.core.mapping.CascadeType#SAVE}
	 */
	@Test
	public void testDeletingWithCascadeSave() {
		// given
		Person person = createPersonWithCascadeSaveAddress();
		mongoOperations.save(person);

		// when
		mongoOperations.remove(person);

		// then
		assertEquals(0, mongoOperations.findAll(Person.class).size());
		assertEquals(1, mongoOperations.findAll(Address.class).size());
	}

	private Person createPersonWithCascadeSaveAddress() {
		Person person = new Person();
		person.setAddressWithCascadeSave(new Address());

		return person;
	}

	/**
	 * Tests {@link MongoOperations#save(Object)} with {@link org.springframework.data.mongodb.core.mapping.CascadeType#DELETE}
	 */
	@Test
	public void testSavingWithCascadeDelete() {
		// given
		Person person = createPersonWithCascadeDeleteAddress();

		try {
			// when
			mongoOperations.save(person);
			fail();

			//then
		} catch (MappingException e) {
			assertEquals("Cannot create a reference to an object with a NULL id.", e.getMessage());
		}
	}

	/**
	 * Tests {@link MongoOperations#remove(Object)} with {@link org.springframework.data.mongodb.core.mapping.CascadeType#DELETE}
	 */
	@Test
	public void testDeletingWithCascadeDelete() {
		// given
		Address address = new Address();
		mongoOperations.save(address);

		Person person = new Person();
		person.setAddressWithCascadeDelete(address);
		mongoOperations.save(person);

		// when
		mongoOperations.remove(person);

		// then
		assertEquals(0, mongoOperations.findAll(Person.class).size());
		assertEquals(0, mongoOperations.findAll(Address.class).size());
	}

	private Person createPersonWithCascadeDeleteAddress() {
		Person person = new Person();
		person.setAddressWithCascadeDelete(new Address());

		return person;
	}

	/**
	 * Tests {@link MongoOperations#save(Object)} with {@link org.springframework.data.mongodb.core.mapping.CascadeType#ALL}
	 */
	@Test
	public void testSavingWithCascadeAll() {
		// given
		Person person = createPersonWithCascadeAllAddress();

		// when
		mongoOperations.save(person);

		// then
		List<Address> addresses = mongoOperations.findAll(Address.class);
		List<Person> persons = mongoOperations.findAll(Person.class);

		assertEquals(1, addresses.size());
		assertEquals(1, persons.size());

		assertEquals(persons.get(0).getAddressWithCascadeAll(), addresses.get(0));
	}

	/**
	 * Tests {@link MongoOperations#remove(Object)} with {@link org.springframework.data.mongodb.core.mapping.CascadeType#ALL}
	 */
	@Test
	public void testDeletingWithCascadeAll() {
		// given
		Person person = createPersonWithCascadeAllAddress();
		mongoOperations.save(person);

		// when
		mongoOperations.remove(person);

		// then
		assertEquals(0, mongoOperations.findAll(Person.class).size());
		assertEquals(0, mongoOperations.findAll(Address.class).size());
	}

	private Person createPersonWithCascadeAllAddress() {
		Person person = new Person();
		person.setAddressWithCascadeAll(new Address());

		return person;
	}

	/**
	 * Tests {@link MongoOperations#save(Object)} with {@link org.springframework.data.mongodb.core.mapping.CascadeType#NONE}
	 */
	@Test
	public void testSavingWithCascadeNone() {
		// given
		Person person = createPersonWithCascadeNoneAddress();

		try {
			// when
			mongoOperations.save(person);
			fail();

			//then
		} catch (MappingException e) {
			assertEquals("Cannot create a reference to an object with a NULL id.", e.getMessage());
		}
	}

	/**
	 * Tests {@link MongoOperations#remove(Object)} with {@link org.springframework.data.mongodb.core.mapping.CascadeType#NONE}
	 */
	@Test
	public void testDeletingWithCascadeNone() {
		// given
		Address address = new Address();
		mongoOperations.save(address);

		Person person = new Person();
		person.setAddressWithCascadeNone(address);
		mongoOperations.save(person);

		// when
		mongoOperations.remove(person);

		// then
		assertEquals(0, mongoOperations.findAll(Person.class).size());
		assertEquals(1, mongoOperations.findAll(Address.class).size());
	}

	private Person createPersonWithCascadeNoneAddress() {
		Person person = new Person();
		person.setAddressWithCascadeNone(new Address());

		return person;
	}

	@Test
	public void testCascadeSaveOnCollections() {
		// given
		Person person = new Person();
		person.setAddresses(Arrays.asList(new Address(), new Address(), new Address()));

		// when
		mongoOperations.save(person);

		//then
		assertEquals(1, mongoOperations.findAll(Person.class).size());
		assertEquals(3, mongoOperations.findAll(Address.class).size());
	}

	@Test
	public void testCascadeDeleteOnCollections() {
		// given
		Person person = new Person();
		person.setAddresses(Arrays.asList(new Address(), new Address(), new Address()));
		mongoOperations.save(person);

		// when
		mongoOperations.remove(person);

		//then
		assertEquals(0, mongoOperations.findAll(Person.class).size());
		assertEquals(0, mongoOperations.findAll(Address.class).size());
	}
}

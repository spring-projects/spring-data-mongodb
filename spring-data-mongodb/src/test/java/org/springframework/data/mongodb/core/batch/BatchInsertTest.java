package org.springframework.data.mongodb.core.batch;

import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.Person;
import org.springframework.util.Assert;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
public class BatchInsertTest extends AbstractBatchConfiguration{

	@Autowired BatchInsertOperation<Person> batch;
	
	@Test
	public void testAddPerson(){
		
		Person person = new Person("Joao");
		
		batch.insert(person);
		
		Assert.notNull(batch);
	}
	
	@Test
	public void testAddOnlyOnePerson(){
		
		Person person = new Person("Joao");
		
		batch.insert(person);
		
		Assert.notNull(batch);
	}
	
	@Test
	public void testCollectionTwoPeople(){
		
		List<Person> people = populateCollection(2);
		
		batch.insert(people);
		
		Assert.notNull(batch);
	}
	
	@Test
	public void testCollectionFourPeople(){
		
		List<Person> people = populateCollection(4);
		
		batch.insert(people);
		
		Assert.notNull(batch);
	}
	
	@Test
	public void testCollectionFourtyPeople(){
		
		List<Person> people = populateCollection(40);
		
		batch.insert(people);
		
		Assert.notNull(batch);
	}
	
	@Test
	public void testCollectionFourHundredPeople(){
		
		List<Person> people = populateCollection(400);
		
		batch.insert(people);
		
		Assert.notNull(batch);
	}
}

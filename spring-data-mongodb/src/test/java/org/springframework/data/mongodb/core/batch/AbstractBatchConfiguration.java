package org.springframework.data.mongodb.core.batch;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractIntegrationTests;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.Person;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Guto Bortolozzo
 * see DATAMONGO-867
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractBatchConfiguration extends AbstractIntegrationTests {

	@Configuration
	static class Config {

		@Bean
		public BatchInsertOperations<Person> batchInsertOperation(){
			return new BatchInsert<Person>(1000);
		}
	}

	@Autowired BatchInsertOperations<Person> batch;
	@Autowired MongoOperations template;
	
	@Before
	public void setup(){
		batch.clear();
		super.cleanUp();
	}
	
	protected List<Person> populateCollection(int quantity){

		List<Person> people = new ArrayList<Person>();

		for(int index = 0; index < quantity; index++)
			people.add(new Person("Joao"));

		return people;
	}
}
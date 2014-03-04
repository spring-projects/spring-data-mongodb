package org.springframework.data.mongodb.core.batch;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.data.mongodb.core.query.Criteria.where;

import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.query.Query;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
public class BatchInsertTest extends AbstractBatchConfiguration{

	@Autowired BatchInsertOperations<Person> batch;
	
	@Test
	public void testAddPersonWithoutFillBatchSize(){
		
		Person person = new Person("Joao");
		
		batch.insert(person);
		
		assertSizeOfPersonCollection(0);
	}

	private void assertSizeOfPersonCollection(int size) {
		
		Long count = operations.count(new Query(where("firstName").is("Joao")), Person.class);
		
		assertThat(count.intValue(), is(size));
	}
	
	@Test
	public void testAddPersonWithoutFillingBatchSize(){
		
		List<Person> people = super.populateCollection(1000);
		
		batch.insert(people);
		
		assertSizeOfPersonCollection(1000);
	}
	
	
}

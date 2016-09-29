package org.springframework.data.mongodb.core.batch;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.data.mongodb.core.query.Criteria.where;

import java.util.List;

import org.junit.Test;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.query.Query;

/**
 * @author Guto Bortolozzo
 * see DATAMONGO-867
 */
public class BatchInsertTest extends AbstractBatchConfiguration{

	@Test
	public void testAddPersonWithoutFillBatchSize(){

		Person person = new Person("Joao");

		batch.insert(person);

		assertSizeOfPersonCollection(0);
		assertThat(batch.contentSize(), is(1));
	}

	private void assertSizeOfPersonCollection(int size) {

		Long count = template.count(new Query(where("firstName").is("Joao")), Person.class);

		assertThat(count.intValue(), is(size));
	}

	@Test
	public void testAddPersonFillingBatchSize(){

		List<Person> people = super.populateCollection(1000);

		batch.insert(people);

		assertSizeOfPersonCollection(1000);
	}
	
	@Test
	public void testAddPersonFillingBatchSizeTwoTimes(){
		
		batch.insert(super.populateCollection(1000));
		
		assertSizeOfPersonCollection(1000);
		
		batch.insert(super.populateCollection(1000));
		
		assertSizeOfPersonCollection(2000);
		
		assertThat(batch.contentSize(), is(0));
	}
}
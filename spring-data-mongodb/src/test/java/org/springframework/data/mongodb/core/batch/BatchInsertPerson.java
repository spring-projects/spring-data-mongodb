package org.springframework.data.mongodb.core.batch;

import org.springframework.data.mongodb.core.Person;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
public class BatchInsertPerson extends BatchInsert<Person> {

	protected BatchInsertPerson() {
		super(1000);
	}
	
	public void insert(Person person){
		super.insert(person);
	}
}

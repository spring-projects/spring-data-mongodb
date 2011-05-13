package org.springframework.data.document.mongodb.mapping;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.document.mongodb.mapping.SimpleMongoMappingContext.SimpleMongoPersistentEntity;
import org.springframework.data.mapping.model.PersistentProperty;

/**
 * 
 * @author Oliver Gierke
 */
public class SimpleMappingContextUnitTests {

	@Test
	public void testname() {
		SimpleMongoMappingContext context = new SimpleMongoMappingContext();
		SimpleMongoPersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getPersistentProperties().isEmpty(), is(false));

		PersistentProperty idProperty = entity.getIdProperty();
		assertThat(idProperty.getName(), is("id"));
	}

	static class Person {
		String id;
	}
}

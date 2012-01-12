package org.springframework.data.mongodb.core.mapping;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.SimpleMongoMappingContext;
import org.springframework.data.mongodb.core.mapping.SimpleMongoMappingContext.SimpleMongoPersistentEntity;

/**
 * Unit tests for {@link SimpleMongoMappingContext}.
 * 
 * @author Oliver Gierke
 */
public class SimpleMappingContextUnitTests {

	@Test
	public void returnsIdPropertyCorrectly() {

		SimpleMongoMappingContext context = new SimpleMongoMappingContext();
		SimpleMongoPersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		MongoPersistentProperty idProperty = entity.getIdProperty();
		assertThat(idProperty, is(notNullValue()));
		assertThat(idProperty.getName(), is("id"));
	}

	static class Person {
		String id;
	}
}

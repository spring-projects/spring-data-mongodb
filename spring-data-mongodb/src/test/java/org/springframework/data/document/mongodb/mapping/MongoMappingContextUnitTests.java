package org.springframework.data.document.mongodb.mapping;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

/**
 * Unit tests for {@link MongoMappingContext}.
 * 
 * @author Oliver Gierke
 */
public class MongoMappingContextUnitTests {

	@Test
	public void addsSelfReferencingPersistentEntityCorrectly() {
		MongoMappingContext context = new MongoMappingContext();
		context.setInitialEntitySet(Collections.singleton(SampleClass.class));
		context.afterPropertiesSet();
	}
	
	public class SampleClass {
		
		Map<String, SampleClass> children;
	}
}

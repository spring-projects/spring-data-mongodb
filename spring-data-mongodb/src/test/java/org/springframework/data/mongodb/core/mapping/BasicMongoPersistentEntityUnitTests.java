/*
 * Copyright 2011-2014 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapping;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicMongoPersistentEntity}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicMongoPersistentEntityUnitTests {

	@Mock ApplicationContext context;

	@Test
	public void subclassInheritsAtDocumentAnnotation() {

		BasicMongoPersistentEntity<Person> entity = new BasicMongoPersistentEntity<Person>(
				ClassTypeInformation.from(Person.class));
		assertThat(entity.getCollection(), is("contacts"));
	}

	@Test
	public void evaluatesSpELExpression() {

		MongoPersistentEntity<Company> entity = new BasicMongoPersistentEntity<Company>(
				ClassTypeInformation.from(Company.class));
		assertThat(entity.getCollection(), is("35"));
	}

	@Test
	public void collectionAllowsReferencingSpringBean() {

		CollectionProvider provider = new CollectionProvider();
		provider.collectionName = "reference";

		when(context.getBean("myBean")).thenReturn(provider);
		when(context.containsBean("myBean")).thenReturn(true);

		BasicMongoPersistentEntity<DynamicallyMapped> entity = new BasicMongoPersistentEntity<DynamicallyMapped>(
				ClassTypeInformation.from(DynamicallyMapped.class));
		entity.setApplicationContext(context);

		assertThat(entity.getCollection(), is("reference"));
	}

	/**
	 * @see DATAMONGO-937
	 */
	@Test
	public void shouldDetectLanguageCorrectly() {

		BasicMongoPersistentEntity<DocumentWithLanguage> entity = new BasicMongoPersistentEntity<DocumentWithLanguage>(
				ClassTypeInformation.from(DocumentWithLanguage.class));
		assertThat(entity.getLanguage(), is("spanish"));
	}

	@Document(collection = "contacts")
	class Contact {

	}

	class Person extends Contact {

	}

	@Document(collection = "#{35}")
	class Company {

	}

	@Document(collection = "#{myBean.collectionName}")
	class DynamicallyMapped {

	}

	class CollectionProvider {
		String collectionName;

		public String getCollectionName() {
			return collectionName;
		}
	}

	@Document(language = "spanish")
	static class DocumentWithLanguage {

	}
}

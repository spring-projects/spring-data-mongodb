/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Unit tests for {@link LowerCaseWithUnderscoresFieldNamingStrategy}.
 * 
 * @author Ryan Tenney
 */
@RunWith(MockitoJUnitRunner.class)
public class LowerCaseWithUnderscoresFieldNamingStrategyUnitTests {

	FieldNamingStrategy strategy = new LowerCaseWithUnderscoresFieldNamingStrategy();

	@Mock MongoPersistentProperty property;

	@Test
	public void foo() {

		assertFieldNameForPropertyName("fooBar", "foo_bar");
		assertFieldNameForPropertyName("FooBar", "foo_bar");
		assertFieldNameForPropertyName("foo_bar", "foo_bar");
		assertFieldNameForPropertyName("FOO_BAR", "foo_bar");
	}

	private void assertFieldNameForPropertyName(String propertyName, String fieldName) {

		when(property.getName()).thenReturn(propertyName);
		assertThat(strategy.getFieldName(property), is(fieldName));
	}
}

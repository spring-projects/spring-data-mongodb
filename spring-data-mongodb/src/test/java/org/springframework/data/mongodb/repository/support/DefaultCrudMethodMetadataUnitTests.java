/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.support.CrudMethodMetadataPostProcessor.DefaultCrudMethodMetadata;
import org.springframework.data.repository.CrudRepository;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link DefaultCrudMethodMetadata}.
 *
 * @author Christoph Strobl
 */
class DefaultCrudMethodMetadataUnitTests {

	@Test // GH-4542
	void detectsReadPreferenceOnRepositoryInterface() {

		DefaultCrudMethodMetadata metadata = new DefaultCrudMethodMetadata(ReadPreferenceAnnotated.class,
				ReflectionUtils.findMethod(ReadPreferenceAnnotated.class, "findAll"));

		assertThat(metadata.getReadPreference()).hasValue(com.mongodb.ReadPreference.primary());
	}

	@Test // GH-4542
	void favorsReadPreferenceOfAnnotatedMethod() {

		DefaultCrudMethodMetadata metadata = new DefaultCrudMethodMetadata(ReadPreferenceAnnotated.class,
				ReflectionUtils.findMethod(ReadPreferenceAnnotated.class, "findById", Object.class));

		assertThat(metadata.getReadPreference()).hasValue(com.mongodb.ReadPreference.secondary());
	}

	@ReadPreference("primary")
	interface ReadPreferenceAnnotated extends CrudRepository<Person, String> {

		@Override
		@ReadPreference("secondary")
		Optional<Person> findById(String s);
	}
}

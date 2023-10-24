/*
 * Copyright 2023. the original author or authors.
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

/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import java.lang.reflect.Method;
import java.util.Optional;

import com.mongodb.ReadPreference;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mongodb.core.annotation.Collation;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link CrudMethodMetadata} that will inspect the backing method for annotations.
 */
class DefaultCrudMethodMetadata implements CrudMethodMetadata {

	private final Optional<ReadPreference> readPreference;
	private final Optional<String> collation;

	/**
	 * Creates a new {@link DefaultCrudMethodMetadata} for the given {@link Method}.
	 *
	 * @param method must not be {@literal null}.
	 */
	DefaultCrudMethodMetadata(Method method) {

		Assert.notNull(method, "Method must not be null");

		this.readPreference = findReadPreference(method);
		this.collation = Optional.ofNullable(AnnotatedElementUtils.findMergedAnnotation(method, Collation.class)).map(Collation::value);
	}

	DefaultCrudMethodMetadata(Optional<ReadPreference> readPreference, Optional<String> collation) {
		this.readPreference = readPreference;
		this.collation = collation;
	}

	private Optional<ReadPreference> findReadPreference(Method method) {

		org.springframework.data.mongodb.repository.ReadPreference preference = AnnotatedElementUtils
				.findMergedAnnotation(method, org.springframework.data.mongodb.repository.ReadPreference.class);

		if (preference == null) {

			preference = AnnotatedElementUtils.findMergedAnnotation(method.getDeclaringClass(),
					org.springframework.data.mongodb.repository.ReadPreference.class);
		}

		if (preference == null) {
			return Optional.empty();
		}

		return Optional.of(ReadPreference.valueOf(preference.value()));
	}

	@Override
	public Optional<ReadPreference> getReadPreference() {
		return readPreference;
	}

	@Override
	public Optional<String> getCollation() {
		return collation;
	}

	@Override
	public CrudMethodMetadata capture() {
		return new DefaultCrudMethodMetadata(readPreference, collation);
	}

	@Override
	public void applyTo(MongoRepositoryAction repositoryAction) {

		if (repositoryAction instanceof QueryAction action) {
			readPreference.ifPresent(it -> action.getQuery().withReadPreference(it));
			collation.map(org.springframework.data.mongodb.core.query.Collation::of).ifPresent(it -> action.getQuery().collation(it));
		}
	}
}

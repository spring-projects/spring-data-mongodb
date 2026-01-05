/*
 * Copyright 2012-present the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;

import java.util.Set;

import org.bson.Document;

/**
 * JSR-303 dependant entities validator.
 * <p>
 * When it is registered as Spring component its automatically invoked after object to {@link Document} conversion and
 * before entities are saved to the database.
 *
 * @author Maciej Walkowiak
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @deprecated since 4.5, use {@link ValidatingEntityCallback} respectively {@link ReactiveValidatingEntityCallback}
 *             instead to ensure ordering and interruption of saving when encountering validation constraint violations.
 */
@Deprecated(since = "4.5")
public class ValidatingMongoEventListener extends AbstractMongoEventListener<Object> {

	private final BeanValidationDelegate delegate;

	/**
	 * Creates a new {@link ValidatingMongoEventListener} using the given {@link Validator}.
	 *
	 * @param validator must not be {@literal null}.
	 */
	public ValidatingMongoEventListener(Validator validator) {
		this.delegate = new BeanValidationDelegate(validator);
	}

	@Override
	public void onBeforeSave(BeforeSaveEvent<Object> event) {

		Set<ConstraintViolation<Object>> violations = delegate.validate(event.getSource());

		if (!violations.isEmpty()) {
			throw new ConstraintViolationException(violations);
		}
	}

}

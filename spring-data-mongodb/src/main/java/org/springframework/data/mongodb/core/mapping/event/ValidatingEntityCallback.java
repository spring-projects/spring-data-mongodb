/*
 * Copyright 2025-present the original author or authors.
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

import org.springframework.core.Ordered;

/**
 * JSR-303 dependant entities validator.
 * <p>
 * When it is registered as Spring component its automatically invoked after object to {@link Document} conversion and
 * before entities are saved to the database.
 *
 * @author Rene Felgenträger
 * @author Mark Paluch
 * @author HeeChul Yang
 * @since 4.5
 */
public class ValidatingEntityCallback implements BeforeSaveCallback<Object>, Ordered {

	private final BeanValidationDelegate delegate;
	private int order = 100;

	/**
	 * Creates a new {@link ValidatingEntityCallback} using the given {@link Validator}.
	 *
	 * @param validator must not be {@literal null}.
	 */
	public ValidatingEntityCallback(Validator validator) {
		this.delegate = new BeanValidationDelegate(validator);
	}

	@Override
	public int getOrder() {
		return this.order;
	}

	/**
	 * Specify the order value for this {@link BeforeConvertCallback}.
	 * <p>
	 * The default value is {@code 100}.
	 *
	 * @see org.springframework.core.Ordered#getOrder()
	 * @since 5.0
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public Object onBeforeSave(Object entity, Document document, String collection) {

		Set<ConstraintViolation<Object>> violations = delegate.validate(entity);

		if (!violations.isEmpty()) {
			throw new ConstraintViolationException(violations);
		}

		return entity;
	}
}

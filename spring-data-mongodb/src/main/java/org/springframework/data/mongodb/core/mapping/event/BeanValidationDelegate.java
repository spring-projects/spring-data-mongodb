/*
 * Copyright 2025 the original author or authors.
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
import jakarta.validation.Validator;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.util.Assert;

/**
 * Delegate to handle common calls to Bean {@link Validator Validation}.
 *
 * @author Mark Paluch
 * @since 4.5
 */
class BeanValidationDelegate {

	private static final Log LOG = LogFactory.getLog(BeanValidationDelegate.class);

	private final Validator validator;

	/**
	 * Creates a new {@link BeanValidationDelegate} using the given {@link Validator}.
	 *
	 * @param validator must not be {@literal null}.
	 */
	public BeanValidationDelegate(Validator validator) {
		Assert.notNull(validator, "Validator must not be null");
		this.validator = validator;
	}

	/**
	 * Validate the given object.
	 *
	 * @param object
	 * @return set of constraint violations.
	 */
	public Set<ConstraintViolation<Object>> validate(Object object) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Validating object: %s", object));
		}

		Set<ConstraintViolation<Object>> violations = validator.validate(object);

		if (!violations.isEmpty()) {
			if (LOG.isDebugEnabled()) {
				LOG.info(String.format("During object: %s validation violations found: %s", object, violations));
			}
		}

		return violations;
	}
}

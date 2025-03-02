/*
 * Copyright 2012-2025 the original author or authors.
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.springframework.core.Ordered;
import org.springframework.util.Assert;

/**
 * JSR-303 dependant entities validator.
 * <p>
 * When it is registered as Spring component its automatically invoked
 * after any {@link AbstractMongoEventListener} and before entities are saved in database.
 *
 * @author original authors of {@link ValidatingMongoEventListener}
 * @author Rene Felgenträger
 *
 * @see {@link ValidatingMongoEventListener}
 */
public class ValidatingEntityCallback implements BeforeSaveCallback<Object>, Ordered {

	private static final Log LOG = LogFactory.getLog(ValidatingEntityCallback.class);

	// TODO: create a validation handler (similar to "AuditingHandler") an reference it from "ValidatingMongoEventListener" and "ValidatingMongoEventListener"
	private final Validator validator;

	/**
	 * Creates a new {@link ValidatingEntityCallback} using the given {@link Validator}.
	 *
	 * @param validator must not be {@literal null}.
	 */
	public ValidatingEntityCallback(Validator validator) {
		Assert.notNull(validator, "Validator must not be null");
		this.validator = validator;
	}

	// TODO: alternatively implement the "BeforeConvertCallback" interface and set the order to highest value ?
	@Override
	public Object onBeforeSave(Object entity, Document document, String collection) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Validating object: %s", entity));
		}
		Set<ConstraintViolation<Object>> violations = validator.validate(entity);

		if (!violations.isEmpty()) {
			if (LOG.isDebugEnabled()) {
				LOG.info(String.format("During object: %s validation violations found: %s", entity, violations));
			}
			throw new ConstraintViolationException(violations);
		}
		return entity;
	}

	@Override
	public int getOrder() {
		return 100;
	}
}

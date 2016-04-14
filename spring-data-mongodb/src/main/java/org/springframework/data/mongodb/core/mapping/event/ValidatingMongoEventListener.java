/*
 * Copyright 2012-2016 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import java.util.Set;

import javax.validation.ConstraintViolationException;
import javax.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * javax.validation dependant entities validator. When it is registered as Spring component its automatically invoked
 * before entities are saved in database.
 * 
 * @author Maciej Walkowiak
 * @author Oliver Gierke
 */
public class ValidatingMongoEventListener extends AbstractMongoEventListener<Object> {

	private static final Logger LOG = LoggerFactory.getLogger(ValidatingMongoEventListener.class);

	private final Validator validator;

	/**
	 * Creates a new {@link ValidatingMongoEventListener} using the given {@link Validator}.
	 * 
	 * @param validator must not be {@literal null}.
	 */
	public ValidatingMongoEventListener(Validator validator) {

		Assert.notNull(validator, "Validator must not be null!");
		this.validator = validator;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener#onBeforeSave(org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent)
	 */
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void onBeforeSave(BeforeSaveEvent<Object> event) {

		Object source = event.getSource();

		LOG.debug("Validating object: {}", source);
		Set violations = validator.validate(source);

		if (!violations.isEmpty()) {

			LOG.info("During object: {} validation violations found: {}", source, violations);
			throw new ConstraintViolationException(violations);
		}
	}
}

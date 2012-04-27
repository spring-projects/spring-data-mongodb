/*
 * Copyright 2010 the original author or authors.
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

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import java.util.Set;

/**
 * javax.validation dependant entities validator.
 * When it is registered as Spring component its automatically invoked before entities are saved in database.
 *
 * @author Maciej Walkowiak <walkowiak.maciej@yahoo.com>
 */
public class ValidatingMongoEventListener extends AbstractMongoEventListener {
	private static final Logger LOG = LoggerFactory.getLogger(ValidatingMongoEventListener.class);

	private final Validator validator;

	public ValidatingMongoEventListener(Validator validator) {
		this.validator = validator;
	}

	@Override
	public void onBeforeSave(Object source, DBObject dbo) {
		LOG.debug("Validating object: {}", source);

		Set violations = validator.validate(source);

		if (violations.size() > 0) {
			LOG.info("During object: {} validation violations found: {}", source, violations);

			throw new ConstraintViolationException((Set<ConstraintViolation<?>>) violations);
		}
	}
}

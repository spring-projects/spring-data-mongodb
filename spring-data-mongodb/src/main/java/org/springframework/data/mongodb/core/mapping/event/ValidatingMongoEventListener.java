package org.springframework.data.mongodb.core.mapping.event;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

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
public class ValidatingMongoEventListener extends AbstractMongoEventListener implements InitializingBean {
	private static final Logger LOG = LoggerFactory.getLogger(ValidatingMongoEventListener.class);

	private Validator validator;

	public void afterPropertiesSet() throws Exception {
		Assert.notNull(validator, "Validator is not set");
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

	public void setValidator(Validator validator) {
		this.validator = validator;
	}
}

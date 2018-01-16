/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core.validation;

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link CriteriaValidator}.
 *
 * @author Andreas Zink
 * @author Christoph Strobl
 */
public class CriteriaValidatorUnitTests {

	@Test // DATAMONGO-1322
	public void testSimpleCriteria() {

		Criteria criteria = Criteria.where("nonNullString").ne(null).type(2).and("rangedInteger").type(16).gte(0).lte(122);
		Document validator = CriteriaValidator.of(criteria).toDocument();

		assertThat(validator.get("nonNullString")).isEqualTo(new Document("$ne", null).append("$type", 2));
		assertThat(validator.get("rangedInteger"))
				.isEqualTo(new Document("$type", 16).append("$gte", 0).append("$lte", 122));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1322
	public void testFailOnNull() {
		CriteriaValidator.of(null);
	}
}

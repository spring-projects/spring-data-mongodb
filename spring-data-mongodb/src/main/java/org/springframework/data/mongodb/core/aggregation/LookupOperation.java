/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $lookup}-operation.
 * We recommend to use the static factory method {@link Aggregation#lookup(String, String, String, String)} instead of
 * creating instances of this class directly.
 *
 * @author Alessio Fachechi
 * @see http://docs.mongodb.org/manual/reference/aggregation/lookup/#stage._S_lookup
 * @since 1.9
 */
public class LookupOperation implements AdditionalFieldsExposingAggregationOperation {

	private ExposedField from;
	private ExposedField localField;
	private ExposedField foreignField;
	private ExposedField as;

	/**
	 * Creates a new {@link LookupOperation} for the given {@link Field}s.
	 *
	 * @param from must not be {@literal null}.
	 * @param localField must not be {@literal null}.
	 * @param foreignField must not be {@literal null}.
	 * @param as must not be {@literal null}.
	 */
	public LookupOperation(Field from, Field localField, Field foreignField, Field as) {
		Assert.notNull(from, "From must not be null!");
		Assert.notNull(localField, "LocalField must not be null!");
		Assert.notNull(foreignField, "ForeignField must not be null!");
		Assert.notNull(as, "As must not be null!");

		this.from = new ExposedField(from, true);
		this.localField = new ExposedField(localField, true);
		this.foreignField = new ExposedField(foreignField, true);
		this.as = new ExposedField(as, true);
	}

	@Override
	public ExposedFields getFields() {
		return ExposedFields.from(as);
	}

	@Override
	public DBObject toDBObject(AggregationOperationContext context) {
		BasicDBObject lookupObject = new BasicDBObject();

		lookupObject.append("from", from.getTarget());
		lookupObject.append("localField", localField.getTarget());
		lookupObject.append("foreignField", foreignField.getTarget());
		lookupObject.append("as", as.getTarget());

		return new BasicDBObject("$lookup", lookupObject);
	}
}

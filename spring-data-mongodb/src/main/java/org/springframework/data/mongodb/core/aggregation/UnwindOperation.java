/*
 * Copyright 2013 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * Encapsulates the aggregation framework {@code $unwind}-operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/unwind/#pipe._S_unwind
 * @author Thomas Darimont
 */
public class UnwindOperation extends AbstractContextProducingAggregateOperation implements
		ContextConsumingAggregateOperation {

	private final String fieldName;

	public UnwindOperation(String fieldName) {

		super("unwind");
		Assert.notNull(fieldName);
		this.fieldName = fieldName;

		getOutputAggregateOperationContext().registerAvailableField(fieldName, fieldName);
	}

	@Override
	public Object getOperationArgument(AggregateOperationContext inputAggregateOperationContext) {
		return ReferenceUtil.safeReference(fieldName);
	}
}

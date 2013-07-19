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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Thomas Darimont
 */
abstract class AbstractContextAwareAggregateOperation extends AbstractAggregateOperation implements
		ContextConsumingAggregateOperation {

	public AbstractContextAwareAggregateOperation(String operationName) {
		super(operationName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AbstractAggregateOperation#getOperationArgument()
	 */
	@Override
	public Object getOperationArgument() {
		throw new UnsupportedOperationException(String.format("This is not supported on an instance of %s",
				ContextConsumingAggregateOperation.class.getName()));
	}

	/**
	 * Creates the argument for the aggregation operation from the given {@code inputAggregateOperationContext}
	 * 
	 * @param inputAggregateOperationContext
	 * @return the argument for the operation
	 */
	public abstract Object getOperationArgument(AggregateOperationContext inputAggregateOperationContext);

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AbstractAggregateOperation#toDbObject()
	 */
	@Override
	public DBObject toDbObject() {
		throw new UnsupportedOperationException(String.format("This is not supported on an instance of %s",
				ContextConsumingAggregateOperation.class.getName()));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.ContextAwareAggregateOperation#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregateOperationContext)
	 */
	public DBObject toDbObject(AggregateOperationContext inputAggregateOperationContext) {
		return new BasicDBObject(getOperationCommand(), getOperationArgument(inputAggregateOperationContext));
	}
}

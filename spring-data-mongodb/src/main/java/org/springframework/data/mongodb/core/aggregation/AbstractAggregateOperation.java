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
abstract class AbstractAggregateOperation implements AggregationOperation {
	private final String operationName;

	/**
	 * @param operationName
	 */
	public AbstractAggregateOperation(String operationName) {
		this.operationName = operationName;
	}

	public String getOperationName() {
		return operationName;
	}

	public String getOperationCommand() {
		return OPERATOR_PREFIX + getOperationName();
	}

	public Object getOperationArgument() {
		return new BasicDBObject();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDbObject()
	 */
	@Override
	public DBObject toDbObject() {
		return new BasicDBObject(getOperationCommand(), getOperationArgument());
	}

	@Override
	public String toString() {
		return String.valueOf(toDbObject());
	}
}

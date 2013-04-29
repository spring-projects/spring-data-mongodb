/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.domain.Sort.Order;

import com.mongodb.DBCursor;

/**
 * Collection of utility methods to apply sorting and pagination to a {@link DBCursor}.
 * 
 * @author Oliver Gierke
 */
@Deprecated
public abstract class QueryUtils {

	private QueryUtils() {

	}

	/**
	 * Turns an {@link Order} into an {@link org.springframework.data.mongodb.core.query.Order}.
	 * 
	 * @deprecated use {@link Order} directly.
	 * @param order
	 * @return
	 */
	@Deprecated
	public static org.springframework.data.mongodb.core.query.Order toOrder(Order order) {
		return order.isAscending() ? org.springframework.data.mongodb.core.query.Order.ASCENDING
				: org.springframework.data.mongodb.core.query.Order.DESCENDING;
	}
}

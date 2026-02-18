/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.bulk;

/**
 * Options for executing a {@link Bulk} write, such as whether operations run in {@link Order#ORDERED ordered} or
 * {@link Order#UNORDERED unordered} mode.
 * 
 * @author Christoph Strobl
 * @since 5.1
 */
public class BulkWriteOptions {

	private final Order order;

	BulkWriteOptions(Order order) {
		this.order = order;
	}

	/**
	 * Returns options for ordered execution: operations run serially and execution stops on the first error.
	 *
	 * @return options for ordered bulk write; never {@literal null}.
	 */
	public static BulkWriteOptions ordered() {
		return new BulkWriteOptions(Order.ORDERED);
	}

	/**
	 * Returns options for unordered execution: operations may run in any order (possibly in parallel) and execution
	 * continues even if some operations fail.
	 *
	 * @return options for unordered bulk write; never {@literal null}.
	 */
	public static BulkWriteOptions unordered() {
		return new BulkWriteOptions(Order.UNORDERED);
	}

	/**
	 * Returns the execution order for the bulk write.
	 *
	 * @return the {@link Order}; never {@literal null}.
	 */
	public Order getOrder() {
		return order;
	}

	/**
	 * Execution order for bulk write operations.
	 */
	public enum Order {

		/**
		 * Execute {@link BulkOperation operations} in the order of {@link Bulk#operations()}; stop on first error.
		 */
		ORDERED,

		/**
		 * Execute {@link BulkOperation operations} in no particular order; continue despite errors.
		 */
		UNORDERED
	}
}

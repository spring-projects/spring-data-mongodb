/*
 * Copyright 2013-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

/**
 * {@link AggregationOperation} that exposes {@link ExposedFields} that can be used for later aggregation pipeline
 * {@code AggregationOperation}s. A {@link FieldsExposingAggregationOperation} implementing the
 * {@link InheritsFieldsAggregationOperation} will expose fields from its parent operations. Not implementing
 * {@link InheritsFieldsAggregationOperation} will replace existing exposed fields.
 *
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public interface FieldsExposingAggregationOperation extends AggregationOperation {

	/**
	 * Returns the fields exposed by the {@link AggregationOperation}.
	 *
	 * @return will never be {@literal null}.
	 */
	ExposedFields getFields();

	/**
	 * @return {@literal true} to conditionally inherit fields from previous operations.
	 * @since 2.0.6
	 */
	default boolean inheritsFields() {
		return false;
	}

	/**
	 * Marker interface for {@link AggregationOperation} that inherits fields from previous operations.
	 */
	interface InheritsFieldsAggregationOperation extends FieldsExposingAggregationOperation {

		@Override
		default boolean inheritsFields() {
			return true;
		}
	}
}

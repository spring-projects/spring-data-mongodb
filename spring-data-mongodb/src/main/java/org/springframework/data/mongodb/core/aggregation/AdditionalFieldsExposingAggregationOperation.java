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

/**
 * {@link AggregationOperation} that exposes <b>additional</b> {@link ExposedFields} that can be used for later
 * aggregation pipeline {@code AggregationOperation}s, e.g. lookup operation produces a field which has to be added to
 * the current ones.
 *
 * @author Alessio Fachechi
 */
public interface AdditionalFieldsExposingAggregationOperation extends FieldsExposingAggregationOperation {

}

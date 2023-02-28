/*
 * Copyright 2023 the original author or authors.
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

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * An {@link AggregationStage} that may consist of a main operation and potential follow up stages for eg. {@code $sort}
 * or {@code $limit}.
 * <p>
 * The {@link MultiOperationAggregationStage} may operate upon domain specific types but will render to the store native
 * representation within a given {@link AggregationOperationContext context}.
 * <p>
 * {@link #toDocument(AggregationOperationContext)} will render a synthetic {@link Document} that contains the ordered
 * stages. The list returned from {@link #toPipelineStages(AggregationOperationContext)}
 * 
 * <pre class="code">
 * [
 *   { $match: { $text: { $search: "operating" } } },
 *   { $sort: { score: { $meta: "textScore" }, posts: -1 } }
 * ]
 * </pre>
 * 
 * will be represented as
 * 
 * <pre class="code">
 * {
 *   $match: { $text: { $search: "operating" } },
 *   $sort: { score: { $meta: "textScore" }, posts: -1 }
 * }
 * </pre>
 * 
 * In case stages appear multiple times the order no longer can be guaranteed when calling
 * {@link #toDocument(AggregationOperationContext)}, so consumers of the API should rely on
 * {@link #toPipelineStages(AggregationOperationContext)}. Nevertheless, by default the values will be collected into a
 * list rendering to
 * 
 * <pre class="code">
 * {
 *   $match: [{ $text: { $search: "operating" } }, { $text: ... }],
 *   $sort: { score: { $meta: "textScore" }, posts: -1 }
 * }
 * </pre>
 * 
 * @author Christoph Strobl
 * @since 4.1
 */
public interface MultiOperationAggregationStage extends AggregationStage {

	/**
	 * Returns a synthetic {@link Document stage} that contains the {@link #toPipelineStages(AggregationOperationContext)
	 * actual stages} by folding them into a single {@link Document}. In case of colliding entries, those used multiple
	 * times thus having the same key, the entries will be held as a list for the given operator.
	 *
	 * @param context the {@link AggregationOperationContext} to operate within. Must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	@Override
	default Document toDocument(AggregationOperationContext context) {

		List<Document> documents = toPipelineStages(context);
		if (documents.size() == 1) {
			return documents.get(0);
		}

		MultiValueMap<String, Document> stages = new LinkedMultiValueMap<>(documents.size());
		for (Document current : documents) {
			String key = current.keySet().iterator().next();
			stages.add(key, current.get(key, Document.class));
		}
		return new Document(stages.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, v -> v.getValue().size() == 1 ? v.getValue().get(0) : v.getValue())));
	}

	/**
	 * Turns the {@link MultiOperationAggregationStage} into list of {@link Document stages} by using the given
	 * {@link AggregationOperationContext}. This allows an {@link AggregationStage} to add follow up stages for eg.
	 * {@code $sort} or {@code $limit}.
	 *
	 * @param context the {@link AggregationOperationContext} to operate within. Must not be {@literal null}.
	 * @return the pipeline stages to run through. Never {@literal null}.
	 */
	List<Document> toPipelineStages(AggregationOperationContext context);
}

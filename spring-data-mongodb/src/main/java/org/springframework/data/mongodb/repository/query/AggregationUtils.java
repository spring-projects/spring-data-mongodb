/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Internal utility class to help avoid duplicate code required in both the reactive and the sync {@link Aggregation}
 * support offered by repositories.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.2
 */
abstract class AggregationUtils {

	private static final ParameterBindingDocumentCodec CODEC = new ParameterBindingDocumentCodec();

	private AggregationUtils() {
	}

	/**
	 * Apply a collation extracted from the given {@literal collationExpression} to the given
	 * {@link org.springframework.data.mongodb.core.aggregation.AggregationOptions.Builder}. Potentially replace parameter
	 * placeholders with values from the {@link ConvertingParameterAccessor accessor}.
	 *
	 * @param builder must not be {@literal null}.
	 * @param collationExpression must not be {@literal null}.
	 * @param accessor must not be {@literal null}.
	 * @return the {@link Query} having proper {@link Collation}.
	 * @see AggregationOptions#getCollation()
	 * @see CollationUtils#computeCollation(String, ConvertingParameterAccessor, MongoParameters, ExpressionParser,
	 *      QueryMethodEvaluationContextProvider)
	 */
	static AggregationOptions.Builder applyCollation(AggregationOptions.Builder builder,
			@Nullable String collationExpression, ConvertingParameterAccessor accessor, MongoParameters parameters,
			ExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		Collation collation = CollationUtils.computeCollation(collationExpression, accessor, parameters, expressionParser,
				evaluationContextProvider);
		return collation == null ? builder : builder.collation(collation);
	}

	/**
	 * Apply {@link Meta#getComment()} and {@link Meta#getCursorBatchSize()}.
	 *
	 * @param builder must not be {@literal null}.
	 * @param queryMethod must not be {@literal null}.
	 */
	static AggregationOptions.Builder applyMeta(AggregationOptions.Builder builder, MongoQueryMethod queryMethod) {

		Meta meta = queryMethod.getQueryMetaAttributes();

		if (StringUtils.hasText(meta.getComment())) {
			builder.comment(meta.getComment());
		}

		if (meta.getCursorBatchSize() != null) {
			builder.cursorBatchSize(meta.getCursorBatchSize());
		}

		if (meta.getMaxTimeMsec() != null && meta.getMaxTimeMsec() > 0) {
			builder.maxTime(Duration.ofMillis(meta.getMaxTimeMsec()));
		}

		if (meta.getAllowDiskUse() != null) {
			builder.allowDiskUse(meta.getAllowDiskUse());
		}

		return builder;
	}

	/**
	 * Compute the {@link AggregationOperation aggregation} pipeline for the given {@link MongoQueryMethod}. The raw
	 * {@link org.springframework.data.mongodb.repository.Aggregation#pipeline()} is parsed with a
	 * {@link ParameterBindingDocumentCodec} to obtain the MongoDB native {@link Document} representation returned by
	 * {@link AggregationOperation#toDocument(AggregationOperationContext)} that is mapped against the domain type
	 * properties.
	 *
	 * @param method
	 * @param accessor
	 * @param expressionParser
	 * @param evaluationContextProvider
	 * @return
	 */
	static List<AggregationOperation> computePipeline(MongoQueryMethod method, ConvertingParameterAccessor accessor,
			ExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		ParameterBindingContext bindingContext = new ParameterBindingContext((accessor::getBindableValue), expressionParser,
				() -> evaluationContextProvider.getEvaluationContext(method.getParameters(), accessor.getValues()));

		List<AggregationOperation> target = new ArrayList<>(method.getAnnotatedAggregation().length);
		for (String source : method.getAnnotatedAggregation()) {
			target.add(ctx -> ctx.getMappedObject(CODEC.decode(source, bindingContext), method.getDomainClass()));
		}
		return target;
	}

	/**
	 * Append {@code $sort} aggregation stage if {@link ConvertingParameterAccessor#getSort()} is present.
	 *
	 * @param aggregationPipeline
	 * @param accessor
	 * @param targetType
	 */
	static void appendSortIfPresent(List<AggregationOperation> aggregationPipeline, ConvertingParameterAccessor accessor,
			Class<?> targetType) {

		if (accessor.getSort().isUnsorted()) {
			return;
		}

		aggregationPipeline.add(ctx -> {

			Document sort = new Document();
			for (Order order : accessor.getSort()) {
				sort.append(order.getProperty(), order.isAscending() ? 1 : -1);
			}

			return ctx.getMappedObject(new Document("$sort", sort), targetType);
		});
	}

	/**
	 * Append {@code $skip} and {@code $limit} aggregation stage if {@link ConvertingParameterAccessor#getSort()} is
	 * present.
	 *
	 * @param aggregationPipeline
	 * @param accessor
	 */
	static void appendLimitAndOffsetIfPresent(List<AggregationOperation> aggregationPipeline,
			ConvertingParameterAccessor accessor) {

		Pageable pageable = accessor.getPageable();
		if (pageable.isUnpaged()) {
			return;
		}

		if (pageable.getOffset() > 0) {
			aggregationPipeline.add(Aggregation.skip(pageable.getOffset()));
		}

		aggregationPipeline.add(Aggregation.limit(pageable.getPageSize()));
	}

	/**
	 * Extract a single entry from the given {@link Document}. <br />
	 * <ol>
	 * <li><strong>empty source:</strong> {@literal null}</li>
	 * <li><strong>single entry</strong> convert that one</li>
	 * <li><strong>single entry when ignoring {@literal _id} field</strong> convert that one</li>
	 * <li><strong>multiple entries</strong> first value assignable to the target type</li>
	 * <li><strong>no match</strong> IllegalArgumentException</li>
	 * </ol>
	 *
	 * @param <T>
	 * @param source
	 * @param targetType
	 * @param converter
	 * @return can be {@literal null} if source {@link Document#isEmpty() is empty}.
	 * @throws IllegalArgumentException when none of the above rules is met.
	 */
	@Nullable
	static <T> T extractSimpleTypeResult(Document source, Class<T> targetType, MongoConverter converter) {

		if (source.isEmpty()) {
			return null;
		}

		if (source.size() == 1) {
			return getPotentiallyConvertedSimpleTypeValue(converter, source.values().iterator().next(), targetType);
		}

		Document intermediate = new Document(source);
		intermediate.remove("_id");

		if (intermediate.size() == 1) {
			return getPotentiallyConvertedSimpleTypeValue(converter, intermediate.values().iterator().next(), targetType);
		}

		for (Map.Entry<String, Object> entry : intermediate.entrySet()) {
			if (entry != null && ClassUtils.isAssignable(targetType, entry.getValue().getClass())) {
				return targetType.cast(entry.getValue());
			}
		}

		throw new IllegalArgumentException(
				String.format("o_O no entry of type %s found in %s.", targetType.getSimpleName(), source.toJson()));
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private static <T> T getPotentiallyConvertedSimpleTypeValue(MongoConverter converter, @Nullable Object value,
			Class<T> targetType) {

		if (value == null) {
			return null;
		}

		if (ClassUtils.isAssignableValue(targetType, value)) {
			return (T) value;
		}

		return converter.getConversionService().convert(value, targetType);
	}
}

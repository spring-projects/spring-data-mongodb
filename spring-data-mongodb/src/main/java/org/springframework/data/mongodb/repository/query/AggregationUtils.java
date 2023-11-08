/*
 * Copyright 2019-2023 the original author or authors.
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
import java.util.Map;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;

import org.bson.Document;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.ExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.mongodb.ReadPreference;

/**
 * Internal utility class to help avoid duplicate code required in both the reactive and the sync {@link Aggregation}
 * support offered by repositories.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Divya Srivastava
 * @since 2.2
 */
abstract class AggregationUtils {

	private AggregationUtils() {}

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

		if (meta.hasComment()) {
			builder.comment(meta.getComment());
		}

		if (meta.getCursorBatchSize() != null) {
			builder.cursorBatchSize(meta.getCursorBatchSize());
		}

		if (meta.hasMaxTime()) {
			builder.maxTime(Duration.ofMillis(meta.getRequiredMaxTimeMsec()));
		}

		if (meta.getAllowDiskUse() != null) {
			builder.allowDiskUse(meta.getAllowDiskUse());
		}

		return builder;
	}

	/**
	 * If present apply the hint from the {@link org.springframework.data.mongodb.repository.Hint} annotation.
	 *
	 * @param builder must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 4.1
	 */
	static AggregationOptions.Builder applyHint(AggregationOptions.Builder builder, MongoQueryMethod queryMethod) {

		if (!queryMethod.hasAnnotatedHint()) {
			return builder;
		}

		return builder.hint(queryMethod.getAnnotatedHint());
	}

	/**
	 * If present apply the preference from the {@link org.springframework.data.mongodb.repository.ReadPreference} annotation.
	 *
	 * @param builder must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 4.2
	 */
	static AggregationOptions.Builder applyReadPreference(AggregationOptions.Builder builder, MongoQueryMethod queryMethod) {

		if (!queryMethod.hasAnnotatedReadPreference()) {
			return builder;
		}

		return builder.readPreference(ReadPreference.valueOf(queryMethod.getAnnotatedReadPreference()));
	}

	/**
	 * Append {@code $sort} aggregation stage if {@link ConvertingParameterAccessor#getSort()} is present.
	 *
	 * @param aggregationPipeline
	 * @param accessor
	 * @param targetType
	 */
	static void appendSortIfPresent(AggregationPipeline aggregationPipeline, ConvertingParameterAccessor accessor,
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
	static void appendLimitAndOffsetIfPresent(AggregationPipeline aggregationPipeline,
			ConvertingParameterAccessor accessor) {
		appendLimitAndOffsetIfPresent(aggregationPipeline, accessor, LongUnaryOperator.identity(),
				IntUnaryOperator.identity());
	}

	/**
	 * Append {@code $skip} and {@code $limit} aggregation stage if {@link ConvertingParameterAccessor#getSort()} is
	 * present.
	 *
	 * @param aggregationPipeline
	 * @param accessor
	 * @param offsetOperator
	 * @param limitOperator
	 * @since 3.3
	 */
	static void appendLimitAndOffsetIfPresent(AggregationPipeline aggregationPipeline,
			ConvertingParameterAccessor accessor, LongUnaryOperator offsetOperator, IntUnaryOperator limitOperator) {

		Pageable pageable = accessor.getPageable();
		if (pageable.isUnpaged()) {
			return;
		}

		if (pageable.getOffset() > 0) {
			aggregationPipeline.add(Aggregation.skip(offsetOperator.applyAsLong(pageable.getOffset())));
		}

		aggregationPipeline.add(Aggregation.limit(limitOperator.applyAsInt(pageable.getPageSize())));
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
	static <T> T extractSimpleTypeResult(@Nullable Document source, Class<T> targetType, MongoConverter converter) {

		if (ObjectUtils.isEmpty(source)) {
			return null;
		}

		if (source.size() == 1) {
			return getPotentiallyConvertedSimpleTypeValue(converter, source.values().iterator().next(), targetType);
		}

		Document intermediate = new Document(source);
		intermediate.remove(FieldName.ID.name());

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

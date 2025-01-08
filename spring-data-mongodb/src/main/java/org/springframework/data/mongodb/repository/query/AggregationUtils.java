/*
 * Copyright 2019-2025 the original author or authors.
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
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;

import org.bson.Document;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.data.util.TypeInformation;
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
	 */
	static AggregationOptions.Builder applyCollation(AggregationOptions.Builder builder,
			@Nullable String collationExpression, ConvertingParameterAccessor accessor, ValueExpressionEvaluator evaluator) {

		Collation collation = CollationUtils.computeCollation(collationExpression, accessor, evaluator);
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
	 * If present apply the preference from the {@link org.springframework.data.mongodb.repository.ReadPreference}
	 * annotation.
	 *
	 * @param builder must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 4.2
	 */
	static AggregationOptions.Builder applyReadPreference(AggregationOptions.Builder builder,
			MongoQueryMethod queryMethod) {

		if (!queryMethod.hasAnnotatedReadPreference()) {
			return builder;
		}

		return builder.readPreference(ReadPreference.valueOf(queryMethod.getAnnotatedReadPreference()));
	}

	static AggregationOptions computeOptions(MongoQueryMethod method, ConvertingParameterAccessor accessor,
			AggregationPipeline pipeline, ValueExpressionEvaluator evaluator) {

		AggregationOptions.Builder builder = Aggregation.newAggregationOptions();

		AggregationUtils.applyCollation(builder, method.getAnnotatedCollation(), accessor, evaluator);
		AggregationUtils.applyMeta(builder, method);
		AggregationUtils.applyHint(builder, method);
		AggregationUtils.applyReadPreference(builder, method);

		TypeInformation<?> returnType = method.getReturnType();
		if (returnType.getComponentType() != null) {
			returnType = returnType.getRequiredComponentType();
		}
		if (ReflectionUtils.isVoid(returnType.getType()) && pipeline.isOutOrMerge()) {
			builder.skipOutput();
		}

		return builder.build();
	}

	/**
	 * Prepares the AggregationPipeline including type discovery and calling {@link AggregationCallback} to run the
	 * aggregation.
	 */
	@Nullable
	static <T> T doAggregate(AggregationPipeline pipeline, MongoQueryMethod method, ResultProcessor processor,
			ConvertingParameterAccessor accessor,
			Function<MongoParameterAccessor, ValueExpressionEvaluator> evaluatorFunction, AggregationCallback<T> callback) {

		Class<?> sourceType = method.getDomainClass();
		ReturnedType returnedType = processor.getReturnedType();
		// 🙈Interface Projections do not happen on the Aggregation level but through our repository infrastructure.
		// Non-projections and raw results (AggregationResults<…>) are handled here. Interface projections read a Document
		// and DTO projections read the returned type.
		// We also support simple return types (String) that are read from a Document
		TypeInformation<?> returnType = method.getReturnType();
		Class<?> returnElementType = (returnType.getComponentType() != null ? returnType.getRequiredComponentType()
				: returnType).getType();
		Class<?> entityType;

		boolean isRawAggregationResult = ClassUtils.isAssignable(AggregationResults.class, method.getReturnedObjectType());

		if (returnElementType.equals(Document.class)) {
			entityType = sourceType;
		} else {
			entityType = returnElementType;
		}

		AggregationUtils.appendSortIfPresent(pipeline, accessor, entityType);

		if (method.isSliceQuery()) {
			AggregationUtils.appendLimitAndOffsetIfPresent(pipeline, accessor, LongUnaryOperator.identity(),
					limit -> limit + 1);
		} else {
			AggregationUtils.appendLimitAndOffsetIfPresent(pipeline, accessor);
		}

		AggregationOptions options = AggregationUtils.computeOptions(method, accessor, pipeline,
				evaluatorFunction.apply(accessor));
		TypedAggregation<?> aggregation = new TypedAggregation<>(sourceType, pipeline.getOperations(), options);

		boolean isSimpleReturnType = MongoSimpleTypes.HOLDER.isSimpleType(returnElementType);
		Class<?> typeToRead;

		if (isSimpleReturnType) {
			typeToRead = Document.class;
		} else if (isRawAggregationResult) {
			typeToRead = returnElementType;
		} else {

			if (returnedType.isProjecting()) {
				typeToRead = returnedType.getReturnedType().isInterface() ? Document.class : returnedType.getReturnedType();
			} else {
				typeToRead = entityType;
			}
		}

		return callback.doAggregate(aggregation, sourceType, typeToRead, returnElementType, isSimpleReturnType,
				isRawAggregationResult);
	}

	static AggregationPipeline computePipeline(AbstractMongoQuery mongoQuery, MongoQueryMethod method,
			ConvertingParameterAccessor accessor) {
		return new AggregationPipeline(mongoQuery.parseAggregationPipeline(method.getAnnotatedAggregation(), accessor));
	}

	/**
	 * Append {@code $sort} aggregation stage if {@link ConvertingParameterAccessor#getSort()} is present.
	 *
	 * @param aggregationPipeline
	 * @param accessor
	 * @param targetType
	 */
	static void appendSortIfPresent(AggregationPipeline aggregationPipeline, ConvertingParameterAccessor accessor,
			@Nullable Class<?> targetType) {

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

	/**
	 * Interface to invoke an aggregation along with source, intermediate, and target types.
	 *
	 * @param <T>
	 */
	interface AggregationCallback<T> {

		/**
		 * @param aggregation
		 * @param domainType
		 * @param typeToRead
		 * @param elementType
		 * @param simpleType whether the aggregation returns {@link Document} or a
		 *          {@link org.springframework.data.mapping.model.SimpleTypeHolder simple type}.
		 * @param rawResult whether the aggregation returns {@link AggregationResults}.
		 * @return
		 */
		@Nullable
		T doAggregate(TypedAggregation<?> aggregation, Class<?> domainType, Class<?> typeToRead, Class<?> elementType,
				boolean simpleType, boolean rawResult);
	}
}

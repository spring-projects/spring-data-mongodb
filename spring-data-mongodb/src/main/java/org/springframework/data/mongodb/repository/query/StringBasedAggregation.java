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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.bson.Document;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.lang.Nullable;

/**
 * {@link AbstractMongoQuery} implementation to run string-based aggregations using
 * {@link org.springframework.data.mongodb.repository.Aggregation}.
 *
 * @author Christoph Strobl
 * @author Divya Srivastava
 * @author Mark Paluch
 * @since 2.2
 */
public class StringBasedAggregation extends AbstractMongoQuery {

	private final MongoOperations mongoOperations;
	private final MongoConverter mongoConverter;

	/**
	 * Creates a new {@link StringBasedAggregation} from the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 * @since 4.4.0
	 */
	public StringBasedAggregation(MongoQueryMethod method, MongoOperations mongoOperations,
			ValueExpressionDelegate delegate) {
		super(method, mongoOperations, delegate);

		if (method.isPageQuery()) {
			throw new InvalidMongoDbApiUsageException(String.format(
					"Repository aggregation method '%s' does not support '%s' return type; Please use 'Slice' or 'List' instead",
					method.getName(), method.getReturnType().getType().getSimpleName()));
		}

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
	}

	@SuppressWarnings("unchecked")
	@Override
	@Nullable
	protected Object doExecute(MongoQueryMethod method, ResultProcessor processor, ConvertingParameterAccessor accessor,
			@Nullable Class<?> ignore) {

		return AggregationUtils.doAggregate(AggregationUtils.computePipeline(this, method, accessor), method, processor,
				accessor, this::getExpressionEvaluatorFor,
				(aggregation, sourceType, typeToRead, elementType, simpleType, rawResult) -> {

					if (method.isStreamQuery()) {

						Stream<?> stream = mongoOperations.aggregateStream(aggregation, typeToRead);

						if (!simpleType || elementType.equals(Document.class)) {
							return stream;
						}

						return stream
								.map(it -> AggregationUtils.extractSimpleTypeResult((Document) it, elementType, mongoConverter));
					}

					AggregationResults<Object> result = (AggregationResults<Object>) mongoOperations.aggregate(aggregation,
							typeToRead);

					if (ReflectionUtils.isVoid(elementType)) {
						return null;
					}

					if (rawResult) {
						return result;
					}

					List<?> results = result.getMappedResults();
					if (method.isCollectionQuery()) {
						return simpleType ? convertResults(elementType, (List<Document>) results) : results;
					}

					if (method.isSliceQuery()) {

						Pageable pageable = accessor.getPageable();
						int pageSize = pageable.getPageSize();
						List<Object> resultsToUse = simpleType ? convertResults(elementType, (List<Document>) results)
								: (List<Object>) results;
						boolean hasNext = resultsToUse.size() > pageSize;
						return new SliceImpl<>(hasNext ? resultsToUse.subList(0, pageSize) : resultsToUse, pageable, hasNext);
					}

					Object uniqueResult = result.getUniqueMappedResult();

					return simpleType
							? AggregationUtils.extractSimpleTypeResult((Document) uniqueResult, elementType, mongoConverter)
							: uniqueResult;
				});
	}

	private List<Object> convertResults(Class<?> targetType, List<Document> mappedResults) {

		List<Object> list = new ArrayList<>(mappedResults.size());
		for (Document it : mappedResults) {
			Object extractSimpleTypeResult = AggregationUtils.extractSimpleTypeResult(it, targetType, mongoConverter);
			list.add(extractSimpleTypeResult);
		}
		return list;
	}

	private boolean isSimpleReturnType(Class<?> targetType) {
		return MongoSimpleTypes.HOLDER.isSimpleType(targetType);
	}

	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {
		throw new UnsupportedOperationException("No query support for aggregation");
	}

	@Override
	protected boolean isCountQuery() {
		return false;
	}

	@Override
	protected boolean isExistsQuery() {
		return false;
	}

	@Override
	protected boolean isDeleteQuery() {
		return false;
	}

	@Override
	protected boolean isLimiting() {
		return false;
	}
}

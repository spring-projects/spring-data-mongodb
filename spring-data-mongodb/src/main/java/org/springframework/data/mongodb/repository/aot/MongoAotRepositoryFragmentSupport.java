/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.repository.query.MongoParameters;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.mongodb.util.json.ValueProvider;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.expression.EvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * Support class for MongoDB AOT repository fragments.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
public class MongoAotRepositoryFragmentSupport {

	private final RepositoryMetadata repositoryMetadata;
	private final MongoOperations mongoOperations;
	private final MongoConverter mongoConverter;
	private final ProjectionFactory projectionFactory;
	private final ValueExpressionDelegate valueExpressionDelegate;

	protected MongoAotRepositoryFragmentSupport(MongoOperations mongoOperations,
			RepositoryFactoryBeanSupport.FragmentCreationContext context) {
		this(mongoOperations, context.getRepositoryMetadata(), context.getProjectionFactory(),
				context.getValueExpressionDelegate());
	}

	protected MongoAotRepositoryFragmentSupport(MongoOperations mongoOperations, RepositoryMetadata repositoryMetadata,
			ProjectionFactory projectionFactory, ValueExpressionDelegate valueExpressionDelegate) {

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
		this.repositoryMetadata = repositoryMetadata;
		this.projectionFactory = projectionFactory;
		this.valueExpressionDelegate = valueExpressionDelegate;
	}

	protected Document bindParameters(String source, Object[] parameters) {
		return new BindableMongoExpression(source, this.mongoConverter, parameters).toDocument();
	}

	protected Document bindParameters(String source, Map<String, Object> parameters) {

		ValueEvaluationContext valueEvaluationContext = this.valueExpressionDelegate.getEvaluationContextAccessor()
				.create(new NoMongoParameters()).getEvaluationContext(parameters.values());

		EvaluationContext evaluationContext = valueEvaluationContext.getEvaluationContext();
		parameters.forEach(evaluationContext::setVariable);

		ParameterBindingContext bindingContext = new ParameterBindingContext(new ValueProvider() {

			private final List<Object> args = new ArrayList<>(parameters.values());

			@Override
			public @Nullable Object getBindableValue(int index) {
				return args.get(index);
			}
		}, new ValueExpressionEvaluator() {

			@Override
			@SuppressWarnings("unchecked")
			public <T> @Nullable T evaluate(String expression) {
				ValueExpression parse = valueExpressionDelegate.getValueExpressionParser().parse(expression);
				return (T) parse.evaluate(valueEvaluationContext);
			}
		});

		return new ParameterBindingDocumentCodec().decode(source, bindingContext);
	}

	protected Object[] arguments(Object... arguments) {
		return arguments;
	}

	protected Map<String, Object> argumentMap(Object... parameters) {

		Assert.state(parameters.length % 2 == 0, "even number of args required");

		LinkedHashMap<String, Object> argumentMap = CollectionUtils.newLinkedHashMap(parameters.length / 2);
		for (int i = 0; i < parameters.length; i += 2) {

			if (!(parameters[i] instanceof String key)) {
				throw new IllegalArgumentException("key must be a String");
			}
			argumentMap.put(key, parameters[i + 1]);
		}

		return argumentMap;
	}

	protected @Nullable Object evaluate(String source, Map<String, Object> parameters) {

		ValueEvaluationContext valueEvaluationContext = this.valueExpressionDelegate.getEvaluationContextAccessor()
				.create(new NoMongoParameters()).getEvaluationContext(parameters.values());

		EvaluationContext evaluationContext = valueEvaluationContext.getEvaluationContext();
		parameters.forEach(evaluationContext::setVariable);

		ValueExpression parse = valueExpressionDelegate.getValueExpressionParser().parse(source);
		return parse.evaluate(valueEvaluationContext);
	}

	protected Consumer<Criteria> scoreBetween(Range.Bound<? extends Score> lower, Range.Bound<? extends Score> upper) {

		return criteria -> {
			if (lower.isBounded()) {
				double value = lower.getValue().get().getValue();
				if (lower.isInclusive()) {
					criteria.gte(value);
				} else {
					criteria.gt(value);
				}
			}

			if (upper.isBounded()) {

				double value = upper.getValue().get().getValue();
				if (upper.isInclusive()) {
					criteria.lte(value);
				} else {
					criteria.lt(value);
				}
			}

		};
	}

	protected ScoringFunction scoringFunction(Range<? extends Score> scoreRange) {

		if (scoreRange != null) {
			if (scoreRange.getUpperBound().isBounded()) {
				return scoreRange.getUpperBound().getValue().get().getFunction();
			}

			if (scoreRange.getLowerBound().isBounded()) {
				return scoreRange.getLowerBound().getValue().get().getFunction();
			}
		}

		return ScoringFunction.unspecified();
	}

	// Range<Score> scoreRange = accessor.getScoreRange();
	//
	// if (scoreRange != null) {
	// if (scoreRange.getUpperBound().isBounded()) {
	// return scoreRange.getUpperBound().getValue().get().getFunction();
	// }
	//
	// if (scoreRange.getLowerBound().isBounded()) {
	// return scoreRange.getLowerBound().getValue().get().getFunction();
	// }
	// }
	//
	// return ScoringFunction.unspecified();

	protected Collation collationOf(@Nullable Object source) {

		if (source == null) {
			return Collation.simple();
		}
		if (source instanceof String) {
			return Collation.parse(source.toString());
		}
		if (source instanceof Locale locale) {
			return Collation.of(locale);
		}
		if (source instanceof Document document) {
			return Collation.from(document);
		}
		if (source instanceof Collation collation) {
			return collation;
		}
		throw new IllegalArgumentException(
				"Unsupported collation source [%s]".formatted(ObjectUtils.nullSafeClassName(source)));
	}

	protected BasicQuery createQuery(String queryString, Object[] parameters) {

		Document queryDocument = bindParameters(queryString, parameters);
		return new BasicQuery(queryDocument);
	}

	protected BasicQuery createQuery(String queryString, Map<String, Object> parameters) {

		Document queryDocument = bindParameters(queryString, parameters);
		return new BasicQuery(queryDocument);
	}

	protected AggregationPipeline createPipeline(List<Object> rawStages) {

		List<AggregationOperation> stages = new ArrayList<>(rawStages.size());
		boolean first = true;
		for (Object rawStage : rawStages) {
			if (rawStage instanceof Document stageDocument) {
				if (first) {
					stages.add((ctx) -> ctx.getMappedObject(stageDocument));
				} else {
					stages.add((ctx) -> stageDocument);
				}
			} else if (rawStage instanceof AggregationOperation aggregationOperation) {
				stages.add(aggregationOperation);
			} else {
				throw new RuntimeException("%s cannot be converted to AggregationOperation".formatted(rawStage.getClass()));
			}
			if (first) {
				first = false;
			}
		}
		return new AggregationPipeline(stages);
	}

	protected List<Object> convertSimpleRawResults(Class<?> targetType, List<Document> rawResults) {

		List<Object> list = new ArrayList<>(rawResults.size());
		for (Document it : rawResults) {
			list.add(extractSimpleTypeResult(it, targetType, mongoConverter));
		}
		return list;
	}

	protected @Nullable Object convertSimpleRawResult(Class<?> targetType, Document rawResult) {
		return extractSimpleTypeResult(rawResult, targetType, mongoConverter);
	}

	private static <T> @Nullable T extractSimpleTypeResult(@Nullable Document source, Class<T> targetType,
			MongoConverter converter) {

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

	static class NoMongoParameters extends MongoParameters {

		NoMongoParameters() {
			super();
		}
	}
}

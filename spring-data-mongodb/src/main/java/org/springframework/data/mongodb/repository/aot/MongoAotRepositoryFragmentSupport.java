/*
 * Copyright 2025-present the original author or authors.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.expression.ValueEvaluationContextProvider;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.MongoRegexCreator;
import org.springframework.data.mongodb.core.query.MongoRegexCreator.MatchMode;
import org.springframework.data.mongodb.repository.query.MongoParameters;
import org.springframework.data.mongodb.repository.query.MongoParametersParameterAccessor;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.ParametersSource;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.util.Lazy;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ConcurrentLruCache;
import org.springframework.util.ObjectUtils;

/**
 * Support class for MongoDB AOT repository fragments.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
public class MongoAotRepositoryFragmentSupport {

	private static final ParameterBindingDocumentCodec CODEC = new ParameterBindingDocumentCodec();

	private final RepositoryMetadata repositoryMetadata;
	private final MongoOperations mongoOperations;
	private final MongoConverter mongoConverter;
	private final ProjectionFactory projectionFactory;
	private final ValueExpressionDelegate valueExpressions;

	private final Lazy<ConcurrentLruCache<String, ValueExpression>> expressions;
	private final Lazy<ConcurrentLruCache<Method, MongoParameters>> mongoParameters;
	private final Lazy<ConcurrentLruCache<Method, ValueEvaluationContextProvider>> contextProviders;

	protected MongoAotRepositoryFragmentSupport(MongoOperations mongoOperations,
			RepositoryFactoryBeanSupport.FragmentCreationContext context) {
		this(mongoOperations, context.getRepositoryMetadata(), context.getValueExpressionDelegate(),
				context.getProjectionFactory());
	}

	protected MongoAotRepositoryFragmentSupport(MongoOperations mongoOperations, RepositoryMetadata repositoryMetadata,
			ValueExpressionDelegate valueExpressions, ProjectionFactory projectionFactory) {

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
		this.repositoryMetadata = repositoryMetadata;
		this.projectionFactory = projectionFactory;
		this.valueExpressions = valueExpressions;

		this.expressions = Lazy.of(() -> new ConcurrentLruCache<>(32, valueExpressions::parse));
		this.mongoParameters = Lazy
				.of(() -> new ConcurrentLruCache<>(32, it -> new MongoParameters(ParametersSource.of(repositoryMetadata, it))));
		this.contextProviders = Lazy.of(() -> new ConcurrentLruCache<>(32,
				it -> valueExpressions.createValueContextProvider(mongoParameters.get().get(it))));
	}

	protected Document parse(String json) {
		return CODEC.decode(json);
	}

	protected Document bindParameters(Method method, String source, Object... args) {

		expandGeoShapes(args);

		MongoParameters mongoParameters = this.mongoParameters.get().get(method);
		MongoParametersParameterAccessor parametersParameterAccessor = new MongoParametersParameterAccessor(mongoParameters,
				args);

		ParameterBindingContext bindingContext = new ParameterBindingContext(parametersParameterAccessor::getBindableValue,
				new ValueExpressionEvaluator() {

					@Override
					@SuppressWarnings("unchecked")
					public <T> @Nullable T evaluate(String expression) {
						return (T) MongoAotRepositoryFragmentSupport.this.evaluate(method, expression, args);
					}
				});

		return CODEC.decode(source, bindingContext);
	}

	protected @Nullable Object evaluate(Method method, String source, Object... args) {

		expandGeoShapes(args);
		ValueExpression expression = this.expressions.get().get(source);
		ValueEvaluationContextProvider contextProvider = this.contextProviders.get().get(method);

		return expression.evaluate(contextProvider.getEvaluationContext(args, expression.getExpressionDependencies()));
	}

	/**
	 * Expand geo shapes in the given arguments to a format that can be handled by the MongoDB converter without us
	 * passing in the actual {@link Shape} object (except for {@link GeoJson}).
	 *
	 * @param args
	 */
	private static void expandGeoShapes(Object[] args) {

		for (int i = 0; i < args.length; i++) {

			// renders as generic $geometry, thus can be handled by the converter when parsing
			if (args[i] instanceof GeoJson) {
				continue;
			}

			if (args[i] instanceof Circle c) {
				args[i] = List.of(List.of(c.getCenter().getX(), c.getCenter().getY()), c.getRadius().getNormalizedValue());
			} else if (args[i] instanceof Sphere s) {
				args[i] = List.of(List.of(s.getCenter().getX(), s.getCenter().getY()), s.getRadius().getNormalizedValue());
			} else if (args[i] instanceof Box b) {
				args[i] = List.of(List.of(b.getFirst().getX(), b.getFirst().getY()),
						List.of(b.getSecond().getX(), b.getSecond().getY()));
			} else if (args[i] instanceof Polygon p) {
				args[i] = p.getPoints().stream().map(it -> List.of(it.getX(), it.getY())).toList();
			}
		}
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

	protected ScoringFunction scoringFunction(@Nullable Range<? extends Score> scoreRange) {

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

	protected Collation collationOf(@Nullable Object source) {

		if (source == null) {
			return Collation.simple();
		}
		if (source instanceof CharSequence) {
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

	protected Object toRegex(Object source) {
		return toRegex(source, null);
	}

	protected Object toRegex(Object source, @Nullable String options) {

		if (source instanceof String sv) {
			return new BsonRegularExpression(MongoRegexCreator.INSTANCE.toRegularExpression(sv, MatchMode.LIKE), options);
		}
		if (source instanceof Pattern pattern) {
			return pattern;
		}
		if (source instanceof Collection<?> collection) {
			return collection.stream().map(it -> toRegex(it, options)).toList();
		}
		if (ObjectUtils.isArray(source)) {
			return toRegex(List.of(source), options);
		}
		return source;
	}

	protected BasicQuery createQuery(Method method, String queryString, Object... parameters) {

		Document queryDocument = bindParameters(method, queryString, parameters);
		return new BasicQuery(queryDocument);
	}

	@SuppressWarnings("NullAway")
	protected AggregationPipeline createPipeline(List<Object> rawStages) {

		if (rawStages.isEmpty()) {
			return new AggregationPipeline(List.of());
		}

		int size = rawStages.size();
		List<AggregationOperation> stages = new ArrayList<>(size);

		Object firstElement = CollectionUtils.firstElement(rawStages);
		stages.add(rawToAggregationOperation(firstElement, true));

		if (size == 1) {
			return new AggregationPipeline(stages);
		}

		for (int i = 1; i < size; i++) {
			stages.add(rawToAggregationOperation(rawStages.get(i), false));
		}

		return new AggregationPipeline(stages);
	}

	private static AggregationOperation rawToAggregationOperation(Object rawStage, boolean requiresMapping) {

		if (rawStage instanceof Document stageDocument) {
			if (requiresMapping) {
				return (ctx) -> ctx.getMappedObject(stageDocument);
			} else {
				return (ctx) -> stageDocument;
			}
		}

		if (rawStage instanceof AggregationOperation aggregationOperation) {
			return aggregationOperation;
		}
		throw new RuntimeException("%s cannot be converted to AggregationOperation".formatted(rawStage.getClass()));

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

}

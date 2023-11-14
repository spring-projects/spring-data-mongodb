/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.MappedDocument.MappedUpdate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.RelaxedTypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.ShardKey;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;

/**
 * {@link QueryOperations} centralizes common operations required before an operation is actually ready to be executed.
 * This involves mapping {@link Query queries} into their respective MongoDB representation, computing execution options
 * for {@literal count}, {@literal remove}, and other methods.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.0
 */
class QueryOperations {

	private final QueryMapper queryMapper;
	private final UpdateMapper updateMapper;
	private final EntityOperations entityOperations;
	private final PropertyOperations propertyOperations;
	private final CodecRegistryProvider codecRegistryProvider;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final AggregationUtil aggregationUtil;
	private final Map<Class<?>, Document> mappedShardKey = new ConcurrentHashMap<>(1);

	/**
	 * Create a new instance of {@link QueryOperations}.
	 *
	 * @param queryMapper must not be {@literal null}.
	 * @param updateMapper must not be {@literal null}.
	 * @param entityOperations must not be {@literal null}.
	 * @param propertyOperations must not be {@literal null}.
	 * @param codecRegistryProvider must not be {@literal null}.
	 */
	QueryOperations(QueryMapper queryMapper, UpdateMapper updateMapper, EntityOperations entityOperations,
			PropertyOperations propertyOperations, CodecRegistryProvider codecRegistryProvider) {

		this.queryMapper = queryMapper;
		this.updateMapper = updateMapper;
		this.entityOperations = entityOperations;
		this.propertyOperations = propertyOperations;
		this.codecRegistryProvider = codecRegistryProvider;
		this.mappingContext = queryMapper.getMappingContext();
		this.aggregationUtil = new AggregationUtil(queryMapper, mappingContext);
	}

	InsertContext createInsertContext(Document source) {
		return createInsertContext(MappedDocument.of(source));
	}

	InsertContext createInsertContext(MappedDocument mappedDocument) {
		return new InsertContext(mappedDocument);
	}

	/**
	 * Create a new {@link QueryContext} instance.
	 *
	 * @param query must not be {@literal null}.
	 * @return new instance of {@link QueryContext}.
	 */
	QueryContext createQueryContext(Query query) {
		return new QueryContext(query);
	}

	/**
	 * Create a new {@link DistinctQueryContext} instance.
	 *
	 * @param query must not be {@literal null}.
	 * @return new instance of {@link DistinctQueryContext}.
	 */
	DistinctQueryContext distinctQueryContext(Query query, String fieldName) {
		return new DistinctQueryContext(query, fieldName);
	}

	/**
	 * Create a new {@link CountContext} instance.
	 *
	 * @param query must not be {@literal null}.
	 * @return new instance of {@link CountContext}.
	 */
	CountContext countQueryContext(Query query) {
		return new CountContext(query);
	}

	/**
	 * Create a new {@link UpdateContext} instance affecting multiple documents.
	 *
	 * @param updateDefinition must not be {@literal null}.
	 * @param query must not be {@literal null}.
	 * @param upsert use {@literal true} to insert diff when no existing document found.
	 * @return new instance of {@link UpdateContext}.
	 */
	UpdateContext updateContext(UpdateDefinition updateDefinition, Query query, boolean upsert) {
		return new UpdateContext(updateDefinition, query, true, upsert);
	}

	/**
	 * Create a new {@link UpdateContext} instance affecting a single document.
	 *
	 * @param updateDefinition must not be {@literal null}.
	 * @param query must not be {@literal null}.
	 * @param upsert use {@literal true} to insert diff when no existing document found.
	 * @return new instance of {@link UpdateContext}.
	 */
	UpdateContext updateSingleContext(UpdateDefinition updateDefinition, Query query, boolean upsert) {
		return new UpdateContext(updateDefinition, query, false, upsert);
	}

	/**
	 * Create a new {@link UpdateContext} instance affecting a single document.
	 *
	 * @param updateDefinition must not be {@literal null}.
	 * @param query must not be {@literal null}.
	 * @param upsert use {@literal true} to insert diff when no existing document found.
	 * @return new instance of {@link UpdateContext}.
	 */
	UpdateContext updateSingleContext(UpdateDefinition updateDefinition, Document query, boolean upsert) {
		return new UpdateContext(updateDefinition, query, false, upsert);
	}

	/**
	 * @param replacement the {@link MappedDocument mapped replacement} document.
	 * @param upsert use {@literal true} to insert diff when no existing document found.
	 * @return new instance of {@link UpdateContext}.
	 */
	UpdateContext replaceSingleContext(MappedDocument replacement, boolean upsert) {
		return new UpdateContext(replacement, upsert);
	}

	/**
	 * @param replacement the {@link MappedDocument mapped replacement} document.
	 * @param upsert use {@literal true} to insert diff when no existing document found.
	 * @return new instance of {@link UpdateContext}.
	 */
	UpdateContext replaceSingleContext(Query query, MappedDocument replacement, boolean upsert) {
		return new UpdateContext(query, replacement, upsert);
	}

	/**
	 * Create a new {@link DeleteContext} instance removing all matching documents.
	 *
	 * @param query must not be {@literal null}.
	 * @return new instance of {@link QueryContext}.
	 */
	DeleteContext deleteQueryContext(Query query) {
		return new DeleteContext(query, true);
	}

	/**
	 * Create a new {@link DeleteContext} instance only the first matching document.
	 *
	 * @param query must not be {@literal null}.
	 * @return new instance of {@link QueryContext}.
	 */
	DeleteContext deleteSingleContext(Query query) {
		return new DeleteContext(query, false);
	}

	/**
	 * Create a new {@link AggregationDefinition} for the given {@link Aggregation}.
	 *
	 * @param aggregation must not be {@literal null}.
	 * @param inputType fallback mapping type in case of untyped aggregation. Can be {@literal null}.
	 * @return new instance of {@link AggregationDefinition}.
	 * @since 3.2
	 */
	AggregationDefinition createAggregation(Aggregation aggregation, @Nullable Class<?> inputType) {
		return new AggregationDefinition(aggregation, inputType);
	}

	/**
	 * Create a new {@link AggregationDefinition} for the given {@link Aggregation}.
	 *
	 * @param aggregation must not be {@literal null}.
	 * @param aggregationOperationContext the {@link AggregationOperationContext} to use. Can be {@literal null}.
	 * @return new instance of {@link AggregationDefinition}.
	 * @since 3.2
	 */
	AggregationDefinition createAggregation(Aggregation aggregation,
			@Nullable AggregationOperationContext aggregationOperationContext) {
		return new AggregationDefinition(aggregation, aggregationOperationContext);
	}

	/**
	 * {@link InsertContext} encapsulates common tasks required to interact with {@link Document} to be inserted.
	 *
	 * @since 3.4.3
	 */
	class InsertContext {

		private final MappedDocument source;

		private InsertContext(MappedDocument source) {
			this.source = source;
		}

		/**
		 * Prepare the {@literal _id} field. May generate a new {@literal id} value and convert it to the id properties
		 * {@link MongoPersistentProperty#getFieldType() target type}.
		 *
		 * @param type must not be {@literal null}.
		 * @param <T>
		 * @return the {@link MappedDocument} containing the changes.
		 * @see #prepareId(MongoPersistentEntity)
		 */
		<T> MappedDocument prepareId(Class<T> type) {
			return prepareId(mappingContext.getPersistentEntity(type));
		}

		/**
		 * Prepare the {@literal _id} field. May generate a new {@literal id} value and convert it to the id properties
		 * {@link MongoPersistentProperty#getFieldType() target type}.
		 *
		 * @param entity can be {@literal null}.
		 * @param <T>
		 * @return the {@link MappedDocument} containing the changes.
		 */
		<T> MappedDocument prepareId(@Nullable MongoPersistentEntity<T> entity) {

			if (entity == null || source.hasId()) {
				return source;
			}

			MongoPersistentProperty idProperty = entity.getIdProperty();
			if (idProperty != null
					&& (idProperty.hasExplicitWriteTarget() || idProperty.isAnnotationPresent(MongoId.class))) {
				if (!ClassUtils.isAssignable(ObjectId.class, idProperty.getFieldType())) {
					source.updateId(queryMapper.convertId(new ObjectId(), idProperty.getFieldType()));
				}
			}
			return source;
		}
	}

	/**
	 * {@link QueryContext} encapsulates common tasks required to convert a {@link Query} into its MongoDB document
	 * representation, mapping field names, as well as determining and applying {@link Collation collations}.
	 *
	 * @author Christoph Strobl
	 */
	class QueryContext {

		private final Query query;

		/**
		 * Create new a {@link QueryContext} instance from the given {@literal query} (can be either a {@link Query} or a
		 * plain {@link Document}.
		 *
		 * @param query can be {@literal null}.
		 */
		private QueryContext(@Nullable Query query) {
			this.query = query != null ? query : new Query();
		}

		/**
		 * @return never {@literal null}.
		 */
		Query getQuery() {
			return query;
		}

		/**
		 * Extract the raw {@link Query#getQueryObject() unmapped document} from the {@link Query}.
		 *
		 * @return
		 */
		Document getQueryObject() {
			return query.getQueryObject();
		}

		/**
		 * Get the already mapped MongoDB query representation.
		 *
		 * @param domainType can be {@literal null}.
		 * @param entityLookup the {@link Function lookup} used to provide the {@link MongoPersistentEntity} for the
		 *          given{@literal domainType}
		 * @param <T>
		 * @return never {@literal null}.
		 */
		<T> Document getMappedQuery(@Nullable Class<T> domainType,
				Function<Class<T>, MongoPersistentEntity<?>> entityLookup) {
			return getMappedQuery(domainType == null ? null : entityLookup.apply(domainType));
		}

		/**
		 * Get the already mapped MongoDB query representation.
		 *
		 * @param entity the Entity to map field names to. Can be {@literal null}.
		 * @param <T>
		 * @return never {@literal null}.
		 */
		<T> Document getMappedQuery(@Nullable MongoPersistentEntity<T> entity) {
			return queryMapper.getMappedObject(getQueryObject(), entity);
		}

		Document getMappedFields(@Nullable MongoPersistentEntity<?> entity, EntityProjection<?, ?> projection) {

			Document fields = evaluateFields(entity);

			if (entity == null) {
				return fields;
			}

			Document mappedFields;
			if (!fields.isEmpty()) {
				mappedFields = queryMapper.getMappedFields(fields, entity);
			} else {
				mappedFields = propertyOperations.computeMappedFieldsForProjection(projection, fields);
				mappedFields = queryMapper.addMetaAttributes(mappedFields, entity);
			}

			if (entity.hasTextScoreProperty() && mappedFields.containsKey(entity.getTextScoreProperty().getFieldName())
					&& !query.getQueryObject().containsKey("$text")) {
				mappedFields.remove(entity.getTextScoreProperty().getFieldName());
			}

			if (mappedFields.isEmpty()) {
				return BsonUtils.EMPTY_DOCUMENT;
			}

			return mappedFields;
		}

		private Document evaluateFields(@Nullable MongoPersistentEntity<?> entity) {

			Document fields = query.getFieldsObject();

			if (fields.isEmpty()) {
				return BsonUtils.EMPTY_DOCUMENT;
			}

			Document evaluated = new Document();

			for (Entry<String, Object> entry : fields.entrySet()) {

				if (entry.getValue()instanceof MongoExpression mongoExpression) {

					AggregationOperationContext ctx = entity == null ? Aggregation.DEFAULT_CONTEXT
							: new RelaxedTypeBasedAggregationOperationContext(entity.getType(), mappingContext, queryMapper);

					evaluated.put(entry.getKey(), AggregationExpression.from(mongoExpression).toDocument(ctx));
				} else {
					evaluated.put(entry.getKey(), entry.getValue());
				}
			}

			return evaluated;
		}

		/**
		 * Get the already mapped {@link Query#getSortObject() sort} option.
		 *
		 * @param entity the Entity to map field names to. Can be {@literal null}.
		 * @return never {@literal null}.
		 */
		Document getMappedSort(@Nullable MongoPersistentEntity<?> entity) {
			return queryMapper.getMappedSort(query.getSortObject(), entity);
		}

		/**
		 * Apply the {@link com.mongodb.client.model.Collation} if present extracted from the {@link Query} or fall back to
		 * the {@literal domain types} default {@link org.springframework.data.mongodb.core.mapping.Document#collation()
		 * collation}.
		 *
		 * @param domainType can be {@literal null}.
		 * @param consumer must not be {@literal null}.
		 */
		void applyCollation(@Nullable Class<?> domainType, Consumer<com.mongodb.client.model.Collation> consumer) {
			getCollation(domainType).ifPresent(consumer);
		}

		/**
		 * Get the {@link com.mongodb.client.model.Collation} extracted from the {@link Query} if present or fall back to
		 * the {@literal domain types} default {@link org.springframework.data.mongodb.core.mapping.Document#collation()
		 * collation}.
		 *
		 * @param domainType can be {@literal null}.
		 * @return never {@literal null}.
		 */
		Optional<com.mongodb.client.model.Collation> getCollation(@Nullable Class<?> domainType) {

			return entityOperations.forType(domainType).getCollation(query) //
					.map(Collation::toMongoCollation);
		}

		/**
		 * Get the {@link HintFunction} reading the actual hint form the {@link Query}.
		 *
		 * @return new instance of {@link HintFunction}.
		 * @since 4.2
		 */
		HintFunction getHintFunction() {
			return HintFunction.from(query.getHint());
		}

		/**
		 * Read and apply the hint from the {@link Query}.
		 *
		 * @since 4.2
		 */
		<R> void applyHint(Function<String, R> stringConsumer, Function<Bson, R> bsonConsumer) {
			getHintFunction().ifPresent(codecRegistryProvider, stringConsumer, bsonConsumer);
		}
	}

	/**
	 * A {@link QueryContext} that encapsulates common tasks required when running {@literal distinct} queries.
	 *
	 * @author Christoph Strobl
	 */
	class DistinctQueryContext extends QueryContext {

		private final String fieldName;

		/**
		 * Create a new {@link DistinctQueryContext} instance.
		 *
		 * @param query can be {@literal null}.
		 * @param fieldName must not be {@literal null}.
		 */
		private DistinctQueryContext(@Nullable Object query, String fieldName) {

			super(query instanceof Document document ? new BasicQuery(document) : (Query) query);
			this.fieldName = fieldName;
		}

		@Override
		Document getMappedFields(@Nullable MongoPersistentEntity<?> entity, EntityProjection<?, ?> projection) {
			return getMappedFields(entity);
		}

		Document getMappedFields(@Nullable MongoPersistentEntity<?> entity) {
			return queryMapper.getMappedFields(new Document(fieldName, 1), entity);
		}

		/**
		 * Get the mapped field name to project to.
		 *
		 * @param entity can be {@literal null}.
		 * @return never {@literal null}.
		 */
		String getMappedFieldName(@Nullable MongoPersistentEntity<?> entity) {
			return getMappedFields(entity).keySet().iterator().next();
		}

		/**
		 * Get the MongoDB native representation of the given {@literal type}.
		 *
		 * @param type must not be {@literal null}.
		 * @param <T>
		 * @return never {@literal null}.
		 */
		@SuppressWarnings("unchecked")
		<T> Class<T> getDriverCompatibleClass(Class<T> type) {

			return codecRegistryProvider.getCodecFor(type) //
					.map(Codec::getEncoderClass) //
					.orElse((Class<T>) BsonValue.class);
		}

		/**
		 * Get the most specific read target type based on the user {@literal requestedTargetType} an the property type
		 * based on meta information extracted from the {@literal domainType}.
		 *
		 * @param requestedTargetType must not be {@literal null}.
		 * @param domainType must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		Class<?> getMostSpecificConversionTargetType(Class<?> requestedTargetType, Class<?> domainType) {

			Class<?> conversionTargetType = requestedTargetType;
			try {

				Class<?> propertyType = PropertyPath.from(fieldName, domainType).getLeafProperty().getLeafType();

				// use the more specific type but favor UserType over property one
				if (ClassUtils.isAssignable(requestedTargetType, propertyType)) {
					conversionTargetType = propertyType;
				}
			} catch (PropertyReferenceException e) {
				// just don't care about it as we default to Object.class anyway.
			}

			return conversionTargetType;
		}
	}

	/**
	 * A {@link QueryContext} that encapsulates common tasks required when running {@literal count} queries.
	 *
	 * @author Christoph Strobl
	 */
	class CountContext extends QueryContext {

		/**
		 * Creates a new {@link CountContext} instance.
		 *
		 * @param query can be {@literal null}.
		 */
		CountContext(@Nullable Query query) {
			super(query);
		}

		/**
		 * Get the {@link CountOptions} applicable for the {@link Query}.
		 *
		 * @param domainType must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		CountOptions getCountOptions(@Nullable Class<?> domainType) {
			return getCountOptions(domainType, null);
		}

		/**
		 * Get the {@link CountOptions} applicable for the {@link Query}.
		 *
		 * @param domainType can be {@literal null}.
		 * @param callback a callback to modify the generated options. Can be {@literal null}.
		 * @return
		 */
		CountOptions getCountOptions(@Nullable Class<?> domainType, @Nullable Consumer<CountOptions> callback) {

			CountOptions options = new CountOptions();
			Query query = getQuery();

			applyCollation(domainType, options::collation);

			if (query.getLimit() > 0) {
				options.limit(query.getLimit());
			}

			if (query.getSkip() > 0) {
				options.skip((int) query.getSkip());
			}

			Meta meta = query.getMeta();
			if (meta.hasValues()) {

				if (meta.hasMaxTime()) {
					options.maxTime(meta.getRequiredMaxTimeMsec(), TimeUnit.MILLISECONDS);
				}

				if (meta.hasComment()) {
					options.comment(meta.getComment());
				}
			}

			HintFunction hintFunction = HintFunction.from(query.getHint());

			if (hintFunction.isPresent()) {
				options = hintFunction.apply(codecRegistryProvider, options::hintString, options::hint);
			}

			if (callback != null) {
				callback.accept(options);
			}

			return options;
		}
	}

	/**
	 * A {@link QueryContext} that encapsulates common tasks required when running {@literal delete} queries.
	 *
	 * @author Christoph Strobl
	 */
	class DeleteContext extends QueryContext {

		private final boolean multi;

		/**
		 * Crate a new {@link DeleteContext} instance.
		 *
		 * @param query can be {@literal null}.
		 * @param multi use {@literal true} to remove all matching documents, {@literal false} for just the first one.
		 */
		DeleteContext(@Nullable Query query, boolean multi) {

			super(query);
			this.multi = multi;
		}

		/**
		 * Get the {@link DeleteOptions} applicable for the {@link Query}.
		 *
		 * @param domainType must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		DeleteOptions getDeleteOptions(@Nullable Class<?> domainType) {
			return getDeleteOptions(domainType, null);
		}

		/**
		 * Get the {@link DeleteOptions} applicable for the {@link Query}.
		 *
		 * @param domainType can be {@literal null}.
		 * @param callback a callback to modify the generated options. Can be {@literal null}.
		 * @return
		 */
		DeleteOptions getDeleteOptions(@Nullable Class<?> domainType, @Nullable Consumer<DeleteOptions> callback) {

			DeleteOptions options = new DeleteOptions();
			applyCollation(domainType, options::collation);

			if (callback != null) {
				callback.accept(options);
			}

			return options;
		}

		/**
		 * @return {@literal true} if all matching documents shall be deleted.
		 */
		boolean isMulti() {
			return multi;
		}
	}

	/**
	 * A {@link QueryContext} that encapsulates common tasks required when running {@literal updates}.
	 */
	class UpdateContext extends QueryContext {

		private final boolean multi;
		private final boolean upsert;
		private final @Nullable UpdateDefinition update;
		private final @Nullable MappedDocument mappedDocument;

		/**
		 * Create a new {@link UpdateContext} instance.
		 *
		 * @param update must not be {@literal null}.
		 * @param query must not be {@literal null}.
		 * @param multi use {@literal true} to update all matching documents.
		 * @param upsert use {@literal true} to insert a new document if none match.
		 */
		UpdateContext(UpdateDefinition update, Document query, boolean multi, boolean upsert) {
			this(update, new BasicQuery(query), multi, upsert);
		}

		/**
		 * Create a new {@link UpdateContext} instance.
		 *
		 * @param update must not be {@literal null}.
		 * @param query can be {@literal null}.
		 * @param multi use {@literal true} to update all matching documents.
		 * @param upsert use {@literal true} to insert a new document if none match.
		 */
		UpdateContext(UpdateDefinition update, @Nullable Query query, boolean multi, boolean upsert) {

			super(query);

			this.multi = multi;
			this.upsert = upsert;
			this.update = update;
			this.mappedDocument = null;
		}

		UpdateContext(MappedDocument update, boolean upsert) {
			this(new BasicQuery(BsonUtils.asDocument(update.getIdFilter())), update, upsert);
		}

		UpdateContext(Query query, MappedDocument update, boolean upsert) {

			super(query);
			this.multi = false;
			this.upsert = upsert;
			this.mappedDocument = update;
			this.update = null;
		}

		/**
		 * Get the {@link UpdateOptions} applicable for the {@link Query}.
		 *
		 * @param domainType must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		UpdateOptions getUpdateOptions(@Nullable Class<?> domainType) {
			return getUpdateOptions(domainType, null);
		}

		/**
		 * Get the {@link UpdateOptions} applicable for the {@link Query}.
		 *
		 * @param domainType can be {@literal null}.
		 * @param callback a callback to modify the generated options. Can be {@literal null}.
		 * @return
		 */
		UpdateOptions getUpdateOptions(@Nullable Class<?> domainType, @Nullable Consumer<UpdateOptions> callback) {

			UpdateOptions options = new UpdateOptions();
			options.upsert(upsert);

			if (update != null && update.hasArrayFilters()) {
				options
						.arrayFilters(update.getArrayFilters().stream().map(ArrayFilter::asDocument).collect(Collectors.toList()));
			}

			HintFunction.from(getQuery().getHint()).ifPresent(codecRegistryProvider, options::hintString, options::hint);
			applyCollation(domainType, options::collation);

			if (callback != null) {
				callback.accept(options);
			}

			return options;
		}

		/**
		 * Get the {@link ReplaceOptions} applicable for the {@link Query}.
		 *
		 * @param domainType must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		ReplaceOptions getReplaceOptions(@Nullable Class<?> domainType) {
			return getReplaceOptions(domainType, null);
		}

		/**
		 * Get the {@link ReplaceOptions} applicable for the {@link Query}.
		 *
		 * @param domainType can be {@literal null}.
		 * @param callback a callback to modify the generated options. Can be {@literal null}.
		 * @return
		 */
		ReplaceOptions getReplaceOptions(@Nullable Class<?> domainType, @Nullable Consumer<ReplaceOptions> callback) {

			UpdateOptions updateOptions = getUpdateOptions(domainType);

			ReplaceOptions options = new ReplaceOptions();
			options.collation(updateOptions.getCollation());
			options.upsert(updateOptions.isUpsert());
			applyHint(options::hintString, options::hint);

			if (callback != null) {
				callback.accept(options);
			}

			return options;
		}

		@Override
		<T> Document getMappedQuery(@Nullable MongoPersistentEntity<T> domainType) {

			Document mappedQuery = super.getMappedQuery(domainType);

			if (multi && update != null && update.isIsolated() && !mappedQuery.containsKey("$isolated")) {
				mappedQuery.put("$isolated", 1);
			}

			return mappedQuery;
		}

		<T> Document applyShardKey(MongoPersistentEntity<T> domainType, Document filter, @Nullable Document existing) {

			Document shardKeySource = existing != null ? existing
					: mappedDocument != null ? mappedDocument.getDocument() : getMappedUpdate(domainType);

			Document filterWithShardKey = new Document(filter);
			getMappedShardKeyFields(domainType)
					.forEach(key -> filterWithShardKey.putIfAbsent(key, BsonUtils.resolveValue((Bson) shardKeySource, key)));

			return filterWithShardKey;
		}

		boolean requiresShardKey(Document filter, @Nullable MongoPersistentEntity<?> domainType) {

			return !multi && domainType != null && domainType.isSharded() && !shardedById(domainType)
					&& !filter.keySet().containsAll(getMappedShardKeyFields(domainType));
		}

		/**
		 * @return {@literal true} if the {@link MongoPersistentEntity#getShardKey() shard key} is the entities
		 *         {@literal id} property.
		 * @since 3.0
		 */
		private boolean shardedById(MongoPersistentEntity<?> domainType) {

			ShardKey shardKey = domainType.getShardKey();
			if (shardKey.size() != 1) {
				return false;
			}

			String key = shardKey.getPropertyNames().iterator().next();
			if (FieldName.ID.name().equals(key)) {
				return true;
			}

			MongoPersistentProperty idProperty = domainType.getIdProperty();
			return idProperty != null && idProperty.getName().equals(key);
		}

		Set<String> getMappedShardKeyFields(MongoPersistentEntity<?> entity) {
			return getMappedShardKey(entity).keySet();
		}

		Document getMappedShardKey(MongoPersistentEntity<?> entity) {
			return mappedShardKey.computeIfAbsent(entity.getType(),
					key -> queryMapper.getMappedFields(entity.getShardKey().getDocument(), entity));
		}

		/**
		 * Get the already mapped aggregation pipeline to use with an {@link #isAggregationUpdate()}.
		 *
		 * @param domainType must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		List<Document> getUpdatePipeline(@Nullable Class<?> domainType) {

			Class<?> type = domainType != null ? domainType : Object.class;

			AggregationOperationContext context = new RelaxedTypeBasedAggregationOperationContext(type, mappingContext,
					queryMapper);
			return aggregationUtil.createPipeline((AggregationUpdate) update, context);
		}

		/**
		 * Get the already mapped update {@link Document}.
		 *
		 * @param entity
		 * @return
		 */
		Document getMappedUpdate(@Nullable MongoPersistentEntity<?> entity) {

			if (update != null) {
				return update instanceof MappedUpdate ? update.getUpdateObject()
						: updateMapper.getMappedObject(update.getUpdateObject(), entity);
			}
			return mappedDocument.getDocument();
		}

		/**
		 * Increase a potential {@link MongoPersistentEntity#getVersionProperty() version property} prior to update if not
		 * already done in the actual {@link UpdateDefinition}
		 *
		 * @param persistentEntity can be {@literal null}.
		 */
		void increaseVersionForUpdateIfNecessary(@Nullable MongoPersistentEntity<?> persistentEntity) {

			if (persistentEntity != null && persistentEntity.hasVersionProperty()) {

				String versionFieldName = persistentEntity.getRequiredVersionProperty().getFieldName();
				if (update != null && !update.modifies(versionFieldName)) {
					update.inc(versionFieldName);
				}
			}
		}

		/**
		 * @return {@literal true} if the update holds an aggregation pipeline.
		 */
		boolean isAggregationUpdate() {
			return update instanceof AggregationUpdate;
		}

		/**
		 * @return {@literal true} if all matching documents should be updated.
		 */
		boolean isMulti() {
			return multi;
		}
	}

	/**
	 * A value object that encapsulates common tasks required when running {@literal aggregations}.
	 *
	 * @since 3.2
	 */
	class AggregationDefinition {

		private final Aggregation aggregation;
		private final Lazy<AggregationOperationContext> aggregationOperationContext;
		private final Lazy<List<Document>> pipeline;
		private final @Nullable Class<?> inputType;

		/**
		 * Creates new instance of {@link AggregationDefinition} extracting the input type from either the
		 * {@link org.springframework.data.mongodb.core.aggregation.Aggregation} in case of a {@link TypedAggregation} or
		 * the given {@literal aggregationOperationContext} if present. <br />
		 * Creates a new {@link AggregationOperationContext} if none given, based on the {@link Aggregation} input type and
		 * the desired {@link AggregationOptions#getDomainTypeMapping() domain type mapping}. <br />
		 * Pipelines are mapped on first access of {@link #getAggregationPipeline()} and cached for reuse.
		 *
		 * @param aggregation the source aggregation.
		 * @param aggregationOperationContext can be {@literal null}.
		 */
		AggregationDefinition(Aggregation aggregation, @Nullable AggregationOperationContext aggregationOperationContext) {

			this.aggregation = aggregation;

			if (aggregation instanceof TypedAggregation typedAggregation) {
				this.inputType = typedAggregation.getInputType();
			} else if (aggregationOperationContext instanceof TypeBasedAggregationOperationContext typeBasedAggregationOperationContext) {
				this.inputType = typeBasedAggregationOperationContext.getType();
			} else {
				this.inputType = null;
			}

			this.aggregationOperationContext = Lazy.of(() -> aggregationOperationContext != null ? aggregationOperationContext
					: aggregationUtil.createAggregationContext(aggregation, getInputType()));
			this.pipeline = Lazy.of(() -> aggregationUtil.createPipeline(this.aggregation, getAggregationOperationContext()));
		}

		/**
		 * Creates new instance of {@link AggregationDefinition} extracting the input type from either the
		 * {@link org.springframework.data.mongodb.core.aggregation.Aggregation} in case of a {@link TypedAggregation} or
		 * the given {@literal aggregationOperationContext} if present. <br />
		 * Creates a new {@link AggregationOperationContext} based on the {@link Aggregation} input type and the desired
		 * {@link AggregationOptions#getDomainTypeMapping() domain type mapping}. <br />
		 * Pipelines are mapped on first access of {@link #getAggregationPipeline()} and cached for reuse.
		 *
		 * @param aggregation the source aggregation.
		 * @param inputType can be {@literal null}.
		 */
		AggregationDefinition(Aggregation aggregation, @Nullable Class<?> inputType) {

			this.aggregation = aggregation;

			if (aggregation instanceof TypedAggregation typedAggregation) {
				this.inputType = typedAggregation.getInputType();
			} else {
				this.inputType = inputType;
			}

			this.aggregationOperationContext = Lazy
					.of(() -> aggregationUtil.createAggregationContext(aggregation, getInputType()));
			this.pipeline = Lazy.of(() -> aggregationUtil.createPipeline(this.aggregation, getAggregationOperationContext()));
		}

		/**
		 * Obtain the already mapped pipeline.
		 *
		 * @return never {@literal null}.
		 */
		List<Document> getAggregationPipeline() {
			return pipeline.get();
		}

		/**
		 * @return {@literal true} if the last aggregation stage is either {@literal $out} or {@literal $merge}.
		 * @see AggregationPipeline#isOutOrMerge()
		 */
		boolean isOutOrMerge() {
			return aggregation.getPipeline().isOutOrMerge();
		}

		/**
		 * Obtain the {@link AggregationOperationContext} used for mapping the pipeline.
		 *
		 * @return never {@literal null}.
		 */
		AggregationOperationContext getAggregationOperationContext() {
			return aggregationOperationContext.get();
		}

		/**
		 * @return the input type to map the pipeline against. Can be {@literal null}.
		 */
		@Nullable
		Class<?> getInputType() {
			return inputType;
		}
	}
}

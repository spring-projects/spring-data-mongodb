/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mongodb.core.convert.ReferenceLoader.DocumentReferenceQuery;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.MongoEntityReader;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.ReferenceCollection;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.mongodb.util.json.ValueProvider;
import org.springframework.data.mongodb.util.spel.ExpressionUtils;
import org.springframework.data.util.Streamable;
import org.springframework.expression.EvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;

/**
 * A common delegate for {@link ReferenceResolver} implementations to resolve a reference to one/many target documents
 * that are converted to entities.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.3
 */
public final class ReferenceLookupDelegate {

	private static final Document NO_RESULTS_PREDICATE = new Document(FieldName.ID.name(),
			new Document("$exists", false));

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final SpELContext spELContext;
	private final ParameterBindingDocumentCodec codec;

	/**
	 * Create a new {@link ReferenceLookupDelegate}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param spELContext must not be {@literal null}.
	 */
	public ReferenceLookupDelegate(
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			SpELContext spELContext) {

		Assert.notNull(mappingContext, "MappingContext must not be null");
		Assert.notNull(spELContext, "SpELContext must not be null");

		this.mappingContext = mappingContext;
		this.spELContext = spELContext;
		this.codec = new ParameterBindingDocumentCodec();
	}

	/**
	 * Read the reference expressed by the given property.
	 *
	 * @param property the reference defining property. Must not be {@literal null}. THe
	 * @param source the source value identifying to the referenced entity. Must not be {@literal null}.
	 * @param lookupFunction to execute a lookup query. Must not be {@literal null}.
	 * @param entityReader the callback to convert raw source values into actual domain types. Must not be
	 *          {@literal null}.
	 * @return can be {@literal null}.
	 */
	@Nullable
	public Object readReference(MongoPersistentProperty property, Object source, LookupFunction lookupFunction,
			MongoEntityReader entityReader) {

		Object value = source instanceof DocumentReferenceSource documentReferenceSource ? documentReferenceSource.getTargetSource()
				: source;

		DocumentReferenceQuery filter = computeFilter(property, source, spELContext);
		ReferenceCollection referenceCollection = computeReferenceContext(property, value, spELContext);

		Iterable<Document> result = lookupFunction.apply(filter, referenceCollection);

		if (property.isCollectionLike()) {
			return entityReader.read(result, property.getTypeInformation());
		}

		if (!result.iterator().hasNext()) {
			return null;
		}

		Object resultValue = result.iterator().next();
		return resultValue != null ? entityReader.read(resultValue, property.getTypeInformation()) : null;
	}

	private ReferenceCollection computeReferenceContext(MongoPersistentProperty property, Object value,
			SpELContext spELContext) {

		// Use the first value as a reference for others in case of collection like
		if (value instanceof Iterable<?> iterable) {

			Iterator<?> iterator = iterable.iterator();
			value = iterator.hasNext() ? iterator.next() : new Document();
		}

		// handle DBRef value
		if (value instanceof DBRef dbRef) {
			return ReferenceCollection.fromDBRef(dbRef);
		}

		String collection = mappingContext.getRequiredPersistentEntity(property.getAssociationTargetType()).getCollection();

		if (value instanceof Document documentPointer) {

			if (property.isDocumentReference()) {

				ParameterBindingContext bindingContext = bindingContext(property, value, spELContext);
				DocumentReference documentReference = property.getDocumentReference();

				String targetDatabase = parseValueOrGet(documentReference.db(), bindingContext,
						() -> documentPointer.get("db", String.class));
				String targetCollection = parseValueOrGet(documentReference.collection(), bindingContext,
						() -> documentPointer.get("collection", collection));
				return new ReferenceCollection(targetDatabase, targetCollection);
			}

			return new ReferenceCollection(documentPointer.getString("db"), documentPointer.get("collection", collection));
		}

		if (property.isDocumentReference()) {

			ParameterBindingContext bindingContext = bindingContext(property, value, spELContext);
			DocumentReference documentReference = property.getDocumentReference();

			String targetDatabase = parseValueOrGet(documentReference.db(), bindingContext, () -> null);
			String targetCollection = parseValueOrGet(documentReference.collection(), bindingContext, () -> collection);

			return new ReferenceCollection(targetDatabase, targetCollection);
		}

		return new ReferenceCollection(null, collection);
	}

	/**
	 * Use the given {@link ParameterBindingContext} to compute potential expressions against the value.
	 *
	 * @param value must not be {@literal null}.
	 * @param bindingContext must not be {@literal null}.
	 * @param defaultValue
	 * @param <T>
	 * @return can be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	private <T> T parseValueOrGet(String value, ParameterBindingContext bindingContext, Supplier<T> defaultValue) {

		if (!StringUtils.hasText(value)) {
			return defaultValue.get();
		}

		// parameter binding requires a document, since we do not have one, construct it.
		if (!BsonUtils.isJsonDocument(value) && value.contains("?#{")) {
			String s = "{ 'target-value' : " + value + "}";
			T evaluated = (T) codec.decode(s, bindingContext).get("target-value");
			return evaluated != null ? evaluated : defaultValue.get();
		}

		if (BsonUtils.isJsonDocument(value)) {
			return (T) codec.decode(value, bindingContext);
		}

		if (!value.startsWith("#") && ExpressionUtils.detectExpression(value) == null) {
			return (T) value;
		}

		T evaluated = (T) bindingContext.evaluateExpression(value);
		return evaluated != null ? evaluated : defaultValue.get();
	}

	ParameterBindingContext bindingContext(MongoPersistentProperty property, Object source, SpELContext spELContext) {

		ValueProvider valueProvider = valueProviderFor(DocumentReferenceSource.getTargetSource(source));

		return new ParameterBindingContext(valueProvider, spELContext.getParser(),
				() -> evaluationContextFor(property, source, spELContext));
	}

	ValueProvider valueProviderFor(Object source) {

		return index -> {
			if (source instanceof Document document) {
				return Streamable.of(document.values()).toList().get(index);
			}
			return source;
		};
	}

	EvaluationContext evaluationContextFor(MongoPersistentProperty property, Object source, SpELContext spELContext) {

		Object target = source instanceof DocumentReferenceSource documentReferenceSource ? documentReferenceSource.getTargetSource()
				: source;

		if (target == null) {
			target = new Document();
		}

		EvaluationContext ctx = spELContext.getEvaluationContext(target);
		ctx.setVariable("target", target);
		ctx.setVariable("self", DocumentReferenceSource.getSelf(source));
		ctx.setVariable(property.getName(), target);

		return ctx;
	}

	/**
	 * Compute the query to retrieve linked documents.
	 *
	 * @param property must not be {@literal null}.
	 * @param source must not be {@literal null}.
	 * @param spELContext must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	DocumentReferenceQuery computeFilter(MongoPersistentProperty property, Object source, SpELContext spELContext) {

		DocumentReference documentReference = property.isDocumentReference() ? property.getDocumentReference()
				: ReferenceEmulatingDocumentReference.INSTANCE;

		String lookup = documentReference.lookup();

		Object value = DocumentReferenceSource.getTargetSource(source);

		Document sort = parseValueOrGet(documentReference.sort(), bindingContext(property, source, spELContext),
				Document::new);

		if (property.isCollectionLike() && (value instanceof Collection || value == null)) {

			if (value == null) {
				return new ListDocumentReferenceQuery(codec.decode(lookup, bindingContext(property, source, spELContext)),
						sort);
			}

			Collection<Object> objects = (Collection<Object>) value;

			if (objects.isEmpty()) {
				return new ListDocumentReferenceQuery(NO_RESULTS_PREDICATE, sort);
			}

			List<Document> ors = new ArrayList<>(objects.size());
			for (Object entry : objects) {

				Document decoded = codec.decode(lookup, bindingContext(property, entry, spELContext));
				ors.add(decoded);
			}

			return new ListDocumentReferenceQuery(new Document("$or", ors), sort);
		}

		if (property.isMap() && value instanceof Map) {

			Set<Entry<Object, Object>> entries = ((Map<Object, Object>) value).entrySet();
			if (entries.isEmpty()) {
				return new MapDocumentReferenceQuery(NO_RESULTS_PREDICATE, sort, Collections.emptyMap());
			}

			Map<Object, Document> filterMap = new LinkedHashMap<>(entries.size());

			for (Entry<Object, Object> entry : entries) {

				Document decoded = codec.decode(lookup, bindingContext(property, entry.getValue(), spELContext));
				filterMap.put(entry.getKey(), decoded);
			}

			return new MapDocumentReferenceQuery(new Document("$or", filterMap.values()), sort, filterMap);
		}

		return new SingleDocumentReferenceQuery(codec.decode(lookup, bindingContext(property, source, spELContext)), sort);
	}

	enum ReferenceEmulatingDocumentReference implements DocumentReference {

		INSTANCE;

		@Override
		public Class<? extends Annotation> annotationType() {
			return DocumentReference.class;
		}

		@Override
		public String db() {
			return "";
		}

		@Override
		public String collection() {
			return "";
		}

		@Override
		public String lookup() {
			return "{ '_id' : ?#{#target} }";
		}

		@Override
		public String sort() {
			return "";
		}

		@Override
		public boolean lazy() {
			return false;
		}
	}

	/**
	 * {@link DocumentReferenceQuery} implementation fetching a single {@link Document}.
	 */
	static class SingleDocumentReferenceQuery implements DocumentReferenceQuery {

		private final Document query;
		private final Document sort;

		public SingleDocumentReferenceQuery(Document query, Document sort) {

			this.query = query;
			this.sort = sort;
		}

		@Override
		public Bson getQuery() {
			return query;
		}

		@Override
		public Document getSort() {
			return sort;
		}

		@Override
		public Iterable<Document> apply(MongoCollection<Document> collection) {

			Document result = collection.find(getQuery()).sort(getSort()).limit(1).first();
			return result != null ? Collections.singleton(result) : Collections.emptyList();
		}
	}

	/**
	 * {@link DocumentReferenceQuery} implementation to retrieve linked {@link Document documents} stored inside a
	 * {@link Map} structure. Restores the original map order by matching individual query documents against the actual
	 * values.
	 */
	static class MapDocumentReferenceQuery implements DocumentReferenceQuery {

		private final Document query;
		private final Document sort;
		private final Map<Object, Document> filterOrderMap;

		public MapDocumentReferenceQuery(Document query, Document sort, Map<Object, Document> filterOrderMap) {

			this.query = query;
			this.sort = sort;
			this.filterOrderMap = filterOrderMap;
		}

		@Override
		public Bson getQuery() {
			return query;
		}

		@Override
		public Bson getSort() {
			return sort;
		}

		@Override
		public Iterable<Document> restoreOrder(Iterable<Document> documents) {

			Map<String, Object> targetMap = new LinkedHashMap<>();
			List<Document> collected = documents instanceof List<Document> list ? list
					: Streamable.of(documents).toList();

			for (Entry<Object, Document> filterMapping : filterOrderMap.entrySet()) {

				Optional<Document> first = collected.stream()
						.filter(it -> it.entrySet().containsAll(filterMapping.getValue().entrySet())).findFirst();

				targetMap.put(filterMapping.getKey().toString(), first.orElse(null));
			}
			return Collections.singleton(new Document(targetMap));
		}
	}

	/**
	 * {@link DocumentReferenceQuery} implementation to retrieve linked {@link Document documents} stored inside a
	 * {@link Collection} like structure. Restores the original order by matching individual query documents against the
	 * actual values.
	 */
	static class ListDocumentReferenceQuery implements DocumentReferenceQuery {

		private final Document query;
		private final Document sort;

		public ListDocumentReferenceQuery(Document query, Document sort) {

			this.query = query;
			this.sort = sort;
		}

		@Override
		public Iterable<Document> restoreOrder(Iterable<Document> documents) {

			List<Document> target = documents instanceof List<Document> list ? list
					: Streamable.of(documents).toList();

			if (!sort.isEmpty() || !query.containsKey("$or")) {
				return target;
			}

			List<Document> ors = query.get("$or", List.class);
			return target.stream().sorted((o1, o2) -> compareAgainstReferenceIndex(ors, o1, o2)).collect(Collectors.toList());
		}

		public Document getQuery() {
			return query;
		}

		@Override
		public Document getSort() {
			return sort;
		}

		int compareAgainstReferenceIndex(List<Document> referenceList, Document document1, Document document2) {

			for (Document document : referenceList) {

				Set<Entry<String, Object>> entries = document.entrySet();
				if (document1.entrySet().containsAll(entries)) {
					return -1;
				}
				if (document2.entrySet().containsAll(entries)) {
					return 1;
				}
			}
			return referenceList.size();
		}
	}

	/**
	 * The function that can execute a given {@link DocumentReferenceQuery} within the {@link ReferenceCollection} to
	 * obtain raw results.
	 */
	@FunctionalInterface
	interface LookupFunction {

		/**
		 * @param referenceQuery never {@literal null}.
		 * @param referenceCollection never {@literal null}.
		 * @return never {@literal null}.
		 */
		Iterable<Document> apply(DocumentReferenceQuery referenceQuery, ReferenceCollection referenceCollection);
	}
}

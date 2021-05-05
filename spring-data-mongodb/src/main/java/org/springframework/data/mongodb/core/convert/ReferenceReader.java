/*
 * Copyright 2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.springframework.data.mongodb.core.convert.ReferenceResolver.LookupFunction;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.ReferenceCollection;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.ResultConversionFunction;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.mongodb.util.json.ValueProvider;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.Streamable;
import org.springframework.expression.EvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;

/**
 * @author Christoph Strobl
 */
public class ReferenceReader {

	private final Lazy<MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty>> mappingContext;
	private final Supplier<SpELContext> spelContextSupplier;
	private final ParameterBindingDocumentCodec codec;

	public ReferenceReader(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			Supplier<SpELContext> spelContextSupplier) {

		this(() -> mappingContext, spelContextSupplier);
	}

	public ReferenceReader(
			Supplier<MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty>> mappingContextSupplier,
			Supplier<SpELContext> spelContextSupplier) {

		this.mappingContext = Lazy.of(mappingContextSupplier);
		this.spelContextSupplier = spelContextSupplier;
		this.codec = new ParameterBindingDocumentCodec();
	}

	Object readReference(MongoPersistentProperty property, Object value, LookupFunction lookupFunction,
			ResultConversionFunction resultConversionFunction) {

		SpELContext spELContext = spelContextSupplier.get();

		DocumentReferenceQuery filter = computeFilter(property, value, spELContext);
		ReferenceCollection referenceCollection = computeReferenceContext(property, value, spELContext);

		Iterable<Document> result = lookupFunction.apply(filter, referenceCollection);

		if (!result.iterator().hasNext()) {
			return null;
		}

		if (property.isCollectionLike()) {
			return resultConversionFunction.apply(result, property.getTypeInformation());
		}

		return resultConversionFunction.apply(result.iterator().next(), property.getTypeInformation());
	}

	private ReferenceCollection computeReferenceContext(MongoPersistentProperty property, Object value,
			SpELContext spELContext) {

		if (value instanceof Iterable) {
			value = ((Iterable<?>) value).iterator().next();
		}

		if (value instanceof DBRef) {
			return ReferenceCollection.fromDBRef((DBRef) value);
		}

		if (value instanceof Document) {

			Document ref = (Document) value;

			if (property.isDocumentReference()) {

				ParameterBindingContext bindingContext = bindingContext(property, value, spELContext);
				DocumentReference documentReference = property.getDocumentReference();

				String targetDatabase = parseValueOrGet(documentReference.db(), bindingContext,
						() -> ref.get("db", String.class));
				String targetCollection = parseValueOrGet(documentReference.collection(), bindingContext,
						() -> ref.get("collection",
								mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection()));
				return new ReferenceCollection(targetDatabase, targetCollection);
			}

			return new ReferenceCollection(ref.getString("db"), ref.get("collection",
					mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection()));
		}

		if (property.isDocumentReference()) {

			ParameterBindingContext bindingContext = bindingContext(property, value, spELContext);
			DocumentReference documentReference = property.getDocumentReference();

			String targetDatabase = parseValueOrGet(documentReference.db(), bindingContext, () -> null);
			String targetCollection = parseValueOrGet(documentReference.collection(), bindingContext,
					() -> mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection());

			return new ReferenceCollection(targetDatabase, targetCollection);
		}

		return new ReferenceCollection(null,
				mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection());
	}

	@Nullable
	private <T> T parseValueOrGet(String value, ParameterBindingContext bindingContext, Supplier<T> defaultValue) {

		if (!StringUtils.hasText(value)) {
			return defaultValue.get();
		}

		if (!BsonUtils.isJsonDocument(value) && value.contains("?#{")) {
			String s = "{ 'target-value' : " + value + "}";
			T evaluated = (T) codec.decode(s, bindingContext).get("target-value ");
			return evaluated != null ? evaluated : defaultValue.get();
		}

		T evaluated = (T) bindingContext.evaluateExpression(value);
		return evaluated != null ? evaluated : defaultValue.get();
	}

	ParameterBindingContext bindingContext(MongoPersistentProperty property, Object source, SpELContext spELContext) {

		return new ParameterBindingContext(valueProviderFor(source), spELContext.getParser(),
				() -> evaluationContextFor(property, source, spELContext));
	}

	ValueProvider valueProviderFor(Object source) {
		return (index) -> {

			if (source instanceof Document) {
				return Streamable.of(((Document) source).values()).toList().get(index);
			}
			return source;
		};
	}

	EvaluationContext evaluationContextFor(MongoPersistentProperty property, Object source, SpELContext spELContext) {

		EvaluationContext ctx = spELContext.getEvaluationContext(source);
		ctx.setVariable("target", source);
		ctx.setVariable(property.getName(), source);

		return ctx;
	}

	DocumentReferenceQuery computeFilter(MongoPersistentProperty property, Object value, SpELContext spELContext) {

		DocumentReference documentReference = property.getDocumentReference();
		String lookup = documentReference.lookup();

		Document sort = parseValueOrGet(documentReference.sort(), bindingContext(property, value, spELContext), () -> null);

		if (property.isCollectionLike() && value instanceof Collection) {

			List<Document> ors = new ArrayList<>();
			for (Object entry : (Collection) value) {

				Document decoded = codec.decode(lookup, bindingContext(property, entry, spELContext));
				ors.add(decoded);
			}

			return new ListDocumentReferenceQuery(new Document("$or", ors), sort);
		}

		if (property.isMap() && value instanceof Map) {

			Map<Object, Document> filterMap = new LinkedHashMap<>();

			for (Entry entry : ((Map<Object, Object>) value).entrySet()) {

				Document decoded = codec.decode(lookup, bindingContext(property, entry.getValue(), spELContext));
				filterMap.put(entry.getKey(), decoded);
			}

			return new MapDocumentReferenceQuery(new Document("$or", filterMap.values()), sort, filterMap);
		}

		return new SingleDocumentReferenceQuery(codec.decode(lookup, bindingContext(property, value, spELContext)), sort);
	}

	static class SingleDocumentReferenceQuery implements DocumentReferenceQuery {

		Document filter;
		Document sort;

		public SingleDocumentReferenceQuery(Document filter, Document sort) {
			this.filter = filter;
			this.sort = sort;
		}

		@Override
		public Bson getFilter() {
			return filter;
		}

		@Override
		public Iterable<Document> apply(MongoCollection<Document> collection) {

			Document result = collection.find(getFilter()).limit(1).first();
			return result != null ? Collections.singleton(result) : Collections.emptyList();
		}
	}

	static class MapDocumentReferenceQuery implements DocumentReferenceQuery {

		private final Document filter;
		private final Document sort;
		private final Map<Object, Document> filterOrderMap;

		public MapDocumentReferenceQuery(Document filter, Document sort, Map<Object, Document> filterOrderMap) {

			this.filter = filter;
			this.sort = sort;
			this.filterOrderMap = filterOrderMap;
		}

		@Override
		public Bson getFilter() {
			return filter;
		}

		@Override
		public Bson getSort() {
			return sort;
		}

		@Override
		public Iterable<Document> restoreOrder(Iterable<Document> documents) {

			Map<String, Object> targetMap = new LinkedHashMap<>();
			List<Document> collected = documents instanceof List ? (List<Document>) documents
					: Streamable.of(documents).toList();

			for (Entry<Object, Document> filterMapping : filterOrderMap.entrySet()) {

				Optional<Document> first = collected.stream()
						.filter(it -> it.entrySet().containsAll(filterMapping.getValue().entrySet())).findFirst();

				targetMap.put(filterMapping.getKey().toString(), first.orElse(null));
			}
			return Collections.singleton(new Document(targetMap));
		}
	}

	static class ListDocumentReferenceQuery implements DocumentReferenceQuery {

		private final Document filter;
		private final Document sort;

		public ListDocumentReferenceQuery(Document filter, Document sort) {

			this.filter = filter;
			this.sort = sort;
		}

		@Override
		public Iterable<Document> restoreOrder(Iterable<Document> documents) {

			if (filter.containsKey("$or")) {
				List<Document> ors = filter.get("$or", List.class);
				List<Document> target = documents instanceof List ? (List<Document>) documents
						: Streamable.of(documents).toList();
				return target.stream().sorted((o1, o2) -> compareAgainstReferenceIndex(ors, o1, o2))
						.collect(Collectors.toList());
			}

			return documents;
		}

		public Document getFilter() {
			return filter;
		}

		@Override
		public Document getSort() {
			return sort;
		}

		int compareAgainstReferenceIndex(List<Document> referenceList, Document document1, Document document2) {

			for (int i = 0; i < referenceList.size(); i++) {

				Set<Entry<String, Object>> entries = referenceList.get(i).entrySet();
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
}

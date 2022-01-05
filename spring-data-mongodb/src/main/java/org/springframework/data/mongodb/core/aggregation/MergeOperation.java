/*
 * Copyright 2020-2021 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Encapsulates the {@code $merge}-operation.
 * <p>
 * We recommend to use the {@link MergeOperationBuilder builder} via {@link MergeOperation#builder()} instead of
 * creating instances of this class directly.
 *
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/merge/">MongoDB Documentation</a>
 * @author Christoph Strobl
 * @since 3.0
 */
public class MergeOperation implements FieldsExposingAggregationOperation, InheritsFieldsAggregationOperation {

	private final MergeOperationTarget into;
	private final UniqueMergeId on;
	private final @Nullable Let let;
	private final @Nullable WhenDocumentsMatch whenMatched;
	private final @Nullable WhenDocumentsDontMatch whenNotMatched;

	/**
	 * Create new instance of {@link MergeOperation}.
	 *
	 * @param into the target (collection and database)
	 * @param on the unique identifier. Can be {@literal null}.
	 * @param let exposed variables for {@link WhenDocumentsMatch#updateWith(Aggregation)}. Can be {@literal null}.
	 * @param whenMatched behavior if a result document matches an existing one in the target collection. Can be
	 *          {@literal null}.
	 * @param whenNotMatched behavior if a result document does not match an existing one in the target collection. Can be
	 *          {@literal null}.
	 */
	public MergeOperation(MergeOperationTarget into, UniqueMergeId on, @Nullable Let let,
			@Nullable WhenDocumentsMatch whenMatched, @Nullable WhenDocumentsDontMatch whenNotMatched) {

		Assert.notNull(into, "Into must not be null! Please provide a target collection.");
		Assert.notNull(on, "On must not be null! Use UniqueMergeId.id() instead.");

		this.into = into;
		this.on = on;
		this.let = let;
		this.whenMatched = whenMatched;
		this.whenNotMatched = whenNotMatched;
	}

	/**
	 * Simplified form to apply all default options for {@code $merge} (including writing to a collection in the same
	 * database).
	 *
	 * @param collection the output collection within the same database.
	 * @return new instance of {@link MergeOperation}.
	 */
	public static MergeOperation mergeInto(String collection) {
		return builder().intoCollection(collection).build();
	}

	/**
	 * Access the {@link MergeOperationBuilder builder API} to create a new instance of {@link MergeOperation}.
	 *
	 * @return new instance of {@link MergeOperationBuilder}.
	 */
	public static MergeOperationBuilder builder() {
		return new MergeOperationBuilder();
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		if (isJustCollection()) {
			return new Document(getOperator(), into.collection);
		}

		Document $merge = new Document();
		$merge.putAll(into.toDocument(context));

		if (!on.isJustIdField()) {
			$merge.putAll(on.toDocument(context));
		}

		if (let != null) {
			$merge.append("let", let.toDocument(context).get("$let", Document.class).get("vars"));
		}

		if (whenMatched != null) {
			$merge.putAll(whenMatched.toDocument(context));
		}

		if (whenNotMatched != null) {
			$merge.putAll(whenNotMatched.toDocument(context));
		}

		return new Document(getOperator(), $merge);
	}

	@Override
	public String getOperator() {
		return "$merge";
	}

	@Override
	public ExposedFields getFields() {

		if (let == null) {
			return ExposedFields.from();
		}

		return ExposedFields.synthetic(Fields.fields(let.getVariableNames()));
	}

	@Override
	public boolean inheritsFields() {
		return true;
	}

	/**
	 * @return true if nothing more than the collection is specified.
	 */
	private boolean isJustCollection() {
		return into.isTargetingSameDatabase() && on.isJustIdField() && let == null && whenMatched == null
				&& whenNotMatched == null;
	}

	/**
	 * Value object representing the unique id used during the merge operation to identify duplicates in the target
	 * collection.
	 *
	 * @author Christoph Strobl
	 */
	public static class UniqueMergeId {

		private static final UniqueMergeId ID = new UniqueMergeId(Collections.emptyList());

		private final Collection<String> uniqueIdentifier;

		private UniqueMergeId(Collection<String> uniqueIdentifier) {
			this.uniqueIdentifier = uniqueIdentifier;
		}

		public static UniqueMergeId ofIdFields(String... fields) {

			Assert.noNullElements(fields, "Fields must not contain null values!");

			if (ObjectUtils.isEmpty(fields)) {
				return id();
			}

			return new UniqueMergeId(Arrays.asList(fields));
		}

		/**
		 * Merge Documents by using the MongoDB {@literal _id} field.
		 *
		 * @return never {@literal null}.
		 */
		public static UniqueMergeId id() {
			return ID;
		}

		boolean isJustIdField() {
			return this.equals(ID);
		}

		Document toDocument(AggregationOperationContext context) {

			List<String> mappedOn = uniqueIdentifier.stream().map(context::getReference).map(FieldReference::getRaw)
					.collect(Collectors.toList());
			return new Document("on", mappedOn.size() == 1 ? mappedOn.iterator().next() : mappedOn);
		}
	}

	/**
	 * Value Object representing the {@code into} field of a {@code $merge} aggregation stage. <br />
	 * If not stated explicitly via {@link MergeOperationTarget#inDatabase(String)} the {@literal collection} is created
	 * in the very same {@literal database}. In this case {@code into} is just a single String holding the collection
	 * name. <br />
	 *
	 * <pre class="code">
	 *     into: "target-collection-name"
	 * </pre>
	 *
	 * If the collection needs to be in a different database {@code into} will be a {@link Document} like the following
	 *
	 * <pre class="code">
	 * {
	 * 	into: {}
	 * }
	 * </pre>
	 *
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	public static class MergeOperationTarget {

		private final @Nullable String database;
		private final String collection;

		private MergeOperationTarget(@Nullable String database, String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");

			this.database = database;
			this.collection = collection;
		}

		/**
		 * @param collection The output collection results will be stored in. Must not be {@literal null}.
		 * @return new instance of {@link MergeOperationTarget}.
		 */
		public static MergeOperationTarget collection(String collection) {
			return new MergeOperationTarget(null, collection);
		}

		/**
		 * Optionally specify the target database if different from the source one.
		 *
		 * @param database must not be {@literal null}.
		 * @return new instance of {@link MergeOperationTarget}.
		 */
		public MergeOperationTarget inDatabase(String database) {
			return new MergeOperationTarget(database, collection);
		}

		boolean isTargetingSameDatabase() {
			return !StringUtils.hasText(database);
		}

		Document toDocument(AggregationOperationContext context) {

			return new Document("into",
					!StringUtils.hasText(database) ? collection : new Document("db", database).append("coll", collection));
		}
	}

	/**
	 * Value Object specifying how to deal with a result document that matches an existing document in the collection
	 * based on the fields of the {@code on} property describing the unique identifier.
	 *
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	public static class WhenDocumentsMatch {

		private final Object value;

		private WhenDocumentsMatch(Object value) {
			this.value = value;
		}

		public static WhenDocumentsMatch whenMatchedOf(String value) {
			return new WhenDocumentsMatch(value);
		}

		/**
		 * Replace the existing document in the output collection with the matching results document.
		 *
		 * @return new instance of {@link WhenDocumentsMatch}.
		 */
		public static WhenDocumentsMatch replaceDocument() {
			return whenMatchedOf("replace");
		}

		/**
		 * Keep the existing document in the output collection.
		 *
		 * @return new instance of {@link WhenDocumentsMatch}.
		 */
		public static WhenDocumentsMatch keepExistingDocument() {
			return whenMatchedOf("keepExisting");
		}

		/**
		 * Merge the matching documents. Please see the MongoDB reference documentation for details.
		 *
		 * @return new instance of {@link WhenDocumentsMatch}.
		 */
		public static WhenDocumentsMatch mergeDocuments() {
			return whenMatchedOf("merge");
		}

		/**
		 * Stop and fail the aggregation operation. Does not revert already performed changes on previous documents.
		 *
		 * @return new instance of {@link WhenDocumentsMatch}.
		 */
		public static WhenDocumentsMatch failOnMatch() {
			return whenMatchedOf("fail");
		}

		/**
		 * Use an {@link Aggregation} to update the document in the collection. Please see the MongoDB reference
		 * documentation for details.
		 *
		 * @param aggregation must not be {@literal null}.
		 * @return new instance of {@link WhenDocumentsMatch}.
		 */
		public static WhenDocumentsMatch updateWith(Aggregation aggregation) {
			return new WhenDocumentsMatch(aggregation);
		}

		/**
		 * Use an aggregation pipeline to update the document in the collection. Please see the MongoDB reference
		 * documentation for details.
		 *
		 * @param aggregationPipeline must not be {@literal null}.
		 * @return new instance of {@link WhenDocumentsMatch}.
		 */
		public static WhenDocumentsMatch updateWith(List<AggregationOperation> aggregationPipeline) {
			return new WhenDocumentsMatch(aggregationPipeline);
		}

		Document toDocument(AggregationOperationContext context) {

			if (value instanceof Aggregation) {
				return new Document("whenMatched", ((Aggregation) value).toPipeline(context));
			}

			return new Document("whenMatched", value);
		}
	}

	/**
	 * Value Object specifying how to deal with a result document that do not match an existing document in the collection
	 * based on the fields of the {@code on} property describing the unique identifier.
	 *
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	public static class WhenDocumentsDontMatch {

		private final String value;

		private WhenDocumentsDontMatch(String value) {

			Assert.notNull(value, "Value must not be null!");

			this.value = value;
		}

		/**
		 * Factory method creating {@link WhenDocumentsDontMatch} from a {@code value} literal.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link WhenDocumentsDontMatch}.
		 */
		public static WhenDocumentsDontMatch whenNotMatchedOf(String value) {
			return new WhenDocumentsDontMatch(value);
		}

		/**
		 * Insert the document into the output collection.
		 *
		 * @return new instance of {@link WhenDocumentsDontMatch}.
		 */
		public static WhenDocumentsDontMatch insertNewDocument() {
			return whenNotMatchedOf("insert");
		}

		/**
		 * Discard the document - do not insert the document into the output collection.
		 *
		 * @return new instance of {@link WhenDocumentsDontMatch}.
		 */
		public static WhenDocumentsDontMatch discardDocument() {
			return whenNotMatchedOf("discard");
		}

		/**
		 * Stop and fail the aggregation operation. Does not revert already performed changes on previous documents.
		 *
		 * @return new instance of {@link WhenDocumentsDontMatch}.
		 */
		public static WhenDocumentsDontMatch failWhenNotMatch() {
			return whenNotMatchedOf("fail");
		}

		public Document toDocument(AggregationOperationContext context) {
			return new Document("whenNotMatched", value);
		}
	}

	/**
	 * Builder API to construct a {@link MergeOperation}.
	 *
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	public static class MergeOperationBuilder {

		private String collection;
		private @Nullable String database;
		private UniqueMergeId id = UniqueMergeId.id();
		private @Nullable Let let;
		private @Nullable WhenDocumentsMatch whenMatched;
		private @Nullable WhenDocumentsDontMatch whenNotMatched;

		public MergeOperationBuilder() {}

		/**
		 * Required output collection name to store results to.
		 *
		 * @param collection must not be {@literal null} nor empty.
		 * @return this.
		 */
		public MergeOperationBuilder intoCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");

			this.collection = collection;
			return this;
		}

		/**
		 * Optionally define a target database if different from the current one.
		 *
		 * @param database must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder inDatabase(String database) {

			this.database = database;
			return this;
		}

		/**
		 * Define the target to store results in.
		 *
		 * @param into must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder into(MergeOperationTarget into) {

			this.database = into.database;
			this.collection = into.collection;
			return this;
		}

		/**
		 * Define the target to store results in.
		 *
		 * @param target must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder target(MergeOperationTarget target) {
			return into(target);
		}

		/**
		 * Appends a single field or multiple fields that act as a unique identifier for a document. The identifier
		 * determines if a results document matches an already existing document in the output collection. <br />
		 * The aggregation results documents must contain the field(s) specified via {@code on}, unless it's the {@code _id}
		 * field.
		 *
		 * @param fields must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder on(String... fields) {
			return id(UniqueMergeId.ofIdFields(fields));
		}

		/**
		 * Set the identifier that determines if a results document matches an already existing document in the output
		 * collection.
		 *
		 * @param id must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder id(UniqueMergeId id) {

			this.id = id;
			return this;
		}

		/**
		 * Expose the variables defined by {@link Let} to the {@link WhenDocumentsMatch#updateWith(Aggregation) update
		 * aggregation}.
		 *
		 * @param let the variable expressions
		 * @return this.
		 */
		public MergeOperationBuilder let(Let let) {

			this.let = let;
			return this;
		}

		/**
		 * Expose the variables defined by {@link Let} to the {@link WhenDocumentsMatch#updateWith(Aggregation) update
		 * aggregation}.
		 *
		 * @param let the variable expressions
		 * @return this.
		 */
		public MergeOperationBuilder exposeVariablesOf(Let let) {
			return let(let);
		}

		/**
		 * The action to take place when documents already exist in the target collection.
		 *
		 * @param whenMatched must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder whenMatched(WhenDocumentsMatch whenMatched) {

			this.whenMatched = whenMatched;
			return this;
		}

		/**
		 * The action to take place when documents already exist in the target collection.
		 *
		 * @param whenMatched must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder whenDocumentsMatch(WhenDocumentsMatch whenMatched) {
			return whenMatched(whenMatched);
		}

		/**
		 * The {@link Aggregation action} to take place when documents already exist in the target collection.
		 *
		 * @param aggregation must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder whenDocumentsMatchApply(Aggregation aggregation) {
			return whenMatched(WhenDocumentsMatch.updateWith(aggregation));
		}

		/**
		 * The action to take place when documents do not already exist in the target collection.
		 *
		 * @param whenNotMatched must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder whenNotMatched(WhenDocumentsDontMatch whenNotMatched) {

			this.whenNotMatched = whenNotMatched;
			return this;
		}

		/**
		 * The action to take place when documents do not already exist in the target collection.
		 *
		 * @param whenNotMatched must not be {@literal null}.
		 * @return this.
		 */
		public MergeOperationBuilder whenDocumentsDontMatch(WhenDocumentsDontMatch whenNotMatched) {
			return whenNotMatched(whenNotMatched);
		}

		/**
		 * @return new instance of {@link MergeOperation}.
		 */
		public MergeOperation build() {
			return new MergeOperation(new MergeOperationTarget(database, collection), id, let, whenMatched, whenNotMatched);
		}
	}
}

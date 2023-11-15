/*
 * Copyright 2013-2023 the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.Fields.*;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.AddFieldsOperation.AddFieldsOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.CountOperation.CountOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.FacetOperation.FacetOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.GraphLookupOperation.StartWithBuilder;
import org.springframework.data.mongodb.core.aggregation.LookupOperation.LookupOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.MergeOperation.MergeOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplaceRootDocumentOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplaceRootOperationBuilder;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.util.Assert;

/**
 * An {@code Aggregation} is a representation of a list of aggregation steps to be performed by the MongoDB Aggregation
 * Framework.
 *
 * @author Tobias Trelle
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Alessio Fachechi
 * @author Christoph Strobl
 * @author Nikolay Bogdanov
 * @author Gustavo de Geus
 * @author Jérôme Guyon
 * @author Sangyong Choi
 * @since 1.3
 */
public class Aggregation {

	/**
	 * References the root document, i.e. the top-level document, currently being processed in the aggregation pipeline
	 * stage.
	 */
	public static final String ROOT = SystemVariable.ROOT.toString();

	/**
	 * References the start of the field path being processed in the aggregation pipeline stage. Unless documented
	 * otherwise, all stages start with CURRENT the same as ROOT.
	 */
	public static final String CURRENT = SystemVariable.CURRENT.toString();

	/**
	 * A variable to conditionally exclude a field. In a {@code $projection}, a field set to the variable
	 * {@literal REMOVE} is excluded from the output.
	 *
	 * <pre>
	 * <code>
	 *
	 * db.books.aggregate( [
	 * {
	 *     $project: {
	 *         title: 1,
	 *         "author.first": 1,
	 *         "author.last" : 1,
	 *         "author.middle": {
	 *             $cond: {
	 *                 if: { $eq: [ "", "$author.middle" ] },
	 *                 then: "$$REMOVE",
	 *                 else: "$author.middle"
	 *             }
	 *         }
	 *     }
	 * } ] )
	 * </code>
	 * </pre>
	 */
	public static final String REMOVE = SystemVariable.REMOVE.toString();

	public static final AggregationOperationContext DEFAULT_CONTEXT = AggregationOperationRenderer.DEFAULT_CONTEXT;
	public static final AggregationOptions DEFAULT_OPTIONS = newAggregationOptions().build();

	protected final AggregationPipeline pipeline;
	private final AggregationOptions options;

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 *
	 * @param operations must not be {@literal null} or empty.
	 */
	public static Aggregation newAggregation(List<? extends AggregationOperation> operations) {
		return newAggregation(operations.toArray(new AggregationOperation[operations.size()]));
	}

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 *
	 * @param operations must not be {@literal null} or empty.
	 */
	public static Aggregation newAggregation(AggregationOperation... operations) {
		return new Aggregation(operations);
	}

	/**
	 * Creates a new {@link AggregationUpdate} from the given {@link AggregationOperation}s.
	 *
	 * @param operations can be {@literal empty} but must not be {@literal null}.
	 * @return new instance of {@link AggregationUpdate}.
	 * @since 3.0
	 */
	public static AggregationUpdate newUpdate(AggregationOperation... operations) {
		return AggregationUpdate.from(Arrays.asList(operations));
	}

	/**
	 * Returns a copy of this {@link Aggregation} with the given {@link AggregationOptions} set. Note that options are
	 * supported in MongoDB version 2.6+.
	 *
	 * @param options must not be {@literal null}.
	 * @return new instance of {@link Aggregation}.
	 * @since 1.6
	 */
	public Aggregation withOptions(AggregationOptions options) {

		Assert.notNull(options, "AggregationOptions must not be null");
		return new Aggregation(this.pipeline.getOperations(), options);
	}

	/**
	 * Creates a new {@link TypedAggregation} for the given type and {@link AggregationOperation}s.
	 *
	 * @param type must not be {@literal null}.
	 * @param operations must not be {@literal null} or empty.
	 */
	public static <T> TypedAggregation<T> newAggregation(Class<T> type, List<? extends AggregationOperation> operations) {
		return newAggregation(type, operations.toArray(new AggregationOperation[operations.size()]));
	}

	/**
	 * Creates a new {@link TypedAggregation} for the given type and {@link AggregationOperation}s.
	 *
	 * @param type must not be {@literal null}.
	 * @param operations must not be {@literal null} or empty.
	 */
	public static <T> TypedAggregation<T> newAggregation(Class<T> type, AggregationOperation... operations) {
		return new TypedAggregation<T>(type, operations);
	}

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 *
	 * @param aggregationOperations must not be {@literal null} or empty.
	 */
	protected Aggregation(AggregationOperation... aggregationOperations) {
		this(asAggregationList(aggregationOperations));
	}

	/**
	 * @param aggregationOperations must not be {@literal null} or empty.
	 * @return
	 */
	protected static List<AggregationOperation> asAggregationList(AggregationOperation... aggregationOperations) {

		Assert.notEmpty(aggregationOperations, "AggregationOperations must not be null or empty");

		return Arrays.asList(aggregationOperations);
	}

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 *
	 * @param aggregationOperations must not be {@literal null} or empty.
	 */
	protected Aggregation(List<AggregationOperation> aggregationOperations) {
		this(aggregationOperations, DEFAULT_OPTIONS);
	}

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 *
	 * @param aggregationOperations must not be {@literal null}.
	 * @param options must not be {@literal null} or empty.
	 */
	protected Aggregation(List<AggregationOperation> aggregationOperations, AggregationOptions options) {

		Assert.notNull(aggregationOperations, "AggregationOperations must not be null");
		Assert.notNull(options, "AggregationOptions must not be null");

		this.pipeline = new AggregationPipeline(aggregationOperations);
		this.options = options;
	}

	/**
	 * Get the {@link AggregationOptions}.
	 *
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	public AggregationOptions getOptions() {
		return options;
	}

	/**
	 * A pointer to the previous {@link AggregationOperation}.
	 *
	 * @return
	 */
	public static String previousOperation() {
		return FieldName.ID.name();
	}

	/**
	 * Obtain an {@link AddFieldsOperationBuilder builder} instance to create a new {@link AddFieldsOperation}. <br />
	 * Starting in version 4.2, MongoDB adds a new aggregation pipeline stage {@link AggregationUpdate#set $set} that is
	 * an alias for {@code $addFields}.
	 *
	 * @return new instance of {@link AddFieldsOperationBuilder}.
	 * @see AddFieldsOperation
	 * @since 3.0
	 */
	public static AddFieldsOperationBuilder addFields() {
		return AddFieldsOperation.builder();
	}

	/**
	 * Creates a new {@link AggregationOperation} taking the given {@link Bson bson value} as is. <br />
	 *
	 * <pre class="code">
	 * Aggregation.stage(Aggregates.search(exists(fieldPath("..."))));
	 * </pre>
	 *
	 * Field mapping against a potential domain type or previous aggregation stages will not happen.
	 *
	 * @param aggregationOperation the must not be {@literal null}.
	 * @return new instance of {@link AggregationOperation}.
	 * @since 4.0
	 */
	public static AggregationOperation stage(Bson aggregationOperation) {
		return new BasicAggregationOperation(aggregationOperation);
	}

	/**
	 * Creates a new {@link AggregationOperation} taking the given {@link String json value} as is. <br />
	 *
	 * <pre class="code">
	 * Aggregation.stage("{ $search : { near : { path : 'released' , origin : ... } } }");
	 * </pre>
	 *
	 * Field mapping against a potential domain type or previous aggregation stages will not happen.
	 *
	 * @param json the JSON representation of the pipeline stage. Must not be {@literal null}.
	 * @return new instance of {@link AggregationOperation}.
	 * @since 4.0
	 */
	public static AggregationOperation stage(String json) {
		return new BasicAggregationOperation(json);
	}

	/**
	 * Creates a new {@link ProjectionOperation} including the given fields.
	 *
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link ProjectionOperation}.
	 */
	public static ProjectionOperation project(String... fields) {
		return project(fields(fields));
	}

	/**
	 * Creates a new {@link ProjectionOperation} including the given {@link Fields}.
	 *
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link ProjectionOperation}.
	 */
	public static ProjectionOperation project(Fields fields) {
		return new ProjectionOperation(fields);
	}

	/**
	 * Creates a new {@link ProjectionOperation} including all top level fields of the given given {@link Class}.
	 *
	 * @param type must not be {@literal null}.
	 * @return new instance of {@link ProjectionOperation}.
	 * @since 2.2
	 */
	public static ProjectionOperation project(Class<?> type) {

		Assert.notNull(type, "Type must not be null");
		return new ProjectionOperation(type);
	}

	/**
	 * Factory method to create a new {@link UnwindOperation} for the field with the given name.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @return new instance of {@link UnwindOperation}.
	 */
	public static UnwindOperation unwind(String field) {
		return new UnwindOperation(field(field));
	}

	/**
	 * Factory method to create a new {@link ReplaceRootOperation} for the field with the given name.
	 *
	 * @param fieldName must not be {@literal null} or empty.
	 * @return new instance of {@link ReplaceRootOperation}.
	 * @since 1.10
	 */
	public static ReplaceRootOperation replaceRoot(String fieldName) {
		return ReplaceRootOperation.builder().withValueOf(fieldName);
	}

	/**
	 * Factory method to create a new {@link ReplaceRootOperation} for the field with the given
	 * {@link AggregationExpression}.
	 *
	 * @param aggregationExpression must not be {@literal null}.
	 * @return new instance of {@link ReplaceRootOperation}.
	 * @since 1.10
	 */
	public static ReplaceRootOperation replaceRoot(AggregationExpression aggregationExpression) {
		return ReplaceRootOperation.builder().withValueOf(aggregationExpression);
	}

	/**
	 * Factory method to create a new {@link ReplaceRootDocumentOperationBuilder} to configure a
	 * {@link ReplaceRootOperation}.
	 *
	 * @return the {@literal ReplaceRootDocumentOperationBuilder}.
	 * @since 1.10
	 */
	public static ReplaceRootOperationBuilder replaceRoot() {
		return ReplaceRootOperation.builder();
	}

	/**
	 * Factory method to create a new {@link UnwindOperation} for the field with the given name and
	 * {@code preserveNullAndEmptyArrays}. Note that extended unwind is supported in MongoDB version 3.2+.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @param preserveNullAndEmptyArrays {@literal true} to output the document if path is {@literal null}, missing or
	 *          array is empty.
	 * @return new {@link UnwindOperation}
	 * @since 1.10
	 */
	public static UnwindOperation unwind(String field, boolean preserveNullAndEmptyArrays) {
		return new UnwindOperation(field(field), preserveNullAndEmptyArrays);
	}

	/**
	 * Factory method to create a new {@link UnwindOperation} for the field with the given name including the name of a
	 * new field to hold the array index of the element as {@code arrayIndex}. Note that extended unwind is supported in
	 * MongoDB version 3.2+.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @param arrayIndex must not be {@literal null} or empty.
	 * @return new {@link UnwindOperation}
	 * @since 1.10
	 */
	public static UnwindOperation unwind(String field, String arrayIndex) {
		return new UnwindOperation(field(field), field(arrayIndex), false);
	}

	/**
	 * Factory method to create a new {@link UnwindOperation} for the field with the given name, including the name of a new
	 * field to hold the array index of the element as {@code arrayIndex} using {@code preserveNullAndEmptyArrays}. Note
	 * that extended unwind is supported in MongoDB version 3.2+.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @param arrayIndex must not be {@literal null} or empty.
	 * @param preserveNullAndEmptyArrays {@literal true} to output the document if path is {@literal null}, missing or
	 *          array is empty.
	 * @return new {@link UnwindOperation}
	 * @since 1.10
	 */
	public static UnwindOperation unwind(String field, String arrayIndex, boolean preserveNullAndEmptyArrays) {
		return new UnwindOperation(field(field), field(arrayIndex), preserveNullAndEmptyArrays);
	}

	/**
	 * Creates a new {@link GroupOperation} for the given fields.
	 *
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link GroupOperation}.
	 */
	public static GroupOperation group(String... fields) {
		return group(fields(fields));
	}

	/**
	 * Creates a new {@link GroupOperation} for the given {@link Fields}.
	 *
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static GroupOperation group(Fields fields) {
		return new GroupOperation(fields);
	}

	/**
	 * Creates a new {@link GraphLookupOperation.GraphLookupOperationFromBuilder} to construct a
	 * {@link GraphLookupOperation} given {@literal fromCollection}.
	 *
	 * @param fromCollection must not be {@literal null} or empty.
	 * @return new instance of {@link StartWithBuilder} for creating a {@link GraphLookupOperation}.
	 * @since 1.10
	 */
	public static StartWithBuilder graphLookup(String fromCollection) {
		return GraphLookupOperation.builder().from(fromCollection);
	}

	/**
	 * Factory method to create a new {@link SortOperation} for the given {@link Sort}.
	 *
	 * @param sort must not be {@literal null}.
	 * @return new instance of {@link SortOperation}.
	 */
	public static SortOperation sort(Sort sort) {
		return new SortOperation(sort);
	}

	/**
	 * Factory method to create a new {@link SortOperation} for the given sort {@link Direction} and {@code fields}.
	 *
	 * @param direction must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link SortOperation}.
	 */
	public static SortOperation sort(Direction direction, String... fields) {
		return new SortOperation(Sort.by(direction, fields));
	}

	/**
	 * Creates a new {@link SortByCountOperation} given {@literal groupByField}.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @return new instance of {@link SortByCountOperation}.
	 * @since 2.1
	 */
	public static SortByCountOperation sortByCount(String field) {
		return new SortByCountOperation(field(field));
	}

	/**
	 * Creates a new {@link SortByCountOperation} given {@link AggregationExpression group and sort expression}.
	 *
	 * @param groupAndSortExpression must not be {@literal null}.
	 * @return new instance of {@link SortByCountOperation}.
	 * @since 2.1
	 */
	public static SortByCountOperation sortByCount(AggregationExpression groupAndSortExpression) {
		return new SortByCountOperation(groupAndSortExpression);
	}

	/**
	 * Creates a new {@link SkipOperation} skipping the given number of elements.
	 *
	 * @param elementsToSkip must not be less than zero.
	 * @return new instance of {@link SkipOperation}.
	 */
	public static SkipOperation skip(long elementsToSkip) {
		return new SkipOperation(elementsToSkip);
	}

	/**
	 * Creates a new {@link LimitOperation} limiting the result to the given number of elements.
	 *
	 * @param maxElements must not be less than zero.
	 * @return new instance of {@link LimitOperation}.
	 */
	public static LimitOperation limit(long maxElements) {
		return new LimitOperation(maxElements);
	}

	/**
	 * Creates a new {@link SampleOperation} to select the specified number of documents from its input randomly.
	 *
	 * @param sampleSize must not be less than zero.
	 * @return new instance of {@link SampleOperation}.
	 * @since 2.0
	 */
	public static SampleOperation sample(long sampleSize) {
		return new SampleOperation(sampleSize);
	}

	/**
	 * Creates a new {@link MatchOperation} using the given {@link Criteria}.
	 *
	 * @param criteria must not be {@literal null}.
	 * @return new instance of {@link MatchOperation}.
	 */
	public static MatchOperation match(Criteria criteria) {
		return new MatchOperation(criteria);
	}

	/**
	 * Creates a new {@link MatchOperation} using the given {@link CriteriaDefinition}.
	 *
	 * @param criteria must not be {@literal null}.
	 * @return new instance of {@link MatchOperation}.
	 * @since 1.10
	 */
	public static MatchOperation match(CriteriaDefinition criteria) {
		return new MatchOperation(criteria);
	}

	/**
	 * Creates a new {@link MatchOperation} using the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link MatchOperation}.
	 * @since 3.3
	 */
	public static MatchOperation match(AggregationExpression expression) {
		return new MatchOperation(expression);
	}

	/**
	 * Creates a new {@link GeoNearOperation} instance from the given {@link NearQuery} and the {@code distanceField}. The
	 * {@code distanceField} defines output field that contains the calculated distance.
	 *
	 * @param query must not be {@literal null}.
	 * @param distanceField must not be {@literal null} or empty.
	 * @return new instance of {@link GeoNearOperation}.
	 * @since 1.7
	 */
	public static GeoNearOperation geoNear(NearQuery query, String distanceField) {
		return new GeoNearOperation(query, distanceField);
	}

	/**
	 * Obtain a {@link MergeOperationBuilder builder} instance to create a new {@link MergeOperation}.
	 *
	 * @return new instance of {@link MergeOperationBuilder}.
	 * @see MergeOperation
	 * @since 3.0
	 */
	public static MergeOperationBuilder merge() {
		return MergeOperation.builder();
	}

	/**
	 * Creates a new {@link OutOperation} using the given collection name. This operation must be the last operation in
	 * the pipeline.
	 *
	 * @param outCollectionName collection name to export aggregation results. The {@link OutOperation} creates a new
	 *          collection in the current database if one does not already exist. The collection is not visible until the
	 *          aggregation completes. If the aggregation fails, MongoDB does not create the collection. Must not be
	 *          {@literal null}.
	 * @return new instance of {@link OutOperation}.
	 */
	public static OutOperation out(String outCollectionName) {
		return new OutOperation(outCollectionName);
	}

	/**
	 * Creates a new {@link BucketOperation} given {@literal groupByField}.
	 *
	 * @param groupByField must not be {@literal null} or empty.
	 * @return new instance of {@link BucketOperation}.
	 * @since 1.10
	 */
	public static BucketOperation bucket(String groupByField) {
		return new BucketOperation(field(groupByField));
	}

	/**
	 * Creates a new {@link BucketOperation} given {@link AggregationExpression group-by expression}.
	 *
	 * @param groupByExpression must not be {@literal null}.
	 * @return new instance of {@link BucketOperation}.
	 * @since 1.10
	 */
	public static BucketOperation bucket(AggregationExpression groupByExpression) {
		return new BucketOperation(groupByExpression);
	}

	/**
	 * Creates a new {@link BucketAutoOperation} given {@literal groupByField}.
	 *
	 * @param groupByField must not be {@literal null} or empty.
	 * @param buckets number of buckets, must be a positive integer.
	 * @return new instance of {@link BucketAutoOperation}.
	 * @since 1.10
	 */
	public static BucketAutoOperation bucketAuto(String groupByField, int buckets) {
		return new BucketAutoOperation(field(groupByField), buckets);
	}

	/**
	 * Creates a new {@link BucketAutoOperation} given {@link AggregationExpression group-by expression}.
	 *
	 * @param groupByExpression must not be {@literal null}.
	 * @param buckets number of buckets, must be a positive integer.
	 * @return new instance of {@link BucketAutoOperation}.
	 * @since 1.10
	 */
	public static BucketAutoOperation bucketAuto(AggregationExpression groupByExpression, int buckets) {
		return new BucketAutoOperation(groupByExpression, buckets);
	}

	/**
	 * Creates a new {@link FacetOperation}.
	 *
	 * @return new instance of {@link FacetOperation}.
	 * @since 1.10
	 */
	public static FacetOperation facet() {
		return FacetOperation.EMPTY;
	}

	/**
	 * Creates a new {@link FacetOperationBuilder} given {@link Aggregation}.
	 *
	 * @param aggregationOperations the sub-pipeline, must not be {@literal null}.
	 * @return new instance of {@link FacetOperation}.
	 * @since 1.10
	 */
	public static FacetOperationBuilder facet(AggregationOperation... aggregationOperations) {
		return facet().and(aggregationOperations);
	}

	/**
	 * Creates a new {@link LookupOperation}.
	 *
	 * @param from must not be {@literal null}.
	 * @param localField must not be {@literal null}.
	 * @param foreignField must not be {@literal null}.
	 * @param as must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.9
	 */
	public static LookupOperation lookup(String from, String localField, String foreignField, String as) {
		return lookup(field(from), field(localField), field(foreignField), field(as));
	}

	/**
	 * Creates a new {@link LookupOperation} for the given {@link Fields}.
	 *
	 * @param from must not be {@literal null}.
	 * @param localField must not be {@literal null}.
	 * @param foreignField must not be {@literal null}.
	 * @param as must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.9
	 */
	public static LookupOperation lookup(Field from, Field localField, Field foreignField, Field as) {
		return new LookupOperation(from, localField, foreignField, as);
	}

	/**
	 * Entrypoint for creating {@link LookupOperation $lookup} using a fluent builder API.
	 * <pre class="code">
	 * Aggregation.lookup().from("restaurants")
	 * 	.localField("restaurant_name")
	 * 	.foreignField("name")
	 * 	.let(newVariable("orders_drink").forField("drink"))
	 * 	.pipeline(match(ctx -> new Document("$expr", new Document("$in", List.of("$$orders_drink", "$beverages")))))
	 * 	.as("matches")
	 * </pre>
	 * @return new instance of {@link LookupOperationBuilder}.
	 * @since 4.1
	 */
	public static LookupOperationBuilder lookup() {
		return new LookupOperationBuilder();
	}

	/**
	 * Creates a new {@link CountOperationBuilder}.
	 *
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public static CountOperationBuilder count() {
		return new CountOperationBuilder();
	}

	/**
	 * Creates a new {@link RedactOperation} that can restrict the content of a document based on information stored
	 * within the document itself.
	 *
	 * <pre class="code">
	 *
	 * Aggregation.redact(ConditionalOperators.when(Criteria.where("level").is(5)) //
	 * 		.then(RedactOperation.PRUNE) //
	 * 		.otherwise(RedactOperation.DESCEND));
	 * </pre>
	 *
	 * @param condition Any {@link AggregationExpression} that resolves to {@literal $$DESCEND}, {@literal $$PRUNE}, or
	 *          {@literal $$KEEP}. Must not be {@literal null}.
	 * @return new instance of {@link RedactOperation}. Never {@literal null}.
	 * @since 3.0
	 */
	public static RedactOperation redact(AggregationExpression condition) {
		return new RedactOperation(condition);
	}

	/**
	 * Creates a new {@link Fields} instance for the given field names.
	 *
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link Fields}.
	 * @see Fields#fields(String...)
	 */
	public static Fields fields(String... fields) {
		return Fields.fields(fields);
	}

	/**
	 * Creates a new {@link Fields} instance from the given field name and target reference.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param target must not be {@literal null} or empty.
	 * @return new instance of {@link Fields}.
	 */
	public static Fields bind(String name, String target) {
		return Fields.from(field(name, target));
	}

	/**
	 * Returns a new {@link AggregationOptions.Builder}.
	 *
	 * @return new instance of {@link AggregationOptions.Builder}.
	 * @since 1.6
	 */
	public static AggregationOptions.Builder newAggregationOptions() {
		return new AggregationOptions.Builder();
	}

	/**
	 * Renders this {@link Aggregation} specification to an aggregation pipeline returning a {@link List} of
	 * {@link Document}.
	 *
	 * @return the aggregation pipeline representing this aggregation.
	 * @since 2.1
	 */
	public List<Document> toPipeline(AggregationOperationContext rootContext) {
		return pipeline.toDocuments(rootContext);
	}

	/**
	 * @return the {@link AggregationPipeline}.
	 * @since 3.0.2
	 */
	public AggregationPipeline getPipeline() {
		return pipeline;
	}

	/**
	 * Converts this {@link Aggregation} specification to a {@link Document}. <br />
	 * MongoDB requires as of 3.6 cursor-based aggregation. Use {@link #toPipeline(AggregationOperationContext)} to render
	 * an aggregation pipeline.
	 *
	 * @param inputCollectionName the name of the input collection.
	 * @return the {@code Document} representing this aggregation.
	 */
	public Document toDocument(String inputCollectionName, AggregationOperationContext rootContext) {

		Document command = new Document("aggregate", inputCollectionName);
		command.put("pipeline", toPipeline(rootContext));

		return options.applyAndReturnPotentiallyChangedCommand(command);
	}

	@Override
	public String toString() {
		return SerializationUtils.serializeToJsonSafely(toDocument("__collection__", DEFAULT_CONTEXT));
	}
}

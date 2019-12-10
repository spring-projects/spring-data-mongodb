/*
 * Copyright 2013-2019 the original author or authors.
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
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.CountOperation.CountOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.FacetOperation.FacetOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.GraphLookupOperation.StartWithBuilder;
import org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplaceRootDocumentOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplaceRootOperationBuilder;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.lang.Nullable;
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

	protected final List<AggregationOperation> operations;
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
	 * @return
	 * @since 1.6
	 */
	public Aggregation withOptions(AggregationOptions options) {

		Assert.notNull(options, "AggregationOptions must not be null.");
		return new Aggregation(this.operations, options);
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

		Assert.notEmpty(aggregationOperations, "AggregationOperations must not be null or empty!");

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

		Assert.notNull(aggregationOperations, "AggregationOperations must not be null!");
		Assert.notNull(options, "AggregationOptions must not be null!");

		// check $out is the last operation if it exists
		for (AggregationOperation aggregationOperation : aggregationOperations) {
			if (aggregationOperation instanceof OutOperation && !isLast(aggregationOperation, aggregationOperations)) {
				throw new IllegalArgumentException("The $out operator must be the last stage in the pipeline.");
			}
		}

		this.operations = aggregationOperations;
		this.options = options;
	}

	private boolean isLast(AggregationOperation aggregationOperation, List<AggregationOperation> aggregationOperations) {
		return aggregationOperations.indexOf(aggregationOperation) == aggregationOperations.size() - 1;
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
		return "_id";
	}

	/**
	 * Creates a new {@link ProjectionOperation} including the given fields.
	 *
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static ProjectionOperation project(String... fields) {
		return project(fields(fields));
	}

	/**
	 * Creates a new {@link ProjectionOperation} including the given {@link Fields}.
	 *
	 * @param fields must not be {@literal null}.
	 * @return
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

		Assert.notNull(type, "Type must not be null!");
		return new ProjectionOperation(type);
	}

	/**
	 * Factory method to create a new {@link UnwindOperation} for the field with the given name.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @return
	 */
	public static UnwindOperation unwind(String field) {
		return new UnwindOperation(field(field));
	}

	/**
	 * Factory method to create a new {@link ReplaceRootOperation} for the field with the given name.
	 *
	 * @param fieldName must not be {@literal null} or empty.
	 * @return
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
	 * @return
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
	 * Factory method to create a new {@link UnwindOperation} for the field with the given nameincluding the name of a new
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
	 * @return
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
	 * @return
	 * @since 1.10
	 */
	public static StartWithBuilder graphLookup(String fromCollection) {
		return GraphLookupOperation.builder().from(fromCollection);
	}

	/**
	 * Factory method to create a new {@link SortOperation} for the given {@link Sort}.
	 *
	 * @param sort must not be {@literal null}.
	 * @return
	 */
	public static SortOperation sort(Sort sort) {
		return new SortOperation(sort);
	}

	/**
	 * Factory method to create a new {@link SortOperation} for the given sort {@link Direction} and {@code fields}.
	 *
	 * @param direction must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static SortOperation sort(Direction direction, String... fields) {
		return new SortOperation(Sort.by(direction, fields));
	}

	/**
	 * Creates a new {@link SortByCountOperation} given {@literal groupByField}.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @return
	 * @since 2.1
	 */
	public static SortByCountOperation sortByCount(String field) {
		return new SortByCountOperation(field(field));
	}

	/**
	 * Creates a new {@link SortByCountOperation} given {@link AggregationExpression group and sort expression}.
	 *
	 * @param groupAndSortExpression must not be {@literal null}.
	 * @return
	 * @since 2.1
	 */
	public static SortByCountOperation sortByCount(AggregationExpression groupAndSortExpression) {
		return new SortByCountOperation(groupAndSortExpression);
	}

	/**
	 * Creates a new {@link SkipOperation} skipping the given number of elements.
	 *
	 * @param elementsToSkip must not be less than zero.
	 * @return
	 * @deprecated prepare to get this one removed in favor of {@link #skip(long)}.
	 */
	public static SkipOperation skip(int elementsToSkip) {
		return new SkipOperation(elementsToSkip);
	}

	/**
	 * Creates a new {@link SkipOperation} skipping the given number of elements.
	 *
	 * @param elementsToSkip must not be less than zero.
	 * @return
	 */
	public static SkipOperation skip(long elementsToSkip) {
		return new SkipOperation(elementsToSkip);
	}

	/**
	 * Creates a new {@link LimitOperation} limiting the result to the given number of elements.
	 *
	 * @param maxElements must not be less than zero.
	 * @return
	 */
	public static LimitOperation limit(long maxElements) {
		return new LimitOperation(maxElements);
	}

	/**
	 * Creates a new {@link SampleOperation} to select the specified number of documents from its input randomly.
	 *
	 * @param sampleSize must not be less than zero.
	 * @return
	 * @since 2.0
	 */
	public static SampleOperation sample(long sampleSize) {
		return new SampleOperation(sampleSize);
	}

	/**
	 * Creates a new {@link MatchOperation} using the given {@link Criteria}.
	 *
	 * @param criteria must not be {@literal null}.
	 * @return
	 */
	public static MatchOperation match(Criteria criteria) {
		return new MatchOperation(criteria);
	}

	/**
	 * Creates a new {@link MatchOperation} using the given {@link CriteriaDefinition}.
	 *
	 * @param criteria must not be {@literal null}.
	 * @return
	 * @since 1.10
	 */
	public static MatchOperation match(CriteriaDefinition criteria) {
		return new MatchOperation(criteria);
	}

	/**
	 * Creates a new {@link OutOperation} using the given collection name. This operation must be the last operation in
	 * the pipeline.
	 *
	 * @param outCollectionName collection name to export aggregation results. The {@link OutOperation} creates a new
	 *          collection in the current database if one does not already exist. The collection is not visible until the
	 *          aggregation completes. If the aggregation fails, MongoDB does not create the collection. Must not be
	 *          {@literal null}.
	 * @return
	 */
	public static OutOperation out(String outCollectionName) {
		return new OutOperation(outCollectionName);
	}

	/**
	 * Creates a new {@link BucketOperation} given {@literal groupByField}.
	 *
	 * @param groupByField must not be {@literal null} or empty.
	 * @return
	 * @since 1.10
	 */
	public static BucketOperation bucket(String groupByField) {
		return new BucketOperation(field(groupByField));
	}

	/**
	 * Creates a new {@link BucketOperation} given {@link AggregationExpression group-by expression}.
	 *
	 * @param groupByExpression must not be {@literal null}.
	 * @return
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
	 * @return
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
	 * @return
	 * @since 1.10
	 */
	public static BucketAutoOperation bucketAuto(AggregationExpression groupByExpression, int buckets) {
		return new BucketAutoOperation(groupByExpression, buckets);
	}

	/**
	 * Creates a new {@link FacetOperation}.
	 *
	 * @return
	 * @since 1.10
	 */
	public static FacetOperation facet() {
		return FacetOperation.EMPTY;
	}

	/**
	 * Creates a new {@link FacetOperationBuilder} given {@link Aggregation}.
	 *
	 * @param aggregationOperations the sub-pipeline, must not be {@literal null}.
	 * @return
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
	 * Creates a new {@link CountOperationBuilder}.
	 *
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public static CountOperationBuilder count() {
		return new CountOperationBuilder();
	}

	/**
	 * Creates a new {@link Fields} instance for the given field names.
	 *
	 * @param fields must not be {@literal null}.
	 * @return
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
	 * @return
	 */
	public static Fields bind(String name, String target) {
		return Fields.from(field(name, target));
	}

	/**
	 * Creates a new {@link GeoNearOperation} instance from the given {@link NearQuery} and the {@code distanceField}. The
	 * {@code distanceField} defines output field that contains the calculated distance.
	 *
	 * @param query must not be {@literal null}.
	 * @param distanceField must not be {@literal null} or empty.
	 * @return
	 * @since 1.7
	 */
	public static GeoNearOperation geoNear(NearQuery query, String distanceField) {
		return new GeoNearOperation(query, distanceField);
	}

	/**
	 * Returns a new {@link AggregationOptions.Builder}.
	 *
	 * @return
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
		return AggregationOperationRenderer.toDocument(operations, rootContext);
	}

	/**
	 * Converts this {@link Aggregation} specification to a {@link Document}.
	 * <p/>
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return SerializationUtils.serializeToJsonSafely(toDocument("__collection__", DEFAULT_CONTEXT));
	}

	/**
	 * Describes the system variables available in MongoDB aggregation framework pipeline expressions.
	 *
	 * @author Thomas Darimont
	 * @author Christoph Strobl
	 * @see <a href="https://docs.mongodb.com/manual/reference/aggregation-variables">Aggregation Variables</a>.
	 */
	enum SystemVariable {

		ROOT, CURRENT, REMOVE;

		private static final String PREFIX = "$$";

		/**
		 * Return {@literal true} if the given {@code fieldRef} denotes a well-known system variable, {@literal false}
		 * otherwise.
		 *
		 * @param fieldRef may be {@literal null}.
		 * @return
		 */
		public static boolean isReferingToSystemVariable(@Nullable String fieldRef) {

			if (fieldRef == null || !fieldRef.startsWith(PREFIX) || fieldRef.length() <= 2) {
				return false;
			}

			int indexOfFirstDot = fieldRef.indexOf('.');
			String candidate = fieldRef.substring(2, indexOfFirstDot == -1 ? fieldRef.length() : indexOfFirstDot);

			for (SystemVariable value : values()) {
				if (value.name().equals(candidate)) {
					return true;
				}
			}

			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Enum#toString()
		 */
		@Override
		public String toString() {
			return PREFIX.concat(name());
		}
	}
}

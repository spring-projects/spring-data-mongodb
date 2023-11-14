/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.function.Supplier;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let.ExpressionVariable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Encapsulates the aggregation framework {@code $lookup}-operation. We recommend to use the builder provided via
 * {@link #newLookup()} instead of creating instances of this class directly.
 *
 * @author Alessio Fachechi
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Sangyong Choi
 * @since 1.9
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/">MongoDB Aggregation Framework:
 *      $lookup</a>
 */
public class LookupOperation implements FieldsExposingAggregationOperation, InheritsFieldsAggregationOperation {

	private final String from;

	@Nullable //
	private final Field localField;

	@Nullable //
	private final Field foreignField;

	@Nullable //
	private final Let let;

	@Nullable //
	private final AggregationPipeline pipeline;

	private final ExposedField as;

	/**
	 * Creates a new {@link LookupOperation} for the given {@link Field}s.
	 *
	 * @param from must not be {@literal null}.
	 * @param localField must not be {@literal null}.
	 * @param foreignField must not be {@literal null}.
	 * @param as must not be {@literal null}.
	 */
	public LookupOperation(Field from, Field localField, Field foreignField, Field as) {
		this(((Supplier<String>) () -> {

			Assert.notNull(from, "From must not be null");
			return from.getTarget();
		}).get(), localField, foreignField, null, null, as);
	}

	/**
	 * Creates a new {@link LookupOperation} for the given combination of {@link Field}s and {@link AggregationPipeline
	 * pipeline}.
	 *
	 * @param from must not be {@literal null}.
	 * @param let must not be {@literal null}.
	 * @param as must not be {@literal null}.
	 * @since 4.1
	 */
	public LookupOperation(String from, @Nullable Let let, AggregationPipeline pipeline, Field as) {
		this(from, null, null, let, pipeline, as);
	}

	/**
	 * Creates a new {@link LookupOperation} for the given combination of {@link Field}s and {@link AggregationPipeline
	 * pipeline}.
	 *
	 * @param from must not be {@literal null}.
	 * @param localField can be {@literal null} if {@literal pipeline} is present.
	 * @param foreignField can be {@literal null} if {@literal pipeline} is present.
	 * @param let can be {@literal null} if {@literal localField} and {@literal foreignField} are present.
	 * @param as must not be {@literal null}.
	 * @since 4.1
	 */
	public LookupOperation(String from, @Nullable Field localField, @Nullable Field foreignField, @Nullable Let let,
			@Nullable AggregationPipeline pipeline, Field as) {

		Assert.notNull(from, "From must not be null");
		if (pipeline == null) {
			Assert.notNull(localField, "LocalField must not be null");
			Assert.notNull(foreignField, "ForeignField must not be null");
		} else if (localField == null && foreignField == null) {
			Assert.notNull(pipeline, "Pipeline must not be null");
		}
		Assert.notNull(as, "As must not be null");

		this.from = from;
		this.localField = localField;
		this.foreignField = foreignField;
		this.as = new ExposedField(as, true);
		this.let = let;
		this.pipeline = pipeline;
	}

	@Override
	public ExposedFields getFields() {
		return ExposedFields.from(as);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document lookupObject = new Document();

		lookupObject.append("from", from);
		if (localField != null) {
			lookupObject.append("localField", localField.getTarget());
		}
		if (foreignField != null) {
			lookupObject.append("foreignField", foreignField.getTarget());
		}
		if (let != null) {
			lookupObject.append("let", let.toDocument(context).get("$let", Document.class).get("vars"));
		}
		if (pipeline != null) {
			lookupObject.append("pipeline", pipeline.toDocuments(context));
		}

		lookupObject.append("as", as.getTarget());

		return new Document(getOperator(), lookupObject);
	}

	@Override
	public String getOperator() {
		return "$lookup";
	}

	/**
	 * Get a builder that allows creation of {@link LookupOperation}.
	 *
	 * @return never {@literal null}.
	 */
	public static FromBuilder newLookup() {
		return new LookupOperationBuilder();
	}

	public interface FromBuilder {

		/**
		 * @param name the collection in the same database to perform the join with, must not be {@literal null} or empty.
		 * @return never {@literal null}.
		 */
		LocalFieldBuilder from(String name);
	}

	public interface LocalFieldBuilder extends PipelineBuilder {

		/**
		 * @param name the field from the documents input to the {@code $lookup} stage, must not be {@literal null} or
		 *          empty.
		 * @return never {@literal null}.
		 */
		ForeignFieldBuilder localField(String name);
	}

	public interface ForeignFieldBuilder {

		/**
		 * @param name the field from the documents in the {@code from} collection, must not be {@literal null} or empty.
		 * @return never {@literal null}.
		 */
		AsBuilder foreignField(String name);
	}

	/**
	 * @since 4.1
	 * @author Christoph Strobl
	 */
	public interface LetBuilder {

		/**
		 * Specifies {@link Let#getVariableNames() variables) that can be used in the
		 * {@link PipelineBuilder#pipeline(AggregationOperation...) pipeline stages}.
		 *
		 * @param let must not be {@literal null}.
		 * @return never {@literal null}.
		 * @see PipelineBuilder
		 */
		PipelineBuilder let(Let let);

		/**
		 * Specifies {@link Let#getVariableNames() variables) that can be used in the
		 * {@link PipelineBuilder#pipeline(AggregationOperation...) pipeline stages}.
		 *
		 * @param variables must not be {@literal null}.
		 * @return never {@literal null}.
		 * @see PipelineBuilder
		 */
		default PipelineBuilder let(ExpressionVariable... variables) {
			return let(Let.just(variables));
		}
	}

	/**
	 * @since 4.1
	 * @author Christoph Strobl
	 */
	public interface PipelineBuilder extends LetBuilder {

		/**
		 * Specifies the {@link AggregationPipeline pipeline} that determines the resulting documents.
		 *
		 * @param pipeline must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		AsBuilder pipeline(AggregationPipeline pipeline);

		/**
		 * Specifies the {@link AggregationPipeline#getOperations() stages} that determine the resulting documents.
		 *
		 * @param stages must not be {@literal null} can be empty.
		 * @return never {@literal null}.
		 */
		default AsBuilder pipeline(AggregationOperation... stages) {
			return pipeline(AggregationPipeline.of(stages));
		}

		/**
		 * @param name the name of the new array field to add to the input documents, must not be {@literal null} or empty.
		 * @return new instance of {@link LookupOperation}.
		 */
		LookupOperation as(String name);
	}

	public interface AsBuilder extends PipelineBuilder {

		/**
		 * @param name the name of the new array field to add to the input documents, must not be {@literal null} or empty.
		 * @return new instance of {@link LookupOperation}.
		 */
		LookupOperation as(String name);
	}

	/**
	 * Builder for fluent {@link LookupOperation} creation.
	 *
	 * @author Christoph Strobl
	 * @since 1.9
	 */
	public static final class LookupOperationBuilder
			implements FromBuilder, LocalFieldBuilder, ForeignFieldBuilder, AsBuilder {

		private @Nullable String from;
		private @Nullable Field localField;
		private @Nullable Field foreignField;
		private @Nullable ExposedField as;
		private @Nullable Let let;
		private @Nullable AggregationPipeline pipeline;

		/**
		 * Creates new builder for {@link LookupOperation}.
		 *
		 * @return never {@literal null}.
		 */
		public static FromBuilder newBuilder() {
			return new LookupOperationBuilder();
		}

		@Override
		public LocalFieldBuilder from(String name) {

			Assert.hasText(name, "'From' must not be null or empty");
			from = name;
			return this;
		}

		@Override
		public AsBuilder foreignField(String name) {

			Assert.hasText(name, "'ForeignField' must not be null or empty");
			foreignField = Fields.field(name);
			return this;
		}

		@Override
		public ForeignFieldBuilder localField(String name) {

			Assert.hasText(name, "'LocalField' must not be null or empty");
			localField = Fields.field(name);
			return this;
		}

		@Override
		public PipelineBuilder let(Let let) {

			Assert.notNull(let, "Let must not be null");
			this.let = let;
			return this;
		}

		@Override
		public AsBuilder pipeline(AggregationPipeline pipeline) {

			Assert.notNull(pipeline, "Pipeline must not be null");
			this.pipeline = pipeline;
			return this;
		}

		@Override
		public LookupOperation as(String name) {

			Assert.hasText(name, "'As' must not be null or empty");
			as = new ExposedField(Fields.field(name), true);
			return new LookupOperation(from, localField, foreignField, let, pipeline, as);
		}
	}
}

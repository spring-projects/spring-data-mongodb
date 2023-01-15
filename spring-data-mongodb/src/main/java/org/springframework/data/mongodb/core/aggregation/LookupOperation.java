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

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.List;

/**
 * Encapsulates the aggregation framework {@code $lookup}-operation. We recommend to use the static factory method
 * {@link Aggregation#lookup(String, String, String, String)} instead of creating instances of this class directly.
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

	private final Field from;
	private final Field localField;
	private final Field foreignField;
	private final ExposedField as;

	@Nullable
	private final Let let;
	@Nullable
	private final AggregationPipeline pipeline;

	/**
	 * Creates a new {@link LookupOperation} for the given {@link Field}s.
	 *
	 * @param from must not be {@literal null}.
	 * @param localField must not be {@literal null}.
	 * @param foreignField must not be {@literal null}.
	 * @param as must not be {@literal null}.
	 */
	public LookupOperation(Field from, Field localField, Field foreignField, Field as) {
		this(from, localField, foreignField, as, null, null);
	}

	public LookupOperation(Field from, Field localField, Field foreignField, Field as, @Nullable Let let, @Nullable AggregationPipeline pipeline) {
		Assert.notNull(from, "From must not be null");
		Assert.notNull(localField, "LocalField must not be null");
		Assert.notNull(foreignField, "ForeignField must not be null");
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

		lookupObject.append("from", from.getTarget());
		lookupObject.append("localField", localField.getTarget());
		lookupObject.append("foreignField", foreignField.getTarget());
		lookupObject.append("as", as.getTarget());

		if (let != null) {
			lookupObject.append("let", let.toDocument(context));
		}

		if (pipeline != null) {
			lookupObject.append("pipeline", pipeline.toDocuments(context));
		}

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

	public static interface FromBuilder {

		/**
		 * @param name the collection in the same database to perform the join with, must not be {@literal null} or empty.
		 * @return never {@literal null}.
		 */
		LocalFieldBuilder from(String name);
	}

	public static interface LocalFieldBuilder {

		/**
		 * @param name the field from the documents input to the {@code $lookup} stage, must not be {@literal null} or
		 *          empty.
		 * @return never {@literal null}.
		 */
		ForeignFieldBuilder localField(String name);
	}

	public static interface ForeignFieldBuilder {

		/**
		 * @param name the field from the documents in the {@code from} collection, must not be {@literal null} or empty.
		 * @return never {@literal null}.
		 */
		AsBuilder foreignField(String name);
	}

	public static interface AsBuilder {

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

		private @Nullable Field from;
		private @Nullable Field localField;
		private @Nullable Field foreignField;
		private @Nullable ExposedField as;

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
			from = Fields.field(name);
			return this;
		}

		@Override
		public LookupOperation as(String name) {

			Assert.hasText(name, "'As' must not be null or empty");
			as = new ExposedField(Fields.field(name), true);
			return new LookupOperation(from, localField, foreignField, as);
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
	}

	public static class Let implements AggregationExpression{

		private final List<ExpressionVariable> vars;

		public Let(List<ExpressionVariable> vars) {
			Assert.notEmpty(vars, "'let' must not be null or empty");
			this.vars = vars;
		}

		@Override
		public Document toDocument(AggregationOperationContext context) {
			return toLet();
		}

		private Document toLet() {
			Document mappedVars = new Document();

			for (ExpressionVariable var : this.vars) {
				mappedVars.putAll(getMappedVariable(var));
			}

			return mappedVars;
		}

		private Document getMappedVariable(ExpressionVariable var) {
			return new Document(var.variableName, prefixDollarSign(var.expression));
		}

		private String prefixDollarSign(String expression) {
			return "$" + expression;
		}

		public static class ExpressionVariable {

			private final String variableName;

			private final String expression;

			public ExpressionVariable(String variableName, String expression) {
				this.variableName = variableName;
				this.expression = expression;
			}
		}
	}
}

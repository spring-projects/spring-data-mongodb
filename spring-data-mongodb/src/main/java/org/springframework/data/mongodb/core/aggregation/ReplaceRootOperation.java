/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.expression.spel.ast.Projection;
import org.springframework.util.Assert;

/**
 * Encapsulates the aggregation framework {@code $replaceRoot}-operation. <br />
 * We recommend to use the static factory method {@link Aggregation#replaceRoot(String)} instead of creating instances
 * of this class directly.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.10
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/">MongoDB Aggregation
 *      Framework: $replaceRoot</a>
 */
public class ReplaceRootOperation implements FieldsExposingAggregationOperation {

	private final Replacement replacement;

	/**
	 * Creates a new {@link ReplaceRootOperation} given the {@link Field} field name.
	 *
	 * @param field must not be {@literal null} or empty.
	 */
	public ReplaceRootOperation(Field field) {
		this(new FieldReplacement(field));
	}

	/**
	 * Creates a new {@link ReplaceRootOperation} given the {@link AggregationExpression} pointing to a document.
	 *
	 * @param aggregationExpression must not be {@literal null}.
	 */
	public ReplaceRootOperation(AggregationExpression aggregationExpression) {
		this(new AggregationExpressionReplacement(aggregationExpression));
	}

	/**
	 * Creates a new {@link ReplaceRootOperation} given the {@link Replacement}.
	 *
	 * @param replacement must not be {@literal null}.
	 */
	public ReplaceRootOperation(Replacement replacement) {

		Assert.notNull(replacement, "Replacement must not be null!");
		this.replacement = replacement;
	}

	/**
	 * Creates a new {@link ReplaceRootDocumentOperationBuilder}.
	 *
	 * @return a new {@link ReplaceRootDocumentOperationBuilder}.
	 */
	public static ReplaceRootOperationBuilder builder() {
		return new ReplaceRootOperationBuilder();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {
		return new Document("$replaceRoot", new Document("newRoot", getReplacement().toDocumentExpression(context)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation#getFields()
	 */
	@Override
	public ExposedFields getFields() {
		return ExposedFields.from();
	}

	/**
	 * Obtain the {@link Replacement}.
	 *
	 * @return never {@literal null}.
	 * @since 3.0
	 */
	protected Replacement getReplacement() {
		return replacement;
	}

	/**
	 * Builder for {@link ReplaceRootOperation}.
	 *
	 * @author Mark Paluch
	 */
	public static class ReplaceRootOperationBuilder {

		/**
		 * Defines a root document replacement based on a {@literal fieldName} that resolves to a document.
		 *
		 * @param fieldName must not be {@literal null} or empty.
		 * @return the final {@link ReplaceRootOperation}.
		 */
		public ReplaceRootOperation withValueOf(String fieldName) {
			return new ReplaceRootOperation(Fields.field(fieldName));
		}

		/**
		 * Defines a root document replacement based on a {@link AggregationExpression} that resolves to a document.
		 *
		 * @param aggregationExpression must not be {@literal null}.
		 * @return the final {@link ReplaceRootOperation}.
		 */
		public ReplaceRootOperation withValueOf(AggregationExpression aggregationExpression) {
			return new ReplaceRootOperation(aggregationExpression);
		}

		/**
		 * Defines a root document replacement based on a composable document that is empty initially. <br />
		 * {@link ReplaceRootOperation} can be populated with individual entries and derive its values from other, existing
		 * documents.
		 *
		 * @return the {@link ReplaceRootDocumentOperation}.
		 */
		public ReplaceRootDocumentOperation withDocument() {
			return new ReplaceRootDocumentOperation();
		}

		/**
		 * Defines a root document replacement based on a composable document given {@literal document}. <br />
		 * {@link ReplaceRootOperation} can be populated with individual entries and derive its values from other, existing
		 * documents.
		 *
		 * @param document must not be {@literal null}.
		 * @return the final {@link ReplaceRootOperation}.
		 */
		public ReplaceRootOperation withDocument(Document document) {

			Assert.notNull(document, "Document must not be null!");

			return new ReplaceRootDocumentOperation().andValuesOf(document);
		}
	}

	/**
	 * Encapsulates the aggregation framework {@code $replaceRoot}-operation to result in a composable replacement
	 * document. <br />
	 * Instances of {@link ReplaceRootDocumentOperation} yield empty upon construction and can be populated with single
	 * values and documents.
	 *
	 * @author Mark Paluch
	 */
	static class ReplaceRootDocumentOperation extends ReplaceRootOperation {

		private final static ReplacementDocument EMPTY = new ReplacementDocument();
		private final ReplacementDocument current;

		/**
		 * Creates an empty {@link ReplaceRootDocumentOperation}.
		 */
		public ReplaceRootDocumentOperation() {
			this(EMPTY);
		}

		private ReplaceRootDocumentOperation(ReplacementDocument replacementDocument) {
			super(replacementDocument);
			current = replacementDocument;
		}

		/**
		 * Creates an extended {@link ReplaceRootDocumentOperation} that combines {@link ReplacementDocument}s from the
		 * {@literal currentOperation} and {@literal extension} operation.
		 *
		 * @param currentOperation must not be {@literal null}.
		 * @param extension must not be {@literal null}.
		 */
		protected ReplaceRootDocumentOperation(ReplaceRootDocumentOperation currentOperation,
				ReplacementDocument extension) {
			this(currentOperation.current.extendWith(extension));
		}

		/**
		 * Creates a new {@link ReplaceRootDocumentOperationBuilder} to define a field for the
		 * {@link AggregationExpression}.
		 *
		 * @param aggregationExpression must not be {@literal null}.
		 * @return the {@link ReplaceRootDocumentOperationBuilder}.
		 */
		public ReplaceRootDocumentOperationBuilder and(AggregationExpression aggregationExpression) {
			return new ReplaceRootDocumentOperationBuilder(this, aggregationExpression);
		}

		/**
		 * Creates a new {@link ReplaceRootDocumentOperationBuilder} to define a field for the {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return the {@link ReplaceRootDocumentOperationBuilder}.
		 */
		public ReplaceRootDocumentOperationBuilder andValue(Object value) {
			return new ReplaceRootDocumentOperationBuilder(this, value);
		}

		/**
		 * Creates a new {@link ReplaceRootDocumentOperation} that merges all existing replacement values with values from
		 * {@literal value}. Existing replacement values are overwritten.
		 *
		 * @param value must not be {@literal null}.
		 * @return the {@link ReplaceRootDocumentOperation}.
		 */
		public ReplaceRootDocumentOperation andValuesOf(Object value) {
			return new ReplaceRootDocumentOperation(this, ReplacementDocument.valueOf(value));
		}
	}

	/**
	 * Builder for {@link ReplaceRootDocumentOperation} to populate {@link ReplacementDocument}
	 *
	 * @author Mark Paluch
	 */
	public static class ReplaceRootDocumentOperationBuilder {

		private final ReplaceRootDocumentOperation currentOperation;
		private final Object value;

		protected ReplaceRootDocumentOperationBuilder(ReplaceRootDocumentOperation currentOperation, Object value) {

			Assert.notNull(currentOperation, "Current ReplaceRootDocumentOperation must not be null!");
			Assert.notNull(value, "Value must not be null!");

			this.currentOperation = currentOperation;
			this.value = value;
		}

		public ReplaceRootDocumentOperation as(String fieldName) {

			if (value instanceof AggregationExpression) {
				return new ReplaceRootDocumentOperation(currentOperation,
						ReplacementDocument.forExpression(fieldName, (AggregationExpression) value));
			}

			return new ReplaceRootDocumentOperation(currentOperation, ReplacementDocument.forSingleValue(fieldName, value));
		}
	}

	/**
	 * Replacement object that results in a replacement document or an expression that results in a document.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	public interface Replacement {

		/**
		 * Renders the current {@link Replacement} into a its MongoDB representation based on the given
		 * {@link AggregationOperationContext}.
		 *
		 * @param context will never be {@literal null}.
		 * @return a replacement document or an expression that results in a document.
		 */
		Object toDocumentExpression(AggregationOperationContext context);
	}

	/**
	 * {@link Replacement} that uses a {@link AggregationExpression} that results in a replacement document.
	 *
	 * @author Mark Paluch
	 */
	private static class AggregationExpressionReplacement implements Replacement {

		private final AggregationExpression aggregationExpression;

		protected AggregationExpressionReplacement(AggregationExpression aggregationExpression) {

			Assert.notNull(aggregationExpression, "AggregationExpression must not be null!");
			this.aggregationExpression = aggregationExpression;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.Replacement#toObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocumentExpression(AggregationOperationContext context) {
			return aggregationExpression.toDocument(context);
		}
	}

	/**
	 * {@link Replacement that references a {@link Field} inside the current aggregation pipeline.
	 *
	 * @author Mark Paluch
	 */
	private static class FieldReplacement implements Replacement {

		private final Field field;

		/**
		 * Creates {@link FieldReplacement} given {@link Field}.
		 */
		protected FieldReplacement(Field field) {

			Assert.notNull(field, "Field must not be null!");
			this.field = field;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.Replacement#toObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Object toDocumentExpression(AggregationOperationContext context) {
			return context.getReference(field).toString();
		}
	}

	/**
	 * Replacement document consisting of multiple {@link ReplacementContributor}s.
	 *
	 * @author Mark Paluch
	 */
	private static class ReplacementDocument implements Replacement {

		private final Collection<ReplacementContributor> replacements;

		/**
		 * Creates an empty {@link ReplacementDocument}.
		 */
		protected ReplacementDocument() {
			replacements = new ArrayList<ReplacementContributor>();
		}

		/**
		 * Creates a {@link ReplacementDocument} given {@link ReplacementContributor}.
		 *
		 * @param contributor
		 */
		protected ReplacementDocument(ReplacementContributor contributor) {

			Assert.notNull(contributor, "ReplacementContributor must not be null!");
			replacements = Collections.singleton(contributor);
		}

		private ReplacementDocument(Collection<ReplacementContributor> replacements) {
			this.replacements = replacements;
		}

		/**
		 * Creates a {@link ReplacementDocument} given a {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static ReplacementDocument valueOf(Object value) {
			return new ReplacementDocument(new DocumentContributor(value));
		}

		/**
		 * Creates a {@link ReplacementDocument} given a single {@literal field} and {@link AggregationExpression}.
		 *
		 * @param aggregationExpression must not be {@literal null}.
		 * @return
		 */
		public static ReplacementDocument forExpression(String field, AggregationExpression aggregationExpression) {
			return new ReplacementDocument(new ExpressionFieldContributor(Fields.field(field), aggregationExpression));
		}

		/**
		 * Creates a {@link ReplacementDocument} given a single {@literal field} and {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static ReplacementDocument forSingleValue(String field, Object value) {
			return new ReplacementDocument(new ValueFieldContributor(Fields.field(field), value));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.Replacement#toObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocumentExpression(AggregationOperationContext context) {

			Document document = new Document();

			for (ReplacementContributor replacement : replacements) {
				document.putAll(replacement.toDocument(context));
			}

			return document;
		}

		/**
		 * Extend a replacement document that merges {@code this} and {@literal replacement} {@link ReplacementContributor}s
		 * in a new {@link ReplacementDocument}.
		 *
		 * @param extension must not be {@literal null}.
		 * @return the new, extended {@link ReplacementDocument}
		 */
		public ReplacementDocument extendWith(ReplacementDocument extension) {

			Assert.notNull(extension, "ReplacementDocument must not be null");

			ReplacementDocument replacementDocument = new ReplacementDocument();

			List<ReplacementContributor> replacements = new ArrayList<ReplacementContributor>(
					this.replacements.size() + extension.replacements.size());

			replacements.addAll(this.replacements);
			replacements.addAll(extension.replacements);

			return new ReplacementDocument(replacements);
		}
	}

	/**
	 * Partial {@link Document} contributor for document replacement.
	 *
	 * @author Mark Paluch
	 */
	private interface ReplacementContributor extends AggregationExpression {

		/**
		 * Renders the current {@link ReplacementContributor} into a {@link Document} based on the given
		 * {@link AggregationOperationContext}.
		 *
		 * @param context will never be {@literal null}.
		 * @return
		 */
		Document toDocument(AggregationOperationContext context);
	}

	/**
	 * {@link ReplacementContributor} to contribute multiple fields based on the input {@literal value}. <br />
	 * The value object is mapped into a MongoDB {@link Document}.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	private static class DocumentContributor implements ReplacementContributor {

		private final Object value;

		/**
		 * Creates new {@link Projection} for the given {@link Field}.
		 *
		 * @param value must not be {@literal null}.
		 */
		public DocumentContributor(Object value) {

			Assert.notNull(value, "Value must not be null!");
			this.value = value;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplacementContributor#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(AggregationOperationContext context) {

			Document document = new Document("$set", value);

			return (Document) context.getMappedObject(document).get("$set");
		}
	}

	/**
	 * Base class for {@link ReplacementContributor} implementations to contribute a single {@literal field} Typically
	 * used to construct a composite document that should contain the resulting key-value pair.
	 *
	 * @author Mark Paluch
	 */
	private abstract static class FieldContributorSupport implements ReplacementContributor {

		private final ExposedField field;

		/**
		 * Creates new {@link FieldContributorSupport} for the given {@link Field}.
		 *
		 * @param field must not be {@literal null}.
		 */
		public FieldContributorSupport(Field field) {

			Assert.notNull(field, "Field must not be null!");
			this.field = new ExposedField(field, true);
		}

		/**
		 * @return the {@link ExposedField}.
		 */
		public ExposedField getField() {
			return field;
		}
	}

	/**
	 * {@link ReplacementContributor} to contribute a single {@literal field} and {@literal value}. The {@literal value}
	 * is mapped to a MongoDB {@link Document} and can be a singular value, a list or subdocument.
	 *
	 * @author Mark Paluch
	 */
	private static class ValueFieldContributor extends FieldContributorSupport {

		private final Object value;

		/**
		 * Creates new {@link Projection} for the given {@link Field}.
		 *
		 * @param field must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 */
		public ValueFieldContributor(Field field, Object value) {

			super(field);

			Assert.notNull(value, "Value must not be null!");

			this.value = value;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplacementContributor#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(AggregationOperationContext context) {

			Document document = new Document("$set", value);
			return new Document(getField().getTarget(), context.getMappedObject(document).get("$set"));
		}
	}

	/**
	 * {@link ReplacementContributor} to contribute a single {@literal field} and value based on a
	 * {@link AggregationExpression}.
	 *
	 * @author Mark Paluch
	 */
	private static class ExpressionFieldContributor extends FieldContributorSupport {

		private final AggregationExpression aggregationExpression;

		/**
		 * Creates new {@link Projection} for the given {@link Field}.
		 *
		 * @param field must not be {@literal null}.
		 * @param aggregationExpression must not be {@literal null}.
		 */
		public ExpressionFieldContributor(Field field, AggregationExpression aggregationExpression) {

			super(field);

			Assert.notNull(aggregationExpression, "AggregationExpression must not be null!");

			this.aggregationExpression = aggregationExpression;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation.ReplacementContributor#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(AggregationOperationContext context) {
			return new Document(getField().getTarget(), aggregationExpression.toDocument(context));
		}
	}
}

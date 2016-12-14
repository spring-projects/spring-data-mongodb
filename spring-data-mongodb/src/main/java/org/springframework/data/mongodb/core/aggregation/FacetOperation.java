/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.Output;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $facet}-operation. <br />
 * Facet of {@link AggregationOperation}s to be used in an {@link Aggregation}. Processes multiple
 * {@link AggregationOperation} pipelines within a single stage on the same set of input documents. Each sub-pipeline
 * has its own field in the output document where its results are stored as an array of documents.
 * {@link FacetOperation} enables various aggregations on the same set of input documents, without needing to retrieve
 * the input documents multiple times. <br />
 * As of MongoDB 3.4, {@link FacetOperation} cannot be used with nested pipelines containing {@link GeoNearOperation},
 * {@link OutOperation} and {@link FacetOperation}. <br />
 * We recommend to use the static factory method {@link Aggregation#facet()} instead of creating instances of this class
 * directly.
 *
 * @see http://docs.mongodb.org/manual/reference/aggregation/facet/
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.10
 */
public class FacetOperation implements FieldsExposingAggregationOperation {

	/**
	 * Empty (initial) {@link FacetOperation}.
	 */
	public static final FacetOperation EMPTY = new FacetOperation();

	private final Facets facets;

	/**
	 * Creates a new {@link FacetOperation}.
	 */
	public FacetOperation() {
		this(Facets.EMPTY);
	}

	private FacetOperation(Facets facets) {
		this.facets = facets;
	}

	/**
	 * Creates a new {@link FacetOperationBuilder} to append a new facet using {@literal operations}. <br />
	 * {@link FacetOperationBuilder} takes a pipeline of {@link AggregationOperation} to categorize documents into a
	 * single facet.
	 *
	 * @param operations must not be {@literal null} or empty.
	 * @return
	 */
	public FacetOperationBuilder and(AggregationOperation... operations) {

		Assert.notNull(operations, "AggregationOperations must not be null!");
		Assert.notEmpty(operations, "AggregationOperations must not be empty!");

		return new FacetOperationBuilder(facets, Arrays.asList(operations));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {
		return new BasicDBObject("$facet", facets.toDBObject(context));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation#getFields()
	 */
	@Override
	public ExposedFields getFields() {
		return facets.asExposedFields();
	}

	/**
	 * Builder for {@link FacetOperation} by adding existing and the new pipeline of {@link AggregationOperation} to the
	 * new {@link FacetOperation}.
	 *
	 * @author Mark Paluch
	 */
	public static class FacetOperationBuilder {

		private final Facets current;
		private final List<AggregationOperation> operations;

		private FacetOperationBuilder(Facets current, List<AggregationOperation> operations) {
			this.current = current;
			this.operations = operations;
		}

		/**
		 * Creates a new {@link FacetOperation} that contains the configured pipeline of {@link AggregationOperation}
		 * exposed as {@literal fieldName} in the resulting facet document.
		 *
		 * @param fieldName must not be {@literal null} or empty.
		 * @return
		 */
		public FacetOperation as(String fieldName) {

			Assert.hasText(fieldName, "FieldName must not be null or empty!");

			return new FacetOperation(current.and(fieldName, operations));
		}
	}

	/**
	 * Encapsulates multiple {@link Facet}s
	 *
	 * @author Mark Paluch
	 */
	private static class Facets {

		private static final Facets EMPTY = new Facets(Collections.<Facet> emptyList());

		private List<Facet> facets;

		/**
		 * Creates a new {@link Facets} given {@link List} of {@link Facet}.
		 *
		 * @param facets
		 */
		private Facets(List<Facet> facets) {
			this.facets = facets;
		}

		/**
		 * @return the {@link ExposedFields} derived from {@link Output}.
		 */
		ExposedFields asExposedFields() {

			ExposedFields fields = ExposedFields.from();

			for (Facet facet : facets) {
				fields = fields.and(facet.getExposedField());
			}

			return fields;
		}

		DBObject toDBObject(AggregationOperationContext context) {

			DBObject dbObject = new BasicDBObject(facets.size());

			for (Facet facet : facets) {
				dbObject.put(facet.getExposedField().getName(), facet.toDBObjects(context));
			}

			return dbObject;
		}

		/**
		 * Adds a facet to this {@link Facets}.
		 *
		 * @param fieldName must not be {@literal null}.
		 * @param operations must not be {@literal null}.
		 * @return the new {@link Facets}.
		 */
		Facets and(String fieldName, List<AggregationOperation> operations) {

			Assert.hasText(fieldName, "FieldName must not be null or empty!");
			Assert.notNull(operations, "AggregationOperations must not be null!");

			List<Facet> facets = new ArrayList<Facet>(this.facets.size() + 1);
			facets.addAll(this.facets);
			facets.add(new Facet(new ExposedField(fieldName, true), operations));

			return new Facets(facets);
		}
	}

	/**
	 * A single facet with a {@link ExposedField} and its {@link AggregationOperation} pipeline.
	 *
	 * @author Mark Paluch
	 */
	private static class Facet {

		private final ExposedField exposedField;
		private final List<AggregationOperation> operations;

		/**
		 * Creates a new {@link Facet} given {@link ExposedField} and {@link AggregationOperation} pipeline.
		 *
		 * @param exposedField must not be {@literal null}.
		 * @param operations must not be {@literal null}.
		 */
		Facet(ExposedField exposedField, List<AggregationOperation> operations) {

			Assert.notNull(exposedField, "ExposedField must not be null!");
			Assert.notNull(operations, "AggregationOperations must not be null!");

			this.exposedField = exposedField;
			this.operations = operations;
		}

		ExposedField getExposedField() {
			return exposedField;
		}

		List<DBObject> toDBObjects(AggregationOperationContext context) {
			return AggregationOperationRenderer.toDBObject(operations, context);
		}
	}
}

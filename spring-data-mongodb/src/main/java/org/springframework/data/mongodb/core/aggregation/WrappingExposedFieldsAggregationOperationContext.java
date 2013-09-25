package org.springframework.data.mongodb.core.aggregation;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;

/**
 * {@link AggregationOperationContext} that combines the available field references from a given
 * {@code AggregationOperationContext} and an {@link FieldsExposingAggregationOperation}.
 * 
 * @author Thomas Darimont
 * @since 1.4
 */
class WrappingExposedFieldsAggregationOperationContext extends ExposedFieldsAggregationOperationContext {

	private final FieldsExposingAggregationOperation fieldExposingOperation;

	/**
	 * Creates a new {@link WrappingExposedFieldsAggregationOperationContext} from the given
	 * {@link FieldsExposingAggregationOperation}.
	 * 
	 * @param fieldExposingOperation
	 */
	public WrappingExposedFieldsAggregationOperationContext(FieldsExposingAggregationOperation fieldExposingOperation) {
		this.fieldExposingOperation = fieldExposingOperation;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.ExposedFieldsAggregationOperationContext#getFields()
	 */
	private ExposedFields getFields() {
		return fieldExposingOperation.getFields();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getReference(java.lang.String)
	 */
	@Override
	public FieldReference getReference(String name) {

		ExposedField field = getFields().getField(name);

		if (field != null) {
			return new FieldReference(field);
		}

		throw new IllegalArgumentException(String.format("Invalid reference '%s'!", name));
	}
}

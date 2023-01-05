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
package org.springframework.data.mongodb.core.aggregation;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Encapsulates the {@code setWindowFields}-operation.
 *
 * @author Christoph Strobl
 * @since 3.3
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/setWindowFields/">https://docs.mongodb.com/manual/reference/operator/aggregation/setWindowFields/</a>
 */
public class SetWindowFieldsOperation
		implements AggregationOperation, FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation {

	private static final String CURRENT = "current";
	private static final String UNBOUNDED = "unbounded";

	private final @Nullable Object partitionBy;
	private final @Nullable AggregationOperation sortBy;
	private final WindowOutput output;

	/**
	 * Create a new {@link SetWindowFieldsOperation} with given args.
	 *
	 * @param partitionBy The field or {@link AggregationExpression} to group by.
	 * @param sortBy the {@link SortOperation operation} to sort the documents by in the partition.
	 * @param output the {@link WindowOutput} containing the fields to add and the rules to calculate their respective
	 *          values.
	 */
	protected SetWindowFieldsOperation(@Nullable Object partitionBy, @Nullable AggregationOperation sortBy,
			WindowOutput output) {

		this.partitionBy = partitionBy;
		this.sortBy = sortBy;
		this.output = output;
	}

	/**
	 * Obtain a {@link SetWindowFieldsOperationBuilder builder} to create a {@link SetWindowFieldsOperation}.
	 *
	 * @return new instance of {@link SetWindowFieldsOperationBuilder}.
	 */
	public static SetWindowFieldsOperationBuilder builder() {
		return new SetWindowFieldsOperationBuilder();
	}

	@Override
	public ExposedFields getFields() {
		return ExposedFields.nonSynthetic(Fields.from(output.fields.toArray(new Field[0])));
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document $setWindowFields = new Document();
		if (partitionBy != null) {
			if (partitionBy instanceof AggregationExpression aggregationExpression) {
				$setWindowFields.append("partitionBy", aggregationExpression.toDocument(context));
			} else if (partitionBy instanceof Field field) {
				$setWindowFields.append("partitionBy", context.getReference(field).toString());
			} else {
				$setWindowFields.append("partitionBy", partitionBy);
			}
		}

		if (sortBy != null) {
			$setWindowFields.append("sortBy", sortBy.toDocument(context).get(sortBy.getOperator()));
		}

		Document output = new Document();
		for (ComputedField field : this.output.fields) {

			Document fieldOperation = field.getWindowOperator().toDocument(context);
			if (field.window != null) {
				fieldOperation.put("window", field.window.toDocument(context));
			}
			output.append(field.getName(), fieldOperation);
		}
		$setWindowFields.append("output", output);

		return new Document(getOperator(), $setWindowFields);
	}

	@Override
	public String getOperator() {
		return "$setWindowFields";
	}

	/**
	 * {@link WindowOutput} defines output of {@literal $setWindowFields} stage by defining the {@link ComputedField
	 * field(s)} to append to the documents in the output.
	 */
	public static class WindowOutput {

		private final List<ComputedField> fields;

		/**
		 * Create a new output containing the single given {@link ComputedField field}.
		 *
		 * @param outputField must not be {@literal null}.
		 */
		public WindowOutput(ComputedField outputField) {

			Assert.notNull(outputField, "OutputField must not be null");

			this.fields = new ArrayList<>();
			this.fields.add(outputField);
		}

		/**
		 * Append the given {@link ComputedField field} to the outptut.
		 *
		 * @param field must not be {@literal null}.
		 * @return this.
		 */
		public WindowOutput append(ComputedField field) {

			Assert.notNull(field, "Field must not be null");

			fields.add(field);
			return this;
		}

		/**
		 * Append the given {@link AggregationExpression} as a {@link ComputedField field} in a fluent way.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ComputedFieldAppender}.
		 * @see #append(ComputedField)
		 */
		public ComputedFieldAppender append(AggregationExpression expression) {

			return new ComputedFieldAppender() {

				@Nullable private Window window;

				@Override
				public WindowOutput as(String fieldname) {

					return WindowOutput.this.append(new ComputedField(fieldname, expression, window));
				}

				@Override
				public ComputedFieldAppender within(Window window) {
					this.window = window;
					return this;
				}
			};
		}

		/**
		 * Tiny little helper to allow fluent API usage for {@link #append(ComputedField)}.
		 */
		interface ComputedFieldAppender {

			/**
			 * Specify the target field name.
			 *
			 * @param fieldname the name of field to add to the target document.
			 * @return the {@link WindowOutput} that started the append operation.
			 */
			WindowOutput as(String fieldname);

			/**
			 * Specify the window boundaries.
			 *
			 * @param window must not be {@literal null}.
			 * @return this.
			 */
			ComputedFieldAppender within(Window window);
		}
	}

	/**
	 * A {@link Field} that the result of a computation done via an {@link AggregationExpression}.
	 *
	 * @author Christoph Strobl
	 */
	public static class ComputedField implements Field {

		private final String name;
		private final AggregationExpression windowOperator;
		private final @Nullable Window window;

		/**
		 * Create a new {@link ComputedField}.
		 *
		 * @param name the target field name.
		 * @param windowOperator the expression to calculate the field value.
		 */
		public ComputedField(String name, AggregationExpression windowOperator) {
			this(name, windowOperator, null);
		}

		/**
		 * Create a new {@link ComputedField}.
		 *
		 * @param name the target field name.
		 * @param windowOperator the expression to calculate the field value.
		 * @param window the boundaries to operate within. Can be {@literal null}.
		 */
		public ComputedField(String name, AggregationExpression windowOperator, @Nullable Window window) {

			this.name = name;
			this.windowOperator = windowOperator;
			this.window = window;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public String getTarget() {
			return getName();
		}

		@Override
		public boolean isAliased() {
			return false;
		}

		public AggregationExpression getWindowOperator() {
			return windowOperator;
		}

		public Window getWindow() {
			return window;
		}
	}

	/**
	 * Quick access to {@link DocumentWindow documents} and {@literal RangeWindow range} {@link Window windows}.
	 *
	 * @author Christoph Strobl
	 */
	public interface Windows {

		/**
		 * Create a document window relative to the position of the current document.
		 *
		 * @param lower an integer for a position relative to the current document, {@literal current} or
		 *          {@literal unbounded}.
		 * @param upper an integer for a position relative to the current document, {@literal current} or
		 *          {@literal unbounded}.
		 * @return new instance of {@link DocumentWindow}.
		 */
		static DocumentWindow documents(Object lower, Object upper) {
			return new DocumentWindow(lower, upper);
		}

		/**
		 * Create a range window defined based on sort expression.
		 *
		 * @param lower a numeric value to add the sort by field value of the current document, {@literal current} or
		 *          {@literal unbounded}.
		 * @param upper a numeric value to add the sort by field value of the current document, {@literal current} or
		 *          {@literal unbounded}.
		 * @return new instance of {@link RangeWindow}.
		 */
		static RangeWindow range(Object lower, Object upper, @Nullable WindowUnit unit) {
			return new RangeWindow(lower, upper, unit == null ? WindowUnits.DEFAULT : unit);
		}

		/**
		 * Create a range window based on the {@link Sort sort value} of the current document via a fluent API.
		 *
		 * @return new instance of {@link RangeWindowBuilder}.
		 */
		static RangeWindowBuilder range() {
			return new RangeWindowBuilder();
		}

		/**
		 * Create a document window relative to the position of the current document via a fluent API.
		 *
		 * @return new instance of {@link DocumentWindowBuilder}.
		 */
		static DocumentWindowBuilder documents() {
			return new DocumentWindowBuilder();
		}
	}

	/**
	 * A {@link Window} to be used for {@link ComputedField#getWindow() ComputedField}.
	 */
	public interface Window {

		/**
		 * The lower (inclusive) boundary.
		 *
		 * @return
		 */
		Object getLower();

		/**
		 * The upper (inclusive) boundary.
		 *
		 * @return
		 */
		Object getUpper();

		/**
		 * Obtain the document representation of the window in a default {@link AggregationOperationContext context}.
		 *
		 * @return never {@literal null}.
		 */
		default Document toDocument() {
			return toDocument(Aggregation.DEFAULT_CONTEXT);
		}

		/**
		 * Obtain the document representation of the window in the given {@link AggregationOperationContext context}.
		 *
		 * @return never {@literal null}.
		 */
		Document toDocument(AggregationOperationContext ctx);
	}

	/**
	 * Builder API for a {@link RangeWindow}.
	 *
	 * @author Christoph Strobl
	 */
	public static class RangeWindowBuilder {

		private @Nullable Object lower;
		private @Nullable Object upper;
		private @Nullable WindowUnit unit;

		/**
		 * The lower (inclusive) range limit based on the sortBy field.
		 *
		 * @param lower eg. {@literal current} or {@literal unbounded}.
		 * @return this.
		 */
		public RangeWindowBuilder from(String lower) {

			this.lower = lower;
			return this;
		}

		/**
		 * The upper (inclusive) range limit based on the sortBy field.
		 *
		 * @param upper eg. {@literal current} or {@literal unbounded}.
		 * @return this.
		 */
		public RangeWindowBuilder to(String upper) {

			this.upper = upper;
			return this;
		}

		/**
		 * The lower (inclusive) range limit value to add to the value based on the sortBy field. Use a negative integer for
		 * a position before the current document. Use a positive integer for a position after the current document.
		 * {@code 0} is the current document position.
		 *
		 * @param lower
		 * @return this.
		 */
		public RangeWindowBuilder from(Number lower) {

			this.lower = lower;
			return this;
		}

		/**
		 * The upper (inclusive) range limit value to add to the value based on the sortBy field. Use a negative integer for
		 * a position before the current document. Use a positive integer for a position after the current document.
		 * {@code 0} is the current document position.
		 *
		 * @param upper
		 * @return this.
		 */
		public RangeWindowBuilder to(Number upper) {

			this.upper = upper;
			return this;
		}

		/**
		 * Use {@literal current} as {@link #from(String) lower} limit.
		 *
		 * @return this.
		 */
		public RangeWindowBuilder fromCurrent() {
			return from(CURRENT);
		}

		/**
		 * Use {@literal unbounded} as {@link #from(String) lower} limit.
		 *
		 * @return this.
		 */
		public RangeWindowBuilder fromUnbounded() {
			return from(UNBOUNDED);
		}

		/**
		 * Use {@literal current} as {@link #to(String) upper} limit.
		 *
		 * @return this.
		 */
		public RangeWindowBuilder toCurrent() {
			return to(CURRENT);
		}

		/**
		 * Use {@literal unbounded} as {@link #to(String) upper} limit.
		 *
		 * @return this.
		 */
		public RangeWindowBuilder toUnbounded() {
			return to(UNBOUNDED);
		}

		/**
		 * Set the {@link WindowUnit unit} or measure for the given {@link Window}.
		 *
		 * @param windowUnit must not be {@literal null}. Can be on of {@link Windows}.
		 * @return this.
		 */
		public RangeWindowBuilder unit(WindowUnit windowUnit) {

			Assert.notNull(windowUnit, "WindowUnit must not be null");
			this.unit = windowUnit;
			return this;
		}

		/**
		 * Build the {@link RangeWindow}.
		 *
		 * @return new instance of {@link RangeWindow}.
		 */
		public RangeWindow build() {

			Assert.notNull(lower, "Lower bound must not be null");
			Assert.notNull(upper, "Upper bound must not be null");
			Assert.notNull(unit, "WindowUnit bound must not be null");

			return new RangeWindow(lower, upper, unit);
		}
	}

	/**
	 * Builder API for a {@link RangeWindow}.
	 *
	 * @author Christoph Strobl
	 */
	public static class DocumentWindowBuilder {

		private @Nullable Object lower;
		private @Nullable Object upper;

		/**
		 * The lower (inclusive) range limit based on current document. Use a negative integer for a position before the
		 * current document. Use a positive integer for a position after the current document. {@code 0} is the current
		 * document position.
		 *
		 * @param lower
		 * @return this.
		 */
		public DocumentWindowBuilder from(Number lower) {

			this.lower = lower;
			return this;
		}

		public DocumentWindowBuilder fromCurrent() {
			return from(CURRENT);
		}

		public DocumentWindowBuilder fromUnbounded() {
			return from(UNBOUNDED);
		}

		public DocumentWindowBuilder to(String upper) {

			this.upper = upper;
			return this;
		}

		/**
		 * The lower (inclusive) range limit based on current document.
		 *
		 * @param lower eg. {@literal current} or {@literal unbounded}.
		 * @return this.
		 */
		public DocumentWindowBuilder from(String lower) {

			this.lower = lower;
			return this;
		}

		/**
		 * The upper (inclusive) range limit based on current document. Use a negative integer for a position before the
		 * current document. Use a positive integer for a position after the current document. {@code 0} is the current
		 * document position.
		 *
		 * @param upper
		 * @return this.
		 */
		public DocumentWindowBuilder to(Number upper) {

			this.upper = upper;
			return this;
		}

		public DocumentWindowBuilder toCurrent() {
			return to(CURRENT);
		}

		public DocumentWindowBuilder toUnbounded() {
			return to(UNBOUNDED);
		}

		public DocumentWindow build() {

			Assert.notNull(lower, "Lower bound must not be null");
			Assert.notNull(upper, "Upper bound must not be null");

			return new DocumentWindow(lower, upper);
		}
	}

	/**
	 * Common base class for {@link Window} implementation.
	 *
	 * @author Christoph Strobl
	 */
	static abstract class WindowImpl implements Window {

		private final Object lower;
		private final Object upper;

		protected WindowImpl(Object lower, Object upper) {
			this.lower = lower;
			this.upper = upper;
		}

		@Override
		public Object getLower() {
			return lower;
		}

		@Override
		public Object getUpper() {
			return upper;
		}
	}

	/**
	 * {@link Window} implementation based on the current document.
	 *
	 * @author Christoph Strobl
	 */
	public static class DocumentWindow extends WindowImpl {

		DocumentWindow(Object lower, Object upper) {
			super(lower, upper);
		}

		@Override
		public Document toDocument(AggregationOperationContext ctx) {
			return new Document("documents", Arrays.asList(getLower(), getUpper()));
		}
	}

	/**
	 * {@link Window} implementation based on the sort fields.
	 *
	 * @author Christoph Strobl
	 */
	public static class RangeWindow extends WindowImpl {

		private final WindowUnit unit;

		protected RangeWindow(Object lower, Object upper, WindowUnit unit) {

			super(lower, upper);
			this.unit = unit;
		}

		@Override
		public Document toDocument(AggregationOperationContext ctx) {

			Document range = new Document("range", new Object[] { getLower(), getUpper() });
			if (unit != null && !WindowUnits.DEFAULT.equals(unit)) {
				range.append("unit", unit.name().toLowerCase());
			}
			return range;
		}
	}

	/**
	 * The actual time unit to apply to a {@link Window}.
	 */
	public interface WindowUnit {

		String name();

		/**
		 * Converts the given time unit into a {@link WindowUnit}. Supported units are: days, hours, minutes, seconds, and
		 * milliseconds.
		 *
		 * @param timeUnit the time unit to convert, must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if the {@link TimeUnit} is {@literal null} or not supported for conversion.
		 */
		static WindowUnit from(TimeUnit timeUnit) {

			Assert.notNull(timeUnit, "TimeUnit must not be null");

			switch (timeUnit) {
				case DAYS:
					return WindowUnits.DAY;
				case HOURS:
					return WindowUnits.HOUR;
				case MINUTES:
					return WindowUnits.MINUTE;
				case SECONDS:
					return WindowUnits.SECOND;
				case MILLISECONDS:
					return WindowUnits.MILLISECOND;
			}

			throw new IllegalArgumentException(String.format("Cannot create WindowUnit from %s", timeUnit));
		}

		/**
		 * Converts the given chrono unit into a {@link WindowUnit}. Supported units are: years, weeks, months, days, hours,
		 * minutes, seconds, and millis.
		 *
		 * @param chronoUnit the chrono unit to convert, must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if the {@link TimeUnit} is {@literal null} or not supported for conversion.
		 */
		static WindowUnit from(ChronoUnit chronoUnit) {

			switch (chronoUnit) {
				case YEARS:
					return WindowUnits.YEAR;
				case WEEKS:
					return WindowUnits.WEEK;
				case MONTHS:
					return WindowUnits.MONTH;
				case DAYS:
					return WindowUnits.DAY;
				case HOURS:
					return WindowUnits.HOUR;
				case MINUTES:
					return WindowUnits.MINUTE;
				case SECONDS:
					return WindowUnits.SECOND;
				case MILLIS:
					return WindowUnits.MILLISECOND;
			}

			throw new IllegalArgumentException(String.format("Cannot create WindowUnit from %s", chronoUnit));
		}
	}

	/**
	 * Quick access to available {@link WindowUnit units}.
	 */
	public enum WindowUnits implements WindowUnit {
		DEFAULT, YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND
	}

	/**
	 * A fluent builder to create a {@link SetWindowFieldsOperation}.
	 *
	 * @author Christoph Strobl
	 */
	public static class SetWindowFieldsOperationBuilder {

		private Object partitionBy;
		private SortOperation sortOperation;
		private WindowOutput output;

		/**
		 * Specify the field to group by.
		 *
		 * @param fieldName must not be {@literal null} or null.
		 * @return this.
		 */
		public SetWindowFieldsOperationBuilder partitionByField(String fieldName) {

			Assert.hasText(fieldName, "Field name must not be empty or null");
			return partitionBy(Fields.field("$" + fieldName, fieldName));
		}

		/**
		 * Specify the {@link AggregationExpression expression} to group by.
		 *
		 * @param expression must not be {@literal null}.
		 * @return this.
		 */
		public SetWindowFieldsOperationBuilder partitionByExpression(AggregationExpression expression) {
			return partitionBy(expression);
		}

		/**
		 * Sort {@link Sort.Direction#ASC ascending} by the given fields.
		 *
		 * @param fields must not be {@literal null}.
		 * @return this.
		 */
		public SetWindowFieldsOperationBuilder sortBy(String... fields) {
			return sortBy(Sort.by(fields));
		}

		/**
		 * Set the sort order.
		 *
		 * @param sort must not be {@literal null}.
		 * @return this.
		 */
		public SetWindowFieldsOperationBuilder sortBy(Sort sort) {
			return sortBy(new SortOperation(sort));
		}

		/**
		 * Set the {@link SortOperation} to use.
		 *
		 * @param sort must not be {@literal null}.
		 * @return this.
		 */
		public SetWindowFieldsOperationBuilder sortBy(SortOperation sort) {

			Assert.notNull(sort, "SortOperation must not be null");

			this.sortOperation = sort;
			return this;
		}

		/**
		 * Define the actual output computation.
		 *
		 * @param output must not be {@literal null}.
		 * @return this.
		 */
		public SetWindowFieldsOperationBuilder output(WindowOutput output) {

			Assert.notNull(output, "WindowOutput must not be null");

			this.output = output;
			return this;
		}

		/**
		 * Add a field capturing the result of the given {@link AggregationExpression expression} to the output.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link WindowChoice}.
		 */
		public WindowChoice output(AggregationExpression expression) {

			return new WindowChoice() {

				@Nullable private Window window;

				@Override
				public As within(Window window) {

					Assert.notNull(window, "Window must not be null");

					this.window = window;
					return this;
				}

				@Override
				public SetWindowFieldsOperationBuilder as(String targetFieldName) {

					Assert.hasText(targetFieldName, "Target field name must not be empty or null");

					ComputedField computedField = new ComputedField(targetFieldName, expression, window);

					if (SetWindowFieldsOperationBuilder.this.output == null) {
						SetWindowFieldsOperationBuilder.this.output = new WindowOutput(computedField);
					} else {
						SetWindowFieldsOperationBuilder.this.output.append(computedField);
					}

					return SetWindowFieldsOperationBuilder.this;
				}
			};
		}

		/**
		 * Interface to capture field name used to capture the computation result.
		 */
		public interface As {

			/**
			 * Define the target name field name to hold the computation result.
			 *
			 * @param targetFieldName must not be {@literal null} or empty.
			 * @return the starting point {@link SetWindowFieldsOperationBuilder builder} instance.
			 */
			SetWindowFieldsOperationBuilder as(String targetFieldName);
		}

		/**
		 * Interface to capture an optional {@link Window} applicable to the field computation.
		 */
		public interface WindowChoice extends As {

			/**
			 * Specify calculation boundaries.
			 *
			 * @param window must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			As within(Window window);

		}

		/**
		 * Partition by a value that translates to a valid mongodb expression.
		 *
		 * @param value must not be {@literal null}.
		 * @return this.
		 */
		public SetWindowFieldsOperationBuilder partitionBy(Object value) {

			Assert.notNull(value, "Partition By must not be null");

			partitionBy = value;
			return this;
		}

		/**
		 * Obtain a new instance of {@link SetWindowFieldsOperation} with previously set arguments.
		 *
		 * @return new instance of {@link SetWindowFieldsOperation}.
		 */
		public SetWindowFieldsOperation build() {
			return new SetWindowFieldsOperation(partitionBy, sortOperation, output);
		}
	}
}

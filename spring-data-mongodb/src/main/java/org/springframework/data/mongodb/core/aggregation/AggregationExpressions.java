/*
 * Copyright 2016. the original author or authors.
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
import java.util.Map;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 * @since 1.10
 */
public interface AggregationExpressions {

	/**
	 * @author Christoph Strobl
	 */
	class SetOperators {

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static SetOperatorFactory arrayAsSet(String fieldRef) {
			return new SetOperatorFactory(fieldRef);
		}

		public static class SetOperatorFactory {

			private final String fieldRef;

			/**
			 * Creates new {@link SetOperatorFactory} for given {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 */
			public SetOperatorFactory(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				this.fieldRef = fieldRef;
			}

			/**
			 * Compares the previously mentioned field to one or more arrays and returns {@literal true} if they have the same
			 * distinct elements and {@literal false} otherwise.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetEquals isEqualTo(String... arrayReferences) {
				return SetEquals.arrayAsSet(fieldRef).isEqualTo(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and one or more arrays and returns an array that contains the
			 * elements that appear in every of those.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetIntersection intersects(String... arrayReferences) {
				return SetIntersection.arrayAsSet(fieldRef).intersects(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and one or more arrays and returns an array that contains the
			 * elements that appear in any of those.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetUnion union(String... arrayReferences) {
				return SetUnion.arrayAsSet(fieldRef).union(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and returns an array containing the elements that do not exist in
			 * the given {@literal arrayReference}.
			 *
			 * @param arrayReference must not be {@literal null}.
			 * @return
			 */
			public SetDifference differenceTo(String arrayReference) {
				return SetDifference.arrayAsSet(fieldRef).differenceTo(arrayReference);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if it is a subset of the given
			 * {@literal arrayReference}.
			 *
			 * @param arrayReference must not be {@literal null}.
			 * @return
			 */
			public SetIsSubset isSubsetOf(String arrayReference) {
				return SetIsSubset.arrayAsSet(fieldRef).isSubsetOf(arrayReference);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if any of the elements are
			 * {@literal true} and {@literal false} otherwise.
			 *
			 * @return
			 */
			public AnyElementTrue anyElementTrue() {
				return AnyElementTrue.arrayAsSet(fieldRef);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if no elements is {@literal false}.
			 *
			 * @return
			 */
			public AllElementsTrue allElementsTrue() {
				return AllElementsTrue.arrayAsSet(fieldRef);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class ArithmeticOperators {

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static ArithmeticOperatorFactory valueOf(String fieldRef) {
			return new ArithmeticOperatorFactory(fieldRef);
		}

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static ArithmeticOperatorFactory valueOf(AggregationExpression fieldRef) {
			return new ArithmeticOperatorFactory(fieldRef);
		}

		public static class ArithmeticOperatorFactory {

			private final String fieldRef;
			private final AggregationExpression expression;

			/**
			 * Creates new {@link ArithmeticOperatorFactory} for given {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 */
			public ArithmeticOperatorFactory(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				this.fieldRef = fieldRef;
				this.expression = null;
			}

			/**
			 * Creats new {@link ArithmeticOperatorFactory} for given {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 */
			public ArithmeticOperatorFactory(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				this.fieldRef = null;
				this.expression = expression;
			}

			/**
			 * Returns the absolute value of the associated number.
			 *
			 * @return
			 */
			public Abs abs() {
				return fieldRef != null ? Abs.absoluteValueOf(fieldRef) : Abs.absoluteValueOf(expression);
			}

			/**
			 * Adds value of {@literal fieldRef} to the associated number.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Add add(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createAdd().add(fieldRef);
			}

			/**
			 * Adds value of {@link AggregationExpression} to the associated number.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Add add(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createAdd().add(expression);
			}

			/**
			 * Adds given {@literal value} to the associated number.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Add add(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createAdd().add(value);
			}

			private Add createAdd() {
				return fieldRef != null ? Add.valueOf(fieldRef) : Add.valueOf(expression);
			}

			/**
			 * Returns the smallest integer greater than or equal to the assoicated number.
			 *
			 * @return
			 */
			public Ceil ceil() {
				return fieldRef != null ? Ceil.ceilValueOf(fieldRef) : Ceil.ceilValueOf(expression);
			}

			/**
			 * Divide associated number by number referenced via {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Divide divideBy(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createDivide().divideBy(fieldRef);
			}

			/**
			 * Divide associated number by number extracted via {@literal expression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Divide divideBy(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createDivide().divideBy(expression);
			}

			/**
			 * Divide associated number by given {@literal value}.
			 *
			 * @param value
			 * @return
			 */
			public Divide divideBy(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createDivide().divideBy(value);
			}

			private Divide createDivide() {
				return fieldRef != null ? Divide.valueOf(fieldRef) : Divide.valueOf(expression);
			}

			/**
			 * Raises Euler’s number (i.e. e ) to the associated number.
			 *
			 * @return
			 */
			public Exp exp() {
				return fieldRef != null ? Exp.expValueOf(fieldRef) : Exp.expValueOf(expression);
			}

			/**
			 * Returns the largest integer less than or equal to the associated number.
			 *
			 * @return
			 */
			public Floor floor() {
				return fieldRef != null ? Floor.floorValueOf(fieldRef) : Floor.floorValueOf(expression);
			}

			/**
			 * Calculates the natural logarithm ln (i.e loge) of the assoicated number.
			 *
			 * @return
			 */
			public Ln ln() {
				return fieldRef != null ? Ln.lnValueOf(fieldRef) : Ln.lnValueOf(expression);
			}

			/**
			 * Calculates the log of the associated number in the specified base referenced via {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Log log(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createLog().log(fieldRef);
			}

			/**
			 * Calculates the log of the associated number in the specified base extracted by given {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Log log(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createLog().log(fieldRef);
			}

			/**
			 * Calculates the log of a the associated number in the specified {@literal base}.
			 *
			 * @param base must not be {@literal null}.
			 * @return
			 */
			public Log log(Number base) {

				Assert.notNull(base, "Base must not be null!");
				return createLog().log(base);
			}

			private Log createLog() {
				return fieldRef != null ? Log.valueOf(fieldRef) : Log.valueOf(expression);
			}

			/**
			 * Calculates the log base 10 for the associated number.
			 *
			 * @return
			 */
			public Log10 log10() {
				return fieldRef != null ? Log10.log10ValueOf(fieldRef) : Log10.log10ValueOf(expression);
			}

			/**
			 * Divides the associated number by another and returns the remainder.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Mod mod(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createMod().mod(fieldRef);
			}

			/**
			 * Divides the associated number by another and returns the remainder.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Mod mod(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createMod().mod(expression);
			}

			/**
			 * Divides the associated number by another and returns the remainder.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Mod mod(Number value) {

				Assert.notNull(value, "Base must not be null!");
				return createMod().mod(value);
			}

			private Mod createMod() {
				return fieldRef != null ? Mod.valueOf(fieldRef) : Mod.valueOf(expression);
			}

			/**
			 * Multiplies the associated number with another.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createMultiply().multiplyBy(fieldRef);
			}

			/**
			 * Multiplies the associated number with another.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createMultiply().multiplyBy(expression);
			}

			/**
			 * Multiplies the associated number with another.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createMultiply().multiplyBy(value);
			}


			private Multiply createMultiply() {
				return fieldRef != null ? Multiply.valueOf(fieldRef) : Multiply.valueOf(expression);
			}

			/**
			 * Raises the associated number to the specified exponent.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Pow pow(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createPow().pow(fieldRef);
			}

			/**
			 * Raises the associated number to the specified exponent.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Pow pow(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createPow().pow(expression);
			}

			/**
			 * Raises the associated number to the specified exponent.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Pow pow(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createPow().pow(value);
			}

			private Pow createPow() {
				return fieldRef != null ? Pow.valueOf(fieldRef) : Pow.valueOf(expression);
			}

			/**
			 * Calculates the square root of the associated number.
			 *
			 * @return
			 */
			public Sqrt sqrt() {
				return fieldRef != null ? Sqrt.sqrtOf(fieldRef) : Sqrt.sqrtOf(expression);
			}

			/**
			 * Subtracts value of given from the associated number.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Subtract subtract(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createSubtract().subtract(fieldRef);
			}

			/**
			 * Subtracts value of given from the associated number.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Subtract subtract(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createSubtract().subtract(expression);
			}

			/**
			 * Subtracts value from the associated number.
			 *
			 * @param value
			 * @return
			 */
			public Subtract subtract(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createSubtract().subtract(value);
			}

			private Subtract createSubtract() {
				return fieldRef != null ? Subtract.valueOf(fieldRef) : Subtract.valueOf(expression);
			}

			/**
			 * Truncates a number to its integer.
			 *
			 * @return
			 */
			public Trunc trunc() {
				return fieldRef != null ? Trunc.truncValueOf(fieldRef) : Trunc.truncValueOf(expression);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	abstract class AbstractAggregationExpression implements AggregationExpression {

		private final Object value;

		protected AbstractAggregationExpression(Object value) {
			this.value = value;
		}

		@Override
		public DBObject toDbObject(AggregationOperationContext context) {
			return toDbObject(this.value, context);
		}

		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			Object valueToUse;
			if (value instanceof List) {

				List<Object> arguments = (List<Object>) value;
				List<Object> args = new ArrayList<Object>(arguments.size());

				for (Object val : arguments) {
					args.add(unpack(val, context));
				}
				valueToUse = args;
			} else if (value instanceof Map) {

				DBObject dbo = new BasicDBObject();
				for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
					dbo.put(entry.getKey(), unpack(entry.getValue(), context));
				}
				valueToUse = dbo;
			} else {
				valueToUse = unpack(value, context);
			}

			return new BasicDBObject(getMongoMethod(), valueToUse);
		}

		protected static List<Field> asFields(String... fieldRefs) {

			if (ObjectUtils.isEmpty(fieldRefs)) {
				return Collections.emptyList();
			}

			return Fields.fields(fieldRefs).asList();
		}

		private Object unpack(Object value, AggregationOperationContext context) {

			if (value instanceof AggregationExpression) {
				return ((AggregationExpression) value).toDbObject(context);
			}

			if (value instanceof Field) {
				return context.getReference((Field) value).toString();
			}

			return value;
		}

		protected List<Object> append(Object value) {

			if (this.value instanceof List) {

				List<Object> clone = new ArrayList<Object>((List) this.value);

				if (value instanceof List) {
					for (Object val : (List) value) {
						clone.add(val);
					}
				} else {
					clone.add(value);
				}
				return clone;
			}

			return Arrays.asList(this.value, value);
		}

		protected Object append(String key, Object value) {
			return null;
		}

		public abstract String getMongoMethod();
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetEquals extends AbstractAggregationExpression {

		private SetEquals(List<?> arrays) {
			super(arrays);
		}

		@Override
		public String getMongoMethod() {
			return "$setEquals";
		}

		public static SetEquals arrayAsSet(String arrayReference) {
			return new SetEquals(asFields(arrayReference));
		}

		public SetEquals isEqualTo(String... arrayReferences) {
			return new SetEquals(append(Fields.fields(arrayReferences).asList()));
		}

		public SetEquals isEqualTo(Object[] compareValue) {
			return new SetEquals(append(compareValue));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetIntersection extends AbstractAggregationExpression {

		private SetIntersection(List<?> arrays) {
			super(arrays);
		}

		@Override
		public String getMongoMethod() {
			return "$setIntersection";
		}

		public static SetIntersection arrayAsSet(String arrayReference) {
			return new SetIntersection(asFields(arrayReference));
		}

		public SetIntersection intersects(String... arrayReferences) {
			return new SetIntersection(append(asFields(arrayReferences)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetUnion extends AbstractAggregationExpression {

		private SetUnion(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setUnion";
		}

		public static SetUnion arrayAsSet(String arrayReference) {
			return new SetUnion(asFields(arrayReference));
		}

		public SetUnion union(String... arrayReferences) {
			return new SetUnion(append(asFields(arrayReferences)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetDifference extends AbstractAggregationExpression {

		private SetDifference(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setDifference";
		}

		public static SetDifference arrayAsSet(String arrayReference) {
			return new SetDifference(asFields(arrayReference));
		}

		public SetDifference differenceTo(String arrayReference) {
			return new SetDifference(append(Fields.field(arrayReference)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetIsSubset extends AbstractAggregationExpression {

		private SetIsSubset(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setIsSubset";
		}

		public static SetIsSubset arrayAsSet(String arrayReference) {
			return new SetIsSubset(asFields(arrayReference));
		}

		public SetIsSubset isSubsetOf(String arrayReference) {
			return new SetIsSubset(append(Fields.field(arrayReference)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class AnyElementTrue extends AbstractAggregationExpression {

		private AnyElementTrue(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$anyElementTrue";
		}

		public static AnyElementTrue arrayAsSet(String arrayReference) {
			return new AnyElementTrue(asFields(arrayReference));
		}

		public AnyElementTrue anyElementTrue() {
			return this;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class AllElementsTrue extends AbstractAggregationExpression {

		private AllElementsTrue(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$allElementsTrue";
		}

		public static AllElementsTrue arrayAsSet(String arrayReference) {
			return new AllElementsTrue(asFields(arrayReference));
		}

		public AllElementsTrue allElementsTrue() {
			return this;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Abs extends AbstractAggregationExpression {

		private Abs(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$abs";
		}

		public static Abs absoluteValueOf(String fieldRef) {
			return new Abs(Fields.field(fieldRef));
		}

		public static Abs absoluteValueOf(AggregationExpression expression) {
			return new Abs(expression);
		}

		public static Abs absoluteValueOf(Number value) {
			return new Abs(value);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Add extends AbstractAggregationExpression {

		protected Add(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$add";
		}

		public static Add valueOf(String fieldRef) {
			return new Add(asFields(fieldRef));
		}

		public static Add valueOf(AggregationExpression expression) {
			return new Add(Collections.singletonList(expression));
		}

		public static Add valueOf(Number value) {
			return new Add(Collections.singletonList(value));
		}

		public Add add(String fieldRef) {
			return new Add(append(Fields.field(fieldRef)));
		}

		public Add add(AggregationExpression expression) {
			return new Add(append(expression));
		}

		public Add add(Number value) {
			return new Add(append(value));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Ceil extends AbstractAggregationExpression {

		private Ceil(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$ceil";
		}

		public static Ceil ceilValueOf(String fieldRef) {
			return new Ceil(Fields.field(fieldRef));
		}

		public static Ceil ceilValueOf(AggregationExpression expression) {
			return new Ceil(expression);
		}

		public static Ceil ceilValueOf(Number value) {
			return new Ceil(value);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Divide extends AbstractAggregationExpression {

		private Divide(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$divide";
		}

		public static Divide valueOf(String fieldRef) {
			return new Divide(asFields(fieldRef));
		}

		public static Divide valueOf(AggregationExpression expression) {
			return new Divide(Collections.singletonList(expression));
		}

		public static Divide valueOf(Number value) {
			return new Divide(Collections.singletonList(value));
		}

		public Divide divideBy(String fieldRef) {
			return new Divide(append(Fields.field(fieldRef)));
		}

		public Divide divideBy(AggregationExpression expression) {
			return new Divide(append(expression));
		}

		public Divide divideBy(Number value) {
			return new Divide(append(value));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Exp extends AbstractAggregationExpression {

		protected Exp(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$exp";
		}

		public static Exp expValueOf(String fieldRef) {
			return new Exp(Fields.field(fieldRef));
		}

		public static Exp expValueOf(AggregationExpression expression) {
			return new Exp(expression);
		}

		public static Exp expValueOf(Number value) {
			return new Exp(value);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Floor extends AbstractAggregationExpression {

		protected Floor(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$floor";
		}

		public static Floor floorValueOf(String fieldRef) {
			return new Floor(Fields.field(fieldRef));
		}

		public static Floor floorValueOf(AggregationExpression expression) {
			return new Floor(expression);
		}

		public static Floor floorValueOf(Number value) {
			return new Floor(value);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Ln extends AbstractAggregationExpression {

		private Ln(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$ln";
		}

		public static Ln lnValueOf(String fieldRef) {
			return new Ln(Fields.field(fieldRef));
		}

		public static Ln lnValueOf(AggregationExpression expression) {
			return new Ln(expression);
		}

		public static Ln lnValueOf(Number value) {
			return new Ln(value);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Log extends AbstractAggregationExpression {

		private Log(List<?> values) {
			super(values);
		}

		@Override
		public String getMongoMethod() {
			return "$log";
		}

		public static Log valueOf(String fieldRef) {
			return new Log(asFields(fieldRef));
		}

		public static Log valueOf(AggregationExpression expression) {
			return new Log(Collections.singletonList(expression));
		}

		public static Log valueOf(Number value) {
			return new Log(Collections.singletonList(value));
		}

		public Log log(String fieldRef) {
			return new Log(append(Fields.field(fieldRef)));
		}

		public Log log(AggregationExpression expression) {
			return new Log(append(expression));
		}

		public Log log(Number base) {
			return new Log(append(base));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Log10 extends AbstractAggregationExpression {

		private Log10(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$log10";
		}

		public static Log10 log10ValueOf(String fieldRef) {
			return new Log10(Fields.field(fieldRef));
		}

		public static Log10 log10ValueOf(AggregationExpression expression) {
			return new Log10(expression);
		}

		public static Log10 log10ValueOf(Number value) {
			return new Log10(value);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Mod extends AbstractAggregationExpression {

		private Mod(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$mod";
		}

		public static Mod valueOf(String fieldRef) {
			return new Mod(asFields(fieldRef));
		}

		public static Mod valueOf(AggregationExpression expression) {
			return new Mod(Collections.singletonList(expression));
		}

		public static Mod valueOf(Number value) {
			return new Mod(Collections.singletonList(value));
		}

		public Mod mod(String fieldRef) {
			return new Mod(append(Fields.field(fieldRef)));
		}

		public Mod mod(AggregationExpression expression) {
			return new Mod(append(expression));
		}

		public Mod mod(Number base) {
			return new Mod(append(base));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Multiply extends AbstractAggregationExpression {

		private Multiply(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$multiply";
		}

		public static Multiply valueOf(String fieldRef) {
			return new Multiply(asFields(fieldRef));
		}

		public static Multiply valueOf(AggregationExpression expression) {
			return new Multiply(Collections.singletonList(expression));
		}

		public static Multiply valueOf(Number value) {
			return new Multiply(Collections.singletonList(value));
		}

		public Multiply multiplyBy(String fieldRef) {
			return new Multiply(append(Fields.field(fieldRef)));
		}

		public Multiply multiplyBy(AggregationExpression expression) {
			return new Multiply(append(expression));
		}

		public Multiply multiplyBy(Number value) {
			return new Multiply(append(value));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Pow extends AbstractAggregationExpression {

		private Pow(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$pow";
		}

		public static Pow valueOf(String fieldRef) {
			return new Pow(asFields(fieldRef));
		}

		public static Pow valueOf(AggregationExpression expression) {
			return new Pow(Collections.singletonList(expression));
		}

		public static Pow valueOf(Number value) {
			return new Pow(Collections.singletonList(value));
		}

		public Pow pow(String fieldRef) {
			return new Pow(append(Fields.field(fieldRef)));
		}

		public Pow pow(AggregationExpression expression) {
			return new Pow(append(expression));
		}

		public Pow pow(Number value) {
			return new Pow(append(value));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Sqrt extends AbstractAggregationExpression {

		private Sqrt(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$sqrt";
		}

		public static Sqrt sqrtOf(String fieldRef) {
			return new Sqrt(Fields.field(fieldRef));
		}

		public static Sqrt sqrtOf(AggregationExpression expression) {
			return new Sqrt(expression);
		}

		public static Sqrt sqrtOf(Number value) {
			return new Sqrt(value);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Subtract extends AbstractAggregationExpression {

		protected Subtract(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$subtract";
		}

		public static Subtract valueOf(String fieldRef) {
			return new Subtract(asFields(fieldRef));
		}

		public static Subtract valueOf(AggregationExpression expression) {
			return new Subtract(Collections.singletonList(expression));
		}

		public static Subtract valueOf(Number value) {
			return new Subtract(Collections.singletonList(value));
		}

		public Subtract subtract(String fieldRef) {
			return new Subtract(append(Fields.field(fieldRef)));
		}

		public Subtract subtract(AggregationExpression expression) {
			return new Subtract(append(expression));
		}

		public Subtract subtract(Number value) {
			return new Subtract(append(value));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class Trunc extends AbstractAggregationExpression {

		private Trunc(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$trunc";
		}

		public static Trunc truncValueOf(String fieldRef) {
			return new Trunc(Fields.field(fieldRef));
		}

		public static Trunc truncValueOf(AggregationExpression expression) {
			return new Trunc(expression);
		}

		public static Trunc truncValueOf(Number value) {
			return new Trunc(value);
		}
	}

	/**
	 * {@code $filter} {@link AggregationExpression} allows to select a subset of the array to return based on the
	 * specified condition.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	class Filter implements AggregationExpression {

		private Object input;
		private ExposedField as;
		private Object condition;

		private Filter() {
			// used by builder
		}

		/**
		 * Set the {@literal field} to apply the {@code $filter} to.
		 *
		 * @param field must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder filter(String field) {

			Assert.notNull(field, "Field must not be null!");
			return filter(Fields.field(field));
		}

		/**
		 * Set the {@literal field} to apply the {@code $filter} to.
		 *
		 * @param field must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder filter(Field field) {

			Assert.notNull(field, "Field must not be null!");
			return new FilterExpressionBuilder().filter(field);
		}

		/**
		 * Set the {@literal values} to apply the {@code $filter} to.
		 *
		 * @param values must not be {@literal null}.
		 * @return
		 */
		public static AsBuilder filter(List<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new FilterExpressionBuilder().filter(values);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDbObject(final AggregationOperationContext context) {

			return toFilter(new ExposedFieldsAggregationOperationContext(ExposedFields.from(as), context) {

				@Override
				public FieldReference getReference(Field field) {

					FieldReference ref = null;
					try {
						ref = context.getReference(field);
					} catch (Exception e) {
						// just ignore that one.
					}
					return ref != null ? ref : super.getReference(field);
				}
			});
		}

		private DBObject toFilter(AggregationOperationContext context) {

			DBObject filterExpression = new BasicDBObject();

			filterExpression.putAll(context.getMappedObject(new BasicDBObject("input", getMappedInput(context))));
			filterExpression.put("as", as.getTarget());

			filterExpression.putAll(context.getMappedObject(new BasicDBObject("cond", getMappedCondition(context))));

			return new BasicDBObject("$filter", filterExpression);
		}

		private Object getMappedInput(AggregationOperationContext context) {
			return input instanceof Field ? context.getReference((Field) input).toString() : input;
		}

		private Object getMappedCondition(AggregationOperationContext context) {

			if (!(condition instanceof AggregationExpression)) {
				return condition;
			}

			NestedDelegatingExpressionAggregationOperationContext nea = new NestedDelegatingExpressionAggregationOperationContext(
					context);
			return ((AggregationExpression) condition).toDbObject(nea);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface InputBuilder {

			/**
			 * Set the {@literal values} to apply the {@code $filter} to.
			 *
			 * @param array must not be {@literal null}.
			 * @return
			 */
			AsBuilder filter(List<?> array);

			/**
			 * Set the {@literal field} holding an array to apply the {@code $filter} to.
			 *
			 * @param field must not be {@literal null}.
			 * @return
			 */
			AsBuilder filter(Field field);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface AsBuilder {

			/**
			 * Set the {@literal variableName} for the elements in the input array.
			 *
			 * @param variableName must not be {@literal null}.
			 * @return
			 */
			ConditionBuilder as(String variableName);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface ConditionBuilder {

			/**
			 * Set the {@link AggregationExpression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(AggregationExpression expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(String expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(DBObject expression);
		}

		/**
		 * @author Christoph Strobl
		 */
		static final class FilterExpressionBuilder implements InputBuilder, AsBuilder, ConditionBuilder {

			private final Filter filter;

			FilterExpressionBuilder() {
				this.filter = new Filter();
			}

			public static InputBuilder newBuilder() {
				return new FilterExpressionBuilder();
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.InputBuilder#filter(java.util.List)
			 */
			@Override
			public AsBuilder filter(List<?> array) {

				Assert.notNull(array, "Array must not be null!");
				filter.input = new ArrayList<Object>(array);
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.InputBuilder#filter(org.springframework.data.mongodb.core.aggregation.Field)
			 */
			@Override
			public AsBuilder filter(Field field) {

				Assert.notNull(field, "Field must not be null!");
				filter.input = field;
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.AsBuilder#as(java.lang.String)
			 */
			@Override
			public ConditionBuilder as(String variableName) {

				Assert.notNull(variableName, "Variable name  must not be null!");
				filter.as = new ExposedField(variableName, true);
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(org.springframework.data.mongodb.core.aggregation.AggregationExpression)
			 */
			@Override
			public Filter by(AggregationExpression condition) {

				Assert.notNull(condition, "Condition must not be null!");
				filter.condition = condition;
				return filter;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(java.lang.String)
			 */
			@Override
			public Filter by(String expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(com.mongodb.DBObject)
			 */
			@Override
			public Filter by(DBObject expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}
		}
	}
}

/*
 * Copyright 2013 the original author or authors.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.util.DBObjectUtils;
import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.CompoundExpression;
import org.springframework.expression.spel.ast.FloatLiteral;
import org.springframework.expression.spel.ast.Indexer;
import org.springframework.expression.spel.ast.InlineList;
import org.springframework.expression.spel.ast.IntLiteral;
import org.springframework.expression.spel.ast.Literal;
import org.springframework.expression.spel.ast.LongLiteral;
import org.springframework.expression.spel.ast.MethodReference;
import org.springframework.expression.spel.ast.NullLiteral;
import org.springframework.expression.spel.ast.OpDivide;
import org.springframework.expression.spel.ast.OpMinus;
import org.springframework.expression.spel.ast.OpModulus;
import org.springframework.expression.spel.ast.OpMultiply;
import org.springframework.expression.spel.ast.OpPlus;
import org.springframework.expression.spel.ast.Operator;
import org.springframework.expression.spel.ast.PropertyOrFieldReference;
import org.springframework.expression.spel.ast.RealLiteral;
import org.springframework.expression.spel.ast.StringLiteral;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Renders the AST of a SpEL expression as a MongoDB Aggregation Framework projection expression.
 * 
 * @author Thomas Darimont
 */
enum SpelExpressionToMongoExpressionTransformer {

	INSTANCE;

	private List<SpelNodeConversion<? extends SpelNode>> conversions;

	/**
	 * Creates a new {@link SpelExpressionToMongoExpressionTransformer}.
	 */
	private SpelExpressionToMongoExpressionTransformer() {

		this.conversions = new ArrayList<SpelNodeConversion<? extends SpelNode>>();
		this.conversions.add(new OperatorNodeConversion());
		this.conversions.add(new LiteralNodeConversion());
		this.conversions.add(new IndexerNodeConversion());
		this.conversions.add(new InlineListNodeConversion());
		this.conversions.add(new PropertyOrFieldReferenceNodeConversion());
		this.conversions.add(new CompoundExpressionNodeConversion());
		this.conversions.add(new MethodReferenceNodeConversion());
	}

	/**
	 * Transforms the given SpEL expression string to a corresponding MongoDB Expression.
	 * 
	 * @param expression must be a SpEL expression.
	 * @return
	 */
	public Object transform(String expression) {
		return transform(expression, Aggregation.DEFAULT_CONTEXT, new Object[0]);
	}

	/**
	 * Transforms the given SpEL expression string to a corresponding MongoDB expression against the
	 * {@link Aggregation#DEFAULT_CONTEXT}.
	 * <p>
	 * Exposes the given @{code params} as <code>[0] ... [n]</code>.
	 * 
	 * @param expression must be a SpEL expression.
	 * @param params must not be {@literal null}
	 * @return
	 */
	public Object transform(String expression, Object... params) {

		return transform(expression, Aggregation.DEFAULT_CONTEXT, params);
	}

	/**
	 * Transforms the given SpEL expression string to a corresponding MongoDB expression against the given
	 * {@link AggregationOperationContext} {@code context}.
	 * <p>
	 * Exposes the given @{code params} as <code>[0] ... [n]</code>.
	 * 
	 * @param expression must not be {@literal null}
	 * @param context must not be {@literal null}
	 * @param params must not be {@literal null}
	 * @return
	 */
	public Object transform(String expression, AggregationOperationContext context, Object[] params) {

		Assert.notNull(expression, "expression must not be null!");

		return transform((SpelExpression) new SpelExpressionParser().parseExpression(expression), context, params);
	}

	/**
	 * Transforms the given SpEL expression to a corresponding MongoDB expression against the given
	 * {@link AggregationOperationContext} {@code context}.
	 * <p>
	 * Exposes the given @{code params} as <code>[0] ... [n]</code>.
	 * 
	 * @param expression must not be {@literal null}
	 * @param context must not be {@literal null}
	 * @param params must not be {@literal null}
	 * @return
	 */
	public Object transform(SpelExpression expression, AggregationOperationContext context, Object[] params) {

		Assert.notNull(params, "params must not be null!");

		return transform(expression, context, new ExpressionState(new StandardEvaluationContext(params)));
	}

	/**
	 * Transforms the given SpEL expression to a corresponding MongoDB expression against the given
	 * {@link AggregationOperationContext} {@code context} and the given {@link ExpressionState}.
	 * 
	 * @param expression
	 * @param aggregationContext
	 * @param expressionState
	 * @return
	 */
	public Object transform(SpelExpression expression, AggregationOperationContext aggregationContext,
			ExpressionState expressionState) {

		Assert.notNull(expression, "expression must not be null!");
		Assert.notNull(aggregationContext, "aggregationContext must not be null!");
		Assert.notNull(expressionState, "expressionState must not be null!");

		ExpressionTransformationContext expressionContext = new ExpressionTransformationContext(expression.getAST(), null,
				null, aggregationContext, expressionState);
		return doTransform(expressionContext);
	}

	/**
	 * @param spelNode
	 * @param context
	 * @return
	 */
	private Object doTransform(ExpressionTransformationContext context) {

		return lookupConversionFor(context.getCurrentNode()).convert(context);
	}

	/**
	 * Returns an appropriate {@link SpelNodeConversion} for the given {@code node}. Throws an
	 * {@link IllegalArgumentException} if no conversion could be found.
	 * 
	 * @param node
	 * @return the appropriate {@link SpelNodeConversion} for the given {@link SpelNode}.
	 */
	private SpelNodeConversion<? extends SpelNode> lookupConversionFor(SpelNode node) {

		for (SpelNodeConversion<? extends SpelNode> candidate : conversions) {
			if (candidate.supports(node)) {
				return candidate;
			}
		}

		throw new IllegalArgumentException("Unsupported Element: " + node + " Type: " + node.getClass()
				+ " You probably have a syntax error in your SpEL expression!");
	}

	/**
	 * Holds information about the current transformation context.
	 * 
	 * @author Thomas Darimont
	 */
	private static class ExpressionTransformationContext {

		private final SpelNode currentNode;

		private final SpelNode parentNode;

		private final Object previousOperationObject;

		private final AggregationOperationContext aggregationContext;

		private final ExpressionState expressionState;

		/**
		 * Creates a <code>ExpressionConversionContext<code>
		 * 
		 * @param currentNode, must not be {@literal null}
		 * @param parentNode
		 * @param previousOperationObject
		 * @param aggregationContext, must not be {@literal null}
		 * @param expressionState, must not be {@literal null}
		 */
		public ExpressionTransformationContext(SpelNode currentNode, SpelNode parentNode, Object previousOperationObject,
				AggregationOperationContext aggregationContext, ExpressionState expressionState) {

			Assert.notNull(currentNode, "currentNode must not be null!");
			Assert.notNull(aggregationContext, "aggregationContext must not be null!");
			Assert.notNull(expressionState, "expressionState must not be null!");

			this.currentNode = currentNode;
			this.parentNode = parentNode;
			this.previousOperationObject = previousOperationObject;
			this.aggregationContext = aggregationContext;
			this.expressionState = expressionState;
		}

		/**
		 * Creates a {@link ExpressionTransformationContext}.
		 * 
		 * @param child, must not be {@literal null}
		 * @param context, must not be {@literal null}
		 */
		public ExpressionTransformationContext(SpelNode currentNode, ExpressionTransformationContext context) {
			this(currentNode, context.getParentNode(), context.getPreviousOperationObject(), context.getAggregationContext(),
					context.getExpressionState());
		}

		public SpelNode getCurrentNode() {
			return currentNode;
		}

		public SpelNode getParentNode() {
			return parentNode;
		}

		public Object getPreviousOperationObject() {
			return previousOperationObject;
		}

		public AggregationOperationContext getAggregationContext() {
			return aggregationContext;
		}

		public ExpressionState getExpressionState() {
			return expressionState;
		}

		public boolean isPreviousOperationPresent() {
			return getPreviousOperationObject() != null;
		}

		/**
		 * Returns a {@link FieldReference} for the given {@code fieldName}. Checks whether a field with the given
		 * {@code fieldName} can be found in the {@link AggregationOperationContext}.
		 * 
		 * @param fieldName
		 * @return
		 */
		private FieldReference getFieldReference(String fieldName) {

			if (aggregationContext == null) {
				return null;
			}

			return aggregationContext.getReference(fieldName);
		}
	}

	/**
	 * Abstract base class for {@link SpelNode} to (Db)-object conversions.
	 * 
	 * @author Thomas Darimont
	 */
	static abstract class SpelNodeConversion<T extends SpelNode> {

		protected final Class<T> nodeType;

		public SpelNodeConversion(Class<T> nodeType) {
			this.nodeType = nodeType;
		}

		/**
		 * @param node
		 * @return true if {@literal this} conversion can be applied to the given {@code node}.
		 */
		protected boolean supports(SpelNode node) {
			return nodeType.isAssignableFrom(node.getClass());
		}

		/**
		 * Performs the actual conversion from {@link SpelNode} to the corresponding representation for MongoDB.
		 * 
		 * @param context
		 * @return
		 */
		abstract Object convert(ExpressionTransformationContext context);

		/**
		 * Extracts the argument list from the given {@code context}.
		 * 
		 * @param context
		 * @return
		 */
		protected static BasicDBList extractArgumentListFrom(DBObject context) {
			return (BasicDBList) context.get(context.keySet().iterator().next());
		}

		protected SpelExpressionToMongoExpressionTransformer getTransformer() {
			return INSTANCE;
		}
	}

	/**
	 * A {@link SpelNodeConversion} that converts arithmetic operations.
	 * 
	 * @author Thomas Darimont
	 */
	static class OperatorNodeConversion extends SpelNodeConversion<Operator> {

		private Map<String, String> arithmeticOperatorsSpelToMongoConversion = new HashMap<String, String>() {
			private static final long serialVersionUID = 1L;

			{
				put("+", "$add");
				put("-", "$subtract");
				put("*", "$multiply");
				put("/", "$divide");
				put("%", "$mod");
			}
		};

		public OperatorNodeConversion() {
			super(Operator.class);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		Object convert(ExpressionTransformationContext context) {

			Operator currentNode = Operator.class.cast(context.getCurrentNode());
			boolean unaryOperator = currentNode.getRightOperand() == null;

			Object operationObject = createOperationObjectAndAddToPreviousArgumentsIfNecessary(context, currentNode,
					unaryOperator);

			Object leftResult = convertPart(currentNode.getLeftOperand(), context, currentNode, operationObject);

			if (unaryOperator && currentNode instanceof OpMinus) {

				return convertUnaryMinusOp(context, leftResult);
			}

			// we deliberately ignore the RHS result
			convertPart(currentNode.getRightOperand(), context, currentNode, operationObject);

			return operationObject;
		}

		private Object convertPart(SpelNode currentNode, ExpressionTransformationContext context, Operator parentNode,
				Object operationObject) {

			return getTransformer().doTransform(
					new ExpressionTransformationContext(currentNode, parentNode, operationObject,
							context.getAggregationContext(), context.getExpressionState()));
		}

		private Object createOperationObjectAndAddToPreviousArgumentsIfNecessary(ExpressionTransformationContext context,
				Operator currentNode, boolean unaryOperator) {

			Object nextDbObject = new BasicDBObject(getOp(currentNode), new BasicDBList());

			if (context.isPreviousOperationPresent()) {

				if (currentNode.getClass().equals(context.getParentNode().getClass())) {

					// same operator applied in a row e.g. 1 + 2 + 3 carry on with the operation and render as $add: [1, 2 ,3]
					nextDbObject = context.getPreviousOperationObject();
				} else if (!unaryOperator) {

					// different operator -> add context object for next level to list if arguments of previous expression
					extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(nextDbObject);
				}
			}

			return nextDbObject;
		}

		private Object convertUnaryMinusOp(ExpressionTransformationContext context, Object leftResult) {

			Object result = leftResult instanceof Number ? leftResult : new BasicDBObject("$multiply", DBObjectUtils.dbList(
					-1, leftResult));

			if (leftResult != null && context.getPreviousOperationObject() != null) {
				extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(result);
			}

			return result;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.SpelNodeWrapper#supports(java.lang.Class)
		 */
		@Override
		protected boolean supports(SpelNode node) {
			return node instanceof OpMinus || node instanceof OpPlus || node instanceof OpMultiply
					|| node instanceof OpDivide || node instanceof OpModulus;
		}

		private String getOp(SpelNode node) {
			return supports(node) ? toMongoOperator((Operator) node) : null;
		}

		private String toMongoOperator(Operator operator) {
			return arithmeticOperatorsSpelToMongoConversion.get(operator.getOperatorName());
		}
	}

	/**
	 * A {@link SpelNodeConversion} that converts indexed expressions.
	 * 
	 * @author Thomas Darimont
	 */
	static class IndexerNodeConversion extends SpelNodeConversion<Indexer> {

		public IndexerNodeConversion() {
			super(Indexer.class);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		Object convert(ExpressionTransformationContext context) {

			Indexer currentNode = Indexer.class.cast(context.getCurrentNode());
			Object value = currentNode.getValue(context.getExpressionState());

			if (context.isPreviousOperationPresent()) {

				extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(value);
				return context.getPreviousOperationObject();
			}

			return value;
		}
	}

	/**
	 * A {@link SpelNodeConversion} that converts in-line list expressions.
	 * 
	 * @author Thomas Darimont
	 */
	static class InlineListNodeConversion extends SpelNodeConversion<InlineList> {

		public InlineListNodeConversion() {
			super(InlineList.class);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		Object convert(ExpressionTransformationContext context) {

			InlineList currentNode = InlineList.class.cast(context.getCurrentNode());

			if (currentNode.getChildCount() == 0) {
				return null;
			}

			// just take the first item
			ExpressionTransformationContext nestedExpressionContext = new ExpressionTransformationContext(
					currentNode.getChild(0), currentNode, null, context.getAggregationContext(), context.getExpressionState());
			return INSTANCE.doTransform(nestedExpressionContext);
		}

	}

	/**
	 * A {@link SpelNodeConversion} that converts property or field reference expressions.
	 * 
	 * @author Thomas Darimont
	 */
	static class PropertyOrFieldReferenceNodeConversion extends SpelNodeConversion<PropertyOrFieldReference> {

		public PropertyOrFieldReferenceNodeConversion() {
			super(PropertyOrFieldReference.class);
		}

		@Override
		Object convert(ExpressionTransformationContext context) {

			PropertyOrFieldReference currentNode = PropertyOrFieldReference.class.cast(context.getCurrentNode());
			FieldReference fieldReference = context.getFieldReference(currentNode.getName());

			if (context.isPreviousOperationPresent()) {
				extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(fieldReference.toString());
				return context.getPreviousOperationObject();
			}

			return fieldReference.toString();
		}
	}

	/**
	 * A {@link SpelNodeConversion} that converts literal expressions.
	 * 
	 * @author Thomas Darimont
	 */
	static class LiteralNodeConversion extends SpelNodeConversion<Literal> {

		public LiteralNodeConversion() {
			super(Literal.class);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		Object convert(ExpressionTransformationContext context) {

			Literal currentNode = Literal.class.cast(context.getCurrentNode());
			Object value = currentNode.getLiteralValue().getValue();

			if (context.isPreviousOperationPresent()) {

				if (context.getParentNode() instanceof OpMinus && ((OpMinus) context.getParentNode()).getRightOperand() == null) {
					// unary minus operator
					return NumberUtils.convertNumberToTargetClass(((Number) value).doubleValue() * -1,
							(Class<Number>) value.getClass()); // retain type, e.g. int to -int
				}

				extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(value);
				return context.getPreviousOperationObject();
			}

			return value;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.SpelNodeWrapper#supports(org.springframework.expression.spel.SpelNode)
		 */
		@Override
		protected boolean supports(SpelNode node) {
			return node instanceof FloatLiteral || node instanceof RealLiteral || node instanceof IntLiteral
					|| node instanceof LongLiteral || node instanceof StringLiteral || node instanceof NullLiteral;
		}
	}

	/**
	 * A {@link SpelNodeConversion} that converts method reference expressions.
	 * 
	 * @author Thomas Darimont
	 */
	static class MethodReferenceNodeConversion extends SpelNodeConversion<MethodReference> {

		private Map<String, String> namedFunctionToMongoExpressionMap = new HashMap<String, String>() {
			private static final long serialVersionUID = 1L;

			{
				put("concat", "$concat"); // Concatenates two strings.
				put("strcasecmp", "$strcasecmp"); // Compares two strings and returns an integer that reflects the comparison.
				put("substr", "$substr"); // Takes a string and returns portion of that string.
				put("toLower", "$toLower"); // Converts a string to lowercase.
				put("toUpper", "$toUpper"); // Converts a string to uppercase.

				put("dayOfYear", "$dayOfYear"); // Converts a date to a number between 1 and 366.
				put("dayOfMonth", "$dayOfMonth"); // Converts a date to a number between 1 and 31.
				put("dayOfWeek", "$dayOfWeek"); // Converts a date to a number between 1 and 7.
				put("year", "$year"); // Converts a date to the full year.
				put("month", "$month"); // Converts a date into a number between 1 and 12.
				put("week", "$week"); // Converts a date into a number between 0 and 53
				put("hour", "$hour"); // Converts a date into a number between 0 and 23.
				put("minute", "$minute"); // Converts a date into a number between 0 and 59.
				put("second", "$second"); // Converts a date into a number between 0 and 59. May be 60 to account for leap
																	// seconds.
				put("millisecond", "$millisecond"); // Returns the millisecond portion of a date as an integer between 0 and
																						// 999.
			}
		};

		public MethodReferenceNodeConversion() {
			super(MethodReference.class);
		}

		private String getMongoFunctionFor(String methodName) {
			return namedFunctionToMongoExpressionMap.get(methodName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		Object convert(ExpressionTransformationContext context) {

			MethodReference currentNode = MethodReference.class.cast(context.getCurrentNode());
			String stringAST = currentNode.toStringAST();
			String methodName = stringAST.substring(0, stringAST.indexOf('('));
			String mongoFunction = getMongoFunctionFor(methodName);

			List<Object> args = new ArrayList<Object>();
			for (int i = 0; i < currentNode.getChildCount(); i++) {
				args.add(getTransformer().doTransform(new ExpressionTransformationContext(currentNode.getChild(i), context)));
			}

			BasicDBObject functionObject = new BasicDBObject(mongoFunction, DBObjectUtils.dbList(args.toArray()));

			if (context.isPreviousOperationPresent()) {
				extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(functionObject);
				return context.getPreviousOperationObject();
			}

			return functionObject;
		}
	}

	/**
	 * A {@link SpelNodeConversion} that converts method compound expressions.
	 * 
	 * @author Thomas Darimont
	 */
	static class CompoundExpressionNodeConversion extends SpelNodeConversion<CompoundExpression> {

		public CompoundExpressionNodeConversion() {
			super(CompoundExpression.class);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionToMongoExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		Object convert(ExpressionTransformationContext context) {

			CompoundExpression currentNode = CompoundExpression.class.cast(context.getCurrentNode());

			if (currentNode.getChildCount() > 0 && !(currentNode.getChild(0) instanceof Indexer)) {
				// we have a property path expression like: foo.bar -> render as reference
				return context.getFieldReference(currentNode.toStringAST()).toString();
			}

			Object value = currentNode.getValue(context.getExpressionState());

			if (context.isPreviousOperationPresent()) {
				extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(value);
				return context.getPreviousOperationObject();
			}

			return value;
		}
	}
}

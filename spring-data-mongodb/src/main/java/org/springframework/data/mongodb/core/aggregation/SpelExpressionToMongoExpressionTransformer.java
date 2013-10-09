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

	private Map<String, String> functionsSpelToMongoConversion = new HashMap<String, String>() {
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
			put("week", "$week"); // Cput("foo","onverts a date into a number between 0 and 53
			put("hour", "$hour"); // Converts a date into a number between 0 and 23.
			put("minute", "$minute"); // Converts a date into a number between 0 and 59.
			put("second", "$second"); // Converts a date into a number between 0 and 59. May be 60 to account for leap
																// seconds.
			put("millisecond", "$millisecond"); // Returns the millisecond portion of a date as an integer between 0 and 999.
		}
	};

	public Object transform(String expression) {
		return transform(expression, Aggregation.DEFAULT_CONTEXT, new Object[0]);
	}

	public Object transform(String expression, Object... params) {
		return transform(expression, Aggregation.DEFAULT_CONTEXT, params);
	}

	public Object transform(String expression, AggregationOperationContext context, Object[] params) {
		return transform((SpelExpression) new SpelExpressionParser().parseExpression(expression), context, params);
	}

	public Object transform(SpelExpression expression, AggregationOperationContext context, Object[] params) {
		return transform(expression, context, new ExpressionState(new StandardEvaluationContext(params)));
	}

	public Object transform(SpelExpression expression, AggregationOperationContext aggregationContext,
			ExpressionState expressionState) {

		ExpressionConversionContext expressionContext = new ExpressionConversionContext(null, null, aggregationContext,
				expressionState);
		return convertSpelNodeToMongoObjectExpression(expression.getAST(), expressionContext);
	}

	private Object convertSpelNodeToMongoObjectExpression(SpelNode spelNode, ExpressionConversionContext context) {

		if (isOperatorNode(spelNode)) {
			return convertOperatorNode((Operator) spelNode, context);
		}

		if (isValueLiteral(spelNode)) {
			return convertValueLiteralNode((Literal) spelNode, context);
		}

		if (isPropertyOrFieldReference(spelNode)) {
			return convertPropertyOrFieldReferenceNode((PropertyOrFieldReference) spelNode, context);
		}

		if (isIndexerNode(spelNode)) {
			return convertIndexerExpression((Indexer) spelNode, context);
		}

		if (isInlistNode(spelNode)) {
			return convertInlineListNode((InlineList) spelNode, context);
		}

		if (isCompoundExpression(spelNode)) {
			return convertCompoundExpression((CompoundExpression) spelNode, context);
		}

		if (isMethodReference(spelNode)) {
			return convertMethodReference((MethodReference) spelNode, context);
		}

		throw new IllegalArgumentException("Unsupported Element: " + spelNode + " Type: " + spelNode.getClass()
				+ " You probably have a syntax error in your SpEL expression!");
	}

	private boolean isMethodReference(SpelNode spelNode) {
		return spelNode instanceof MethodReference;
	}

	private boolean isCompoundExpression(SpelNode spelNode) {
		return spelNode instanceof CompoundExpression;
	}

	private boolean isPropertyOrFieldReference(SpelNode node) {
		return node instanceof PropertyOrFieldReference;
	}

	private boolean isValueLiteral(SpelNode node) {
		return node instanceof FloatLiteral || node instanceof RealLiteral || node instanceof IntLiteral
				|| node instanceof LongLiteral || node instanceof StringLiteral;
	}

	private boolean isOperatorNode(SpelNode node) {
		return node instanceof OpMinus || node instanceof OpPlus || node instanceof OpMultiply || node instanceof OpDivide
				|| node instanceof OpModulus;
	}

	private boolean isInlistNode(SpelNode node) {
		return node instanceof InlineList;
	}

	private boolean isIndexerNode(SpelNode node) {
		return node instanceof Indexer;
	}

	private String toMongoOperator(SpelNode node) {
		return arithmeticOperatorsSpelToMongoConversion.get(((Operator) node).getOperatorName());
	}

	private Object convertIndexerExpression(Indexer indexer, ExpressionConversionContext context) {

		Object value = indexer.getValue(context.getExpressionState());

		if (context.isPreviousOperationPresent()) {
			extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(value);
			return context.getPreviousOperationObject();
		}

		return value;
	}

	private Object convertInlineListNode(InlineList list, ExpressionConversionContext context) {

		if (list.getChildCount() == 0) {
			return null;
		}

		// just take the first item
		ExpressionConversionContext nestedExpressionContext = new ExpressionConversionContext(list, null,
				context.getAggregationContext(), context.getExpressionState());
		return convertSpelNodeToMongoObjectExpression(list.getChild(0), nestedExpressionContext);
	}

	private Object convertPropertyOrFieldReferenceNode(PropertyOrFieldReference propertyOrFieldReference,
			ExpressionConversionContext context) {

		FieldReference fieldReference = context.getFieldReference(propertyOrFieldReference.getName());

		if (context.isPreviousOperationPresent()) {
			extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(fieldReference.toString());
			return context.getPreviousOperationObject();
		}

		return fieldReference.toString();
	}

	private Object convertValueLiteralNode(Literal literal, ExpressionConversionContext context) {

		Object value = literal.getLiteralValue().getValue();

		if (context.isPreviousOperationPresent()) {

			if (context.getParentNode() instanceof OpMinus && ((OpMinus) context.getParentNode()).getRightOperand() == null) {
				// unary minus operator
				return NumberUtils.convertNumberToTargetClass(((Number) value).doubleValue() * -1,
						(Class<Number>) value.getClass());
			}

			extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(value);
			return context.getPreviousOperationObject();
		}

		return value;
	}

	private Object convertOperatorNode(Operator currentOperator, ExpressionConversionContext context) {

		boolean unaryOperator = currentOperator.getRightOperand() == null;
		Object nextDbObject = new BasicDBObject(getOp(currentOperator), new BasicDBList());

		if (context.isPreviousOperationPresent()) {
			if (currentOperator.getClass().equals(context.getParentNode().getClass())) {
				// same operator applied in a row e.g. 1 + 2 + 3 carry on with the operation and render as $add: [1, 2 ,3]
				nextDbObject = context.getPreviousOperationObject();
			} else if (!unaryOperator) {
				// different operator -> add context object for next level to list if arguments of previous expression
				extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(nextDbObject);
			}
		}

		Object leftResult = convertSpelNodeToMongoObjectExpression(
				currentOperator.getLeftOperand(),
				new ExpressionConversionContext(currentOperator, nextDbObject, context.getAggregationContext(), context
						.getExpressionState()));

		if (unaryOperator && currentOperator instanceof OpMinus) {
			return convertUnaryMinusOperator(context.getPreviousOperationObject(), leftResult);
		}

		// we deliberately ignore the RHS result
		convertSpelNodeToMongoObjectExpression(currentOperator.getRightOperand(), new ExpressionConversionContext(
				currentOperator, nextDbObject, context.getAggregationContext(), context.getExpressionState()));

		return nextDbObject;
	}

	private Object convertUnaryMinusOperator(Object dbObjectOrPlainValue, Object leftResult) {

		Object result = leftResult instanceof Number ? leftResult : new BasicDBObject("$multiply", DBObjectUtils.dbList(-1,
				leftResult));

		if (dbObjectOrPlainValue != null) {
			extractArgumentListFrom((DBObject) dbObjectOrPlainValue).add(result);
		}

		return result;
	}

	private Object convertMethodReference(MethodReference methodReference, ExpressionConversionContext context) {

		String stringAST = methodReference.toStringAST();
		String methodName = stringAST.substring(0, stringAST.indexOf('('));
		String mongoFunction = getMongoFunctionFor(methodName);

		List<Object> args = new ArrayList<Object>();
		for (int i = 0; i < methodReference.getChildCount(); i++) {
			args.add(convertSpelNodeToMongoObjectExpression(methodReference.getChild(i), context));
		}

		BasicDBObject functionObject = new BasicDBObject(mongoFunction, DBObjectUtils.dbList(args.toArray()));

		if (context.isPreviousOperationPresent()) {
			extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(functionObject);
			return context.getPreviousOperationObject();
		}

		return functionObject;
	}

	private String getMongoFunctionFor(String methodName) {
		return functionsSpelToMongoConversion.get(methodName);
	}

	private Object convertCompoundExpression(CompoundExpression compoundExpression, ExpressionConversionContext context) {

		if (compoundExpression.getChildCount() > 0 && !isIndexerNode(compoundExpression.getChild(0))) {
			// we have a property path expression like: foo.bar -> render as reference
			return context.getFieldReference(compoundExpression.toStringAST()).toString();
		}

		Object value = compoundExpression.getValue(context.getExpressionState());

		if (context.isPreviousOperationPresent()) {
			extractArgumentListFrom((DBObject) context.getPreviousOperationObject()).add(value);
			return context.getPreviousOperationObject();
		}

		return value;
	}

	private BasicDBList extractArgumentListFrom(DBObject context) {
		return (BasicDBList) context.get(context.keySet().iterator().next());
	}

	private String getOp(SpelNode node) {
		return isOperatorNode(node) ? toMongoOperator(node) : null;
	}

	private static class ExpressionConversionContext {

		private final SpelNode parentNode;
		private final Object dbObjectOrPlainValue;
		private final AggregationOperationContext aggregationContext;
		private final ExpressionState expressionState;

		public ExpressionConversionContext(SpelNode parentNode, Object dbObjectOrPlainValue,
				AggregationOperationContext aggregationContext, ExpressionState expressionState) {
			this.parentNode = parentNode;
			this.dbObjectOrPlainValue = dbObjectOrPlainValue;
			this.aggregationContext = aggregationContext;
			this.expressionState = expressionState;
		}

		public SpelNode getParentNode() {
			return parentNode;
		}

		public Object getPreviousOperationObject() {
			return dbObjectOrPlainValue;
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

		private FieldReference getFieldReference(String fieldName) {

			if (aggregationContext == null) {
				return null;
			}

			return aggregationContext.getReference(fieldName);
		}
	}
}

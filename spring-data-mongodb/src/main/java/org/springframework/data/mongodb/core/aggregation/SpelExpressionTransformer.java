/*
 * Copyright 2013-2016 the original author or authors.
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

import org.bson.Document;
import org.springframework.core.GenericTypeResolver;
import org.springframework.data.mongodb.core.spel.ExpressionNode;
import org.springframework.data.mongodb.core.spel.ExpressionTransformationContextSupport;
import org.springframework.data.mongodb.core.spel.LiteralNode;
import org.springframework.data.mongodb.core.spel.MethodReferenceNode;
import org.springframework.data.mongodb.core.spel.MethodReferenceNode.AggregationMethodReference;
import org.springframework.data.mongodb.core.spel.MethodReferenceNode.AggregationMethodReference.ArgumentType;
import org.springframework.data.mongodb.core.spel.NotOperatorNode;
import org.springframework.data.mongodb.core.spel.OperatorNode;
import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.ast.CompoundExpression;
import org.springframework.expression.spel.ast.ConstructorReference;
import org.springframework.expression.spel.ast.Indexer;
import org.springframework.expression.spel.ast.InlineList;
import org.springframework.expression.spel.ast.InlineMap;
import org.springframework.expression.spel.ast.OperatorNot;
import org.springframework.expression.spel.ast.PropertyOrFieldReference;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;

/**
 * Renders the AST of a SpEL expression as a MongoDB Aggregation Framework projection expression.
 * 
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
class SpelExpressionTransformer implements AggregationExpressionTransformer {

	// TODO: remove explicit usage of a configuration once SPR-11031 gets fixed
	private static final SpelParserConfiguration CONFIG = new SpelParserConfiguration(false, false);
	private static final SpelExpressionParser PARSER = new SpelExpressionParser(CONFIG);
	private final List<ExpressionNodeConversion<? extends ExpressionNode>> conversions;

	/**
	 * Creates a new {@link SpelExpressionTransformer}.
	 */
	public SpelExpressionTransformer() {

		List<ExpressionNodeConversion<? extends ExpressionNode>> conversions = new ArrayList<ExpressionNodeConversion<? extends ExpressionNode>>();
		conversions.add(new OperatorNodeConversion(this));
		conversions.add(new LiteralNodeConversion(this));
		conversions.add(new IndexerNodeConversion(this));
		conversions.add(new InlineListNodeConversion(this));
		conversions.add(new PropertyOrFieldReferenceNodeConversion(this));
		conversions.add(new CompoundExpressionNodeConversion(this));
		conversions.add(new MethodReferenceNodeConversion(this));
		conversions.add(new NotOperatorNodeConversion(this));
		conversions.add(new ValueRetrievingNodeConversion(this));

		this.conversions = Collections.unmodifiableList(conversions);
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
	public Object transform(String expression, AggregationOperationContext context, Object... params) {

		Assert.notNull(expression, "Expression must not be null!");
		Assert.notNull(context, "AggregationOperationContext must not be null!");
		Assert.notNull(params, "Parameters must not be null!");

		SpelExpression spelExpression = (SpelExpression) PARSER.parseExpression(expression);
		ExpressionState state = new ExpressionState(new StandardEvaluationContext(params), CONFIG);
		ExpressionNode node = ExpressionNode.from(spelExpression.getAST(), state);

		return transform(new AggregationExpressionTransformationContext<ExpressionNode>(node, null, null, context));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.spel.ExpressionTransformer#transform(org.springframework.data.mongodb.core.spel.ExpressionTransformationContextSupport)
	 */
	public Object transform(AggregationExpressionTransformationContext<ExpressionNode> context) {
		return lookupConversionFor(context.getCurrentNode()).convert(context);
	}

	/**
	 * Returns an appropriate {@link ExpressionNodeConversion} for the given {@code node}. Throws an
	 * {@link IllegalArgumentException} if no conversion could be found.
	 * 
	 * @param node
	 * @return the appropriate {@link ExpressionNodeConversion} for the given {@link ExpressionNode}.
	 */
	@SuppressWarnings("unchecked")
	private ExpressionNodeConversion<ExpressionNode> lookupConversionFor(ExpressionNode node) {

		for (ExpressionNodeConversion<? extends ExpressionNode> candidate : conversions) {
			if (candidate.supports(node)) {
				return (ExpressionNodeConversion<ExpressionNode>) candidate;
			}
		}

		throw new IllegalArgumentException("Unsupported Element: " + node + " Type: " + node.getClass()
				+ " You probably have a syntax error in your SpEL expression!");
	}

	/**
	 * Abstract base class for {@link SpelNode} to (Db)-object conversions.
	 * 
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 */
	private static abstract class ExpressionNodeConversion<T extends ExpressionNode>
			implements AggregationExpressionTransformer {

		private final AggregationExpressionTransformer transformer;
		private final Class<? extends ExpressionNode> nodeType;

		/**
		 * Creates a new {@link ExpressionNodeConversion}.
		 * 
		 * @param transformer must not be {@literal null}.
		 */
		@SuppressWarnings("unchecked")
		public ExpressionNodeConversion(AggregationExpressionTransformer transformer) {

			Assert.notNull(transformer, "Transformer must not be null!");

			this.nodeType = (Class<? extends ExpressionNode>) GenericTypeResolver.resolveTypeArgument(this.getClass(),
					ExpressionNodeConversion.class);
			this.transformer = transformer;
		}

		/**
		 * Returns whether the current conversion supports the given {@link ExpressionNode}. By default we will match the
		 * node type against the genric type the subclass types the type parameter to.
		 * 
		 * @param node will never be {@literal null}.
		 * @return true if {@literal this} conversion can be applied to the given {@code node}.
		 */
		protected boolean supports(ExpressionNode node) {
			return nodeType.equals(node.getClass());
		}

		/**
		 * Triggers the transformation for the given {@link ExpressionNode} and the given current context.
		 * 
		 * @param node must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 * @return
		 */
		protected Object transform(ExpressionNode node, AggregationExpressionTransformationContext<?> context) {

			Assert.notNull(node, "ExpressionNode must not be null!");
			Assert.notNull(context, "AggregationExpressionTransformationContext must not be null!");

			return transform(node, context.getParentNode(), null, context);
		}

		/**
		 * Triggers the transformation with the given new {@link ExpressionNode}, new parent node, the current operation and
		 * the previous context.
		 * 
		 * @param node must not be {@literal null}.
		 * @param parent
		 * @param operation
		 * @param context must not be {@literal null}.
		 * @return
		 */
		protected Object transform(ExpressionNode node, ExpressionNode parent, Document operation,
				AggregationExpressionTransformationContext<?> context) {

			Assert.notNull(node, "ExpressionNode must not be null!");
			Assert.notNull(context, "AggregationExpressionTransformationContext must not be null!");

			return transform(new AggregationExpressionTransformationContext<ExpressionNode>(node, parent, operation,
					context.getAggregationContext()));
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.NodeConversion#transform(org.springframework.data.mongodb.core.aggregation.AggregationExpressionTransformer.AggregationExpressionTransformationContext)
		 */
		@Override
		public Object transform(AggregationExpressionTransformationContext<ExpressionNode> context) {
			return transformer.transform(context);
		}

		/**
		 * Performs the actual conversion from {@link SpelNode} to the corresponding representation for MongoDB.
		 * 
		 * @param context
		 * @return
		 */
		protected abstract Object convert(AggregationExpressionTransformationContext<T> context);
	}

	/**
	 * A {@link ExpressionNodeConversion} that converts arithmetic operations.
	 * 
	 * @author Thomas Darimont
	 */
	private static class OperatorNodeConversion extends ExpressionNodeConversion<OperatorNode> {

		public OperatorNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		protected Object convert(AggregationExpressionTransformationContext<OperatorNode> context) {

			OperatorNode currentNode = context.getCurrentNode();
			Document operationObject = createOperationObjectAndAddToPreviousArgumentsIfNecessary(context, currentNode);

			if (currentNode.isLogicalOperator()) {

				for (ExpressionNode expressionNode : currentNode) {
					transform(expressionNode, currentNode, operationObject, context);
				}

				return operationObject;
			}

			Object leftResult = transform(currentNode.getLeft(), currentNode, operationObject, context);

			if (currentNode.isUnaryMinus()) {
				return convertUnaryMinusOp(context, leftResult);
			}

			// we deliberately ignore the RHS result
			transform(currentNode.getRight(), currentNode, operationObject, context);

			return operationObject;
		}

		private Document createOperationObjectAndAddToPreviousArgumentsIfNecessary(
				AggregationExpressionTransformationContext<OperatorNode> context, OperatorNode currentNode) {

			Document nextDocument = new Document(currentNode.getMongoOperator(), new ArrayList<Object>());

			if (!context.hasPreviousOperation()) {
				return nextDocument;
			}

			if (context.parentIsSameOperation()) {

				// same operator applied in a row e.g. 1 + 2 + 3 carry on with the operation and render as $add: [1, 2 ,3]
				nextDocument = context.getPreviousOperationObject();
			} else if (!currentNode.isUnaryOperator()) {

				// different operator -> add context object for next level to list if arguments of previous expression
				context.addToPreviousOperation(nextDocument);
			}

			return nextDocument;
		}

		private Object convertUnaryMinusOp(ExpressionTransformationContextSupport<OperatorNode> context,
				Object leftResult) {

			Object result = leftResult instanceof Number ? leftResult
					: new Document("$multiply", Arrays.<Object> asList(Integer.valueOf(-1), leftResult));

			if (leftResult != null && context.hasPreviousOperation()) {
				context.addToPreviousOperation(result);
			}

			return result;
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#supports(java.lang.Class)
		 */
		@Override
		protected boolean supports(ExpressionNode node) {
			return node.isMathematicalOperation() || node.isLogicalOperator();
		}
	}

	/**
	 * A {@link ExpressionNodeConversion} that converts indexed expressions.
	 * 
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 */
	private static class IndexerNodeConversion extends ExpressionNodeConversion<ExpressionNode> {

		public IndexerNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		protected Object convert(AggregationExpressionTransformationContext<ExpressionNode> context) {
			return context.addToPreviousOrReturn(context.getCurrentNode().getValue());
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.NodeConversion#supports(org.springframework.data.mongodb.core.spel.ExpressionNode)
		 */
		@Override
		protected boolean supports(ExpressionNode node) {
			return node.isOfType(Indexer.class);
		}
	}

	/**
	 * A {@link ExpressionNodeConversion} that converts in-line list expressions.
	 * 
	 * @author Thomas Darimont
	 */
	private static class InlineListNodeConversion extends ExpressionNodeConversion<ExpressionNode> {

		public InlineListNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		protected Object convert(AggregationExpressionTransformationContext<ExpressionNode> context) {

			ExpressionNode currentNode = context.getCurrentNode();

			if (!currentNode.hasChildren()) {
				return null;
			}

			// just take the first item
			return transform(currentNode.getChild(0), currentNode, null, context);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.NodeConversion#supports(org.springframework.data.mongodb.core.spel.ExpressionNode)
		 */
		@Override
		protected boolean supports(ExpressionNode node) {
			return node.isOfType(InlineList.class);
		}
	}

	/**
	 * A {@link ExpressionNodeConversion} that converts property or field reference expressions.
	 * 
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 */
	private static class PropertyOrFieldReferenceNodeConversion extends ExpressionNodeConversion<ExpressionNode> {

		public PropertyOrFieldReferenceNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.NodeConversion#convert(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionTransformationContext)
		 */
		@Override
		protected Object convert(AggregationExpressionTransformationContext<ExpressionNode> context) {

			String fieldReference = context.getFieldReference().toString();
			return context.addToPreviousOrReturn(fieldReference);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.NodeConversion#supports(org.springframework.data.mongodb.core.spel.ExpressionNode)
		 */
		@Override
		protected boolean supports(ExpressionNode node) {
			return node.isOfType(PropertyOrFieldReference.class);
		}
	}

	/**
	 * A {@link ExpressionNodeConversion} that converts literal expressions.
	 * 
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 */
	private static class LiteralNodeConversion extends ExpressionNodeConversion<LiteralNode> {

		public LiteralNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		@SuppressWarnings("unchecked")
		protected Object convert(AggregationExpressionTransformationContext<LiteralNode> context) {

			LiteralNode node = context.getCurrentNode();
			Object value = node.getValue();

			if (context.hasPreviousOperation()) {

				if (node.isUnaryMinus(context.getParentNode())) {
					// unary minus operator
					return NumberUtils.convertNumberToTargetClass(((Number) value).doubleValue() * -1,
							(Class<Number>) value.getClass()); // retain type, e.g. int to -int
				}

				return context.addToPreviousOperation(value);
			}

			return value;
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#supports(org.springframework.expression.spel.SpelNode)
		 */
		@Override
		protected boolean supports(ExpressionNode node) {
			return node.isLiteral();
		}
	}

	/**
	 * A {@link ExpressionNodeConversion} that converts method reference expressions.
	 * 
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 */
	private static class MethodReferenceNodeConversion extends ExpressionNodeConversion<MethodReferenceNode> {

		public MethodReferenceNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		protected Object convert(AggregationExpressionTransformationContext<MethodReferenceNode> context) {

			MethodReferenceNode node = context.getCurrentNode();
			AggregationMethodReference methodReference = node.getMethodReference();

			Object args = null;

			if (ObjectUtils.nullSafeEquals(methodReference.getArgumentType(), ArgumentType.SINGLE)) {
				args = transform(node.getChild(0), context);
			} else if (ObjectUtils.nullSafeEquals(methodReference.getArgumentType(), ArgumentType.MAP)) {

				Document dbo = new Document();
				for (int i = 0; i < methodReference.getArgumentMap().length; i++) {
					dbo.put(methodReference.getArgumentMap()[i], transform(node.getChild(i), context));
				}
				args = dbo;
			} else {

				List<Object> argList = new ArrayList<Object>();

				for (ExpressionNode childNode : node) {
					argList.add(transform(childNode, context));
				}

				args = argList;
			}

			return context.addToPreviousOrReturn(new Document(methodReference.getMongoOperator(), args));
		}
	}

	/**
	 * A {@link ExpressionNodeConversion} that converts method compound expressions.
	 * 
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 */
	private static class CompoundExpressionNodeConversion extends ExpressionNodeConversion<ExpressionNode> {

		public CompoundExpressionNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		protected Object convert(AggregationExpressionTransformationContext<ExpressionNode> context) {

			ExpressionNode currentNode = context.getCurrentNode();

			if (currentNode.hasfirstChildNotOfType(Indexer.class)) {
				// we have a property path expression like: foo.bar -> render as reference
				return context.addToPreviousOrReturn(context.getFieldReference().toString());
			}

			return context.addToPreviousOrReturn(currentNode.getValue());
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.NodeConversion#supports(org.springframework.data.mongodb.core.spel.ExpressionNode)
		 */
		@Override
		protected boolean supports(ExpressionNode node) {
			return node.isOfType(CompoundExpression.class);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	static class NotOperatorNodeConversion extends ExpressionNodeConversion<NotOperatorNode> {

		/**
		 * Creates a new {@link ExpressionNodeConversion}.
		 *
		 * @param transformer must not be {@literal null}.
		 */
		public NotOperatorNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		protected Object convert(AggregationExpressionTransformationContext<NotOperatorNode> context) {

			NotOperatorNode node = context.getCurrentNode();
			List<Object> args = new ArrayList<Object>();

			for (ExpressionNode childNode : node) {
				args.add(transform(childNode, context));
			}

			return context.addToPreviousOrReturn(new Document(node.getMongoOperator(), args));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.NodeConversion#supports(org.springframework.data.mongodb.core.spel.ExpressionNode)
		 */
		@Override
		protected boolean supports(ExpressionNode node) {
			return node.isOfType(OperatorNot.class);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	static class ValueRetrievingNodeConversion extends ExpressionNodeConversion<ExpressionNode> {

		/**
		 * Creates a new {@link ExpressionNodeConversion}.
		 *
		 * @param transformer must not be {@literal null}.
		 */
		public ValueRetrievingNodeConversion(AggregationExpressionTransformer transformer) {
			super(transformer);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.SpelNodeWrapper#convertSpelNodeToMongoObjectExpression(org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.ExpressionConversionContext)
		 */
		@Override
		protected Object convert(AggregationExpressionTransformationContext<ExpressionNode> context) {

			Object value = context.getCurrentNode().getValue();
			return ObjectUtils.isArray(value) ? Arrays.asList(ObjectUtils.toObjectArray(value)) : value;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.SpelExpressionTransformer.NodeConversion#supports(org.springframework.data.mongodb.core.spel.ExpressionNode)
		 */
		@Override
		protected boolean supports(ExpressionNode node) {
			return node.isOfType(InlineMap.class) || node.isOfType(InlineList.class)
					|| node.isOfType(ConstructorReference.class);
		}
	}
}

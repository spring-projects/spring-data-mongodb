/*
 * Copyright 2020 the original author or authors.
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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.aggregation.ScriptOperators.Accumulator.AccumulatorBuilder;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Gateway to {@literal $function} and {@literal $accumulator} aggregation operations.
 * <p />
 * Using {@link ScriptOperators} as part of the {@link Aggregation} requires MongoDB server to have
 * <a href="https://docs.mongodb.com/master/core/server-side-javascript/">server-side JavaScript</a> execution
 * <a href="https://docs.mongodb.com/master/reference/configuration-options/#security.javascriptEnabled">enabled</a>.
 *
 * @author Christoph Strobl
 * @since 3.1
 */
public class ScriptOperators {

	/**
	 * Create a custom aggregation
	 * <a href="https://docs.mongodb.com/master/reference/operator/aggregation/function/">$function<a /> in JavaScript.
	 *
	 * @param body The function definition. Must not be {@literal null}.
	 * @return new instance of {@link Function}.
	 */
	public static Function function(String body) {
		return Function.function(body);
	}

	/**
	 * Create a custom <a href="https://docs.mongodb.com/master/reference/operator/aggregation/accumulator/">$accumulator
	 * operator</a> in Javascript.
	 *
	 * @return new instance of {@link Function}.
	 */
	public static AccumulatorBuilder accumulator() {
		return new AccumulatorBuilder();
	}

	/**
	 * {@link Function} defines a custom aggregation
	 * <a href="https://docs.mongodb.com/master/reference/operator/aggregation/function/">$function</a> in JavaScript.
	 * <p />
	 * <code class="java">
	 * {
	 *   $function: {
	 *     body: ...,
	 *     args: ...,
	 *     lang: "js"
	 *   }
	 * }
	 * </code>
	 * <p />
	 * {@link Function} cannot be used as part of {@link org.springframework.data.mongodb.core.schema.MongoJsonSchema
	 * schema} validation query expression. <br />
	 * <b>NOTE:</b> <a href="https://docs.mongodb.com/master/core/server-side-javascript/">Server-Side JavaScript</a>
	 * execution must be
	 * <a href="https://docs.mongodb.com/master/reference/configuration-options/#security.javascriptEnabled">enabled</a>
	 *
	 * @see <a href="https://docs.mongodb.com/master/reference/operator/aggregation/function/">MongoDB Documentation:
	 *      $function</a>
	 * @since 3.1
	 */
	public static class Function extends AbstractAggregationExpression {

		private Function(Map<String, Object> values) {
			super(values);
		}

		/**
		 * Create a new {@link Function} with the given function definition.
		 *
		 * @param body must not be {@literal null}.
		 * @return new instance of {@link Function}.
		 */
		public static Function function(String body) {

			Map<String, Object> function = new LinkedHashMap<>(2);
			function.put(Fields.BODY.toString(), body);
			function.put(Fields.ARGS.toString(), Collections.emptyList());
			function.put(Fields.LANG.toString(), "js");

			return new Function(function);
		}

		/**
		 * Set the arguments passed to the function body.
		 *
		 * @param args the arguments passed to the function body. Leave empty if the function does not take any arguments.
		 * @return new instance of {@link Function}.
		 */
		public Function args(Object... args) {
			return args(Arrays.asList(args));
		}

		/**
		 * Set the arguments passed to the function body.
		 *
		 * @param args the arguments passed to the function body. Leave empty if the function does not take any arguments.
		 * @return new instance of {@link Function}.
		 */
		public Function args(List<Object> args) {

			Assert.notNull(args, "Args must not be null! Use an empty list instead.");
			return new Function(appendAt(1, Fields.ARGS.toString(), args));
		}

		/**
		 * The language used in the body.
		 *
		 * @param lang must not be {@literal null} nor empty.
		 * @return new instance of {@link Function}.
		 */
		public Function lang(String lang) {

			Assert.hasText(lang, "Lang must not be null nor emtpy! The default would be 'js'.");
			return new Function(appendAt(2, Fields.LANG.toString(), lang));
		}

		@Nullable
		List<Object> getArgs() {
			return get(Fields.ARGS.toString());
		}

		String getBody() {
			return get(Fields.BODY.toString());
		}

		String getLang() {
			return get(Fields.LANG.toString());
		}

		@Override
		protected String getMongoMethod() {
			return "$function";
		}

		enum Fields {

			BODY, ARGS, LANG;

			@Override
			public String toString() {
				return name().toLowerCase();
			}
		}
	}

	/**
	 * {@link Accumulator} defines a custom aggregation
	 * <a href="https://docs.mongodb.com/master/reference/operator/aggregation/accumulator/">$accumulator operator</a>,
	 * one that maintains its state (e.g. totals, maximums, minimums, and related data) as documents progress through the
	 * pipeline, in JavaScript.
	 * <p />
	 * <code class="java">
	 * {
	 *   $accumulator: {
	 *     init: ...,
	 *     intArgs: ...,
	 *     accumulate: ...,
	 *     accumulateArgs: ...,
	 *     merge: ...,
	 *     finalize: ...,
	 *     lang: "js"
	 *   }
	 * }
	 * </code>
	 * <p />
	 * {@link Accumulator} can be used as part of {@link GroupOperation $group}, {@link BucketOperation $bucket} and
	 * {@link BucketAutoOperation $bucketAuto} pipeline stages. <br />
	 * <b>NOTE:</b> <a href="https://docs.mongodb.com/master/core/server-side-javascript/">Server-Side JavaScript</a>
	 * execution must be
	 * <a href="https://docs.mongodb.com/master/reference/configuration-options/#security.javascriptEnabled">enabled</a>
	 *
	 * @see <a href="https://docs.mongodb.com/master/reference/operator/aggregation/accumulator/">MongoDB Documentation:
	 *      $accumulator</a>
	 * @since 3.1
	 */
	public static class Accumulator extends AbstractAggregationExpression {

		private Accumulator(Map<String, Object> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$accumulator";
		}

		enum Fields {

			ACCUMULATE("accumulate"), //
			ACCUMULATE_ARGS("accumulateArgs"), //
			FINALIZE("finalize"), //
			INIT("init"), //
			INIT_ARGS("initArgs"), //
			LANG("lang"), //
			MERGE("merge"); //

			private String field;

			Fields(String field) {
				this.field = field;
			}

			@Override
			public String toString() {
				return field;
			}
		}

		public static class AccumulatorBuilder {

			private List<Object> initArgs;
			private String initFunction;
			private List<Object> accumulateArgs;
			private String accumulateFunction;
			private String mergeFunction;
			private String finalizeFunction;
			private String lang = "js";

			/**
			 * Define the {@code init} {@link Function} for the {@link Accumulator accumulators} initial state. The function
			 * receives its arguments from the {@link Function#args(Object...) initArgs} array expression.
			 * <p />
			 * <code class="java">
			 * function(initArg1, initArg2, ...) {
			 *   ...
			 *   return initialState
			 * }
			 * </code>
			 *
			 * @param function must not be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder init(Function function) {
				return init(function.getBody()).initArgs(function.getArgs());
			}

			/**
			 * Define the {@code init} function for the {@link Accumulator accumulators} initial state. The function receives
			 * its arguments from the {@link #initArgs(Object...)} array expression.
			 * <p />
			 * <code class="java">
			 * function(initArg1, initArg2, ...) {
			 *   ...
			 *   return initialState
			 * }
			 * </code>
			 *
			 * @param function must not be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder init(String function) {

				this.initFunction = function;
				return this;
			}

			/**
			 * Define the optional {@code initArgs} for the {@link #init(String)} function.
			 *
			 * @param args must not be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder initArgs(Object... args) {
				return initArgs(Arrays.asList(args));
			}

			/**
			 * Define the optional {@code initArgs} for the {@link #init(String)} function.
			 *
			 * @param args can be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder initArgs(@Nullable List<Object> args) {

				this.initArgs = args != null ? new ArrayList<>(args) : Collections.emptyList();
				return this;
			}

			/**
			 * Set the {@code accumulate} {@link Function} that updates the state for each document. The functions first
			 * argument is the current {@code state}, additional arguments can be defined via {@link Function#args(Object...)
			 * accumulateArgs}.
			 * <p />
			 * <code class="java">
			 * function(state, accumArg1, accumArg2, ...) {
			 *   ...
			 *   return newState
			 * }
			 * </code>
			 *
			 * @param function must not be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder accumulate(Function function) {
				return accumulate(function.getBody()).accumulateArgs(function.getArgs());
			}

			/**
			 * Set the {@code accumulate} function that updates the state for each document. The functions first argument is
			 * the current {@code state}, additional arguments can be defined via {@link #accumulateArgs(Object...)}.
			 * <p />
			 * <code class="java">
			 * function(state, accumArg1, accumArg2, ...) {
			 *   ...
			 *   return newState
			 * }
			 * </code>
			 *
			 * @param function must not be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder accumulate(String function) {

				this.accumulateFunction = function;
				return this;
			}

			/**
			 * Define additional {@code accumulateArgs} for the {@link #accumulate(String)} function.
			 *
			 * @param args must not be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder accumulateArgs(Object... args) {
				return accumulateArgs(Arrays.asList(args));
			}

			/**
			 * Define additional {@code accumulateArgs} for the {@link #accumulate(String)} function.
			 *
			 * @param args can be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder accumulateArgs(@Nullable List<Object> args) {

				this.accumulateArgs = args != null ? new ArrayList<>(args) : Collections.emptyList();
				return this;
			}

			/**
			 * Set the {@code merge} function used to merge two internal states. <br />
			 * This might be required because the operation is run on a sharded cluster or when the operator exceeds its
			 * memory limit.
			 * <p />
			 * <code class="java">
			 * function(state1, state2) {
			 *   ...
			 *   return newState
			 * }
			 * </code>
			 *
			 * @param function must not be {@literal null}.
			 * @return this.
			 */
			public AccumulatorBuilder merge(String function) {

				this.mergeFunction = function;
				return this;
			}

			/**
			 * The language used in the {@code $accumulator} code.
			 *
			 * @param lang must not be {@literal null}. Default is {@literal js}.
			 * @return this.
			 */
			public AccumulatorBuilder lang(String lang) {

				this.lang = lang;
				return this;
			}

			/**
			 * Set the {@code finalize} function used to update the result of the accumulation when all documents have been
			 * processed.
			 * <p />
			 * <code class="java">
			 * function(state) {
			 *   ...
			 *   return finalState
			 * }
			 * </code>
			 *
			 * @param function must not be {@literal null}.
			 * @return new instance of {@link Accumulator}.
			 */
			public Accumulator finalize(String function) {

				this.finalizeFunction = function;

				Map<String, Object> args = new LinkedHashMap<>();
				args.put(Fields.INIT.toString(), initFunction);
				if (!CollectionUtils.isEmpty(initArgs)) {
					args.put(Fields.INIT_ARGS.toString(), initArgs);
				}
				args.put(Fields.ACCUMULATE.toString(), accumulateFunction);
				if (!CollectionUtils.isEmpty(accumulateArgs)) {
					args.put(Fields.ACCUMULATE_ARGS.toString(), accumulateArgs);
				}
				args.put(Fields.MERGE.toString(), mergeFunction);
				args.put(Fields.FINALIZE.toString(), finalizeFunction);
				args.put(Fields.LANG.toString(), lang);

				return new Accumulator(args);
			}

		}
	}
}

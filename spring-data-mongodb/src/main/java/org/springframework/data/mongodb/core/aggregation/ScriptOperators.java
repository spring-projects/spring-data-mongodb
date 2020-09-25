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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
	}
}

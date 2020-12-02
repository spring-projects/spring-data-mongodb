/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.annotation.meta.When;

import org.springframework.core.annotation.AliasFor;

/**
 * The annotation to configure a value object as embedded (flattened out) in the target document.
 * <p />
 * Depending on the {@link OnEmpty value} of {@link #onEmpty()} the property is set to {@literal null} or an empty
 * instance in the case all embedded values are {@literal null} when reading from the result set.
 *
 * @author Christoph Strobl
 * @since 3.2
 */
@Documented
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = { ElementType.ANNOTATION_TYPE, ElementType.FIELD, ElementType.METHOD })
public @interface Embedded {

	/**
	 * Set the load strategy for the embedded object if all contained fields yield {@literal null} values.
	 * <p />
	 * {@link Nullable @Embedded.Nullable} and {@link Empty @Embedded.Empty} offer shortcuts for this.
	 *
	 * @return never {@link} null.
	 */
	OnEmpty onEmpty();

	/**
	 * @return prefix for columns in the embedded value object. An empty {@link String} by default.
	 */
	String prefix() default "";

	/**
	 * Load strategy to be used {@link Embedded#onEmpty()}.
	 *
	 * @author Christoph Strobl
	 */
	enum OnEmpty {
		USE_NULL, USE_EMPTY
	}

	/**
	 * Shortcut for a nullable embedded property.
	 *
	 * <pre class="code">
	 * &#64;Embedded.Nullable private Address address;
	 * </pre>
	 *
	 * as alternative to the more verbose
	 *
	 * <pre class="code">
	 * &#64;Embedded(onEmpty = USE_NULL) &#64;javax.annotation.Nonnull(when = When.MAYBE) private Address address;
	 * </pre>
	 *
	 * @author Christoph Strobl
	 * @see Embedded#onEmpty()
	 */
	@Embedded(onEmpty = OnEmpty.USE_NULL)
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.FIELD, ElementType.METHOD })
	@javax.annotation.Nonnull(when = When.MAYBE)
	@interface Nullable {

		/**
		 * @return prefix for columns in the embedded value object. An empty {@link String} by default.
		 */
		@AliasFor(annotation = Embedded.class, attribute = "prefix")
		String prefix() default "";

		/**
		 * @return value for columns in the embedded value object. An empty {@link String} by default.
		 */
		@AliasFor(annotation = Embedded.class, attribute = "prefix")
		String value() default "";
	}

	/**
	 * Shortcut for an empty embedded property.
	 *
	 * <pre class="code">
	 * &#64;Embedded.Empty private Address address;
	 * </pre>
	 *
	 * as alternative to the more verbose
	 *
	 * <pre class="code">
	 * &#64;Embedded(onEmpty = USE_EMPTY) &#64;javax.annotation.Nonnull(when = When.NEVER) private Address address;
	 * </pre>
	 *
	 * @author Christoph Strobl
	 * @see Embedded#onEmpty()
	 */
	@Embedded(onEmpty = OnEmpty.USE_EMPTY)
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.FIELD, ElementType.METHOD })
	@javax.annotation.Nonnull(when = When.NEVER)
	@interface Empty {

		/**
		 * @return prefix for columns in the embedded value object. An empty {@link String} by default.
		 */
		@AliasFor(annotation = Embedded.class, attribute = "prefix")
		String prefix() default "";

		/**
		 * @return value for columns in the embedded value object. An empty {@link String} by default.
		 */
		@AliasFor(annotation = Embedded.class, attribute = "prefix")
		String value() default "";
	}
}

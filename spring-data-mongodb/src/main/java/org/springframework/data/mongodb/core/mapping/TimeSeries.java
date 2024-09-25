/*
 * Copyright 2021-2024 the original author or authors.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mongodb.core.timeseries.Granularity;

/**
 * Identifies a domain object to be persisted to a MongoDB Time Series collection.
 *
 * @author Christoph Strobl
 * @author Ben Foster
 * @since 3.3
 * @see <a href="https://docs.mongodb.com/manual/core/timeseries-collections">https://docs.mongodb.com/manual/core/timeseries-collections</a>
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
@Document
public @interface TimeSeries {

	/**
	 * The collection the document representing the entity is supposed to be stored in. If not configured, a default
	 * collection name will be derived from the type's name. The attribute supports SpEL expressions to dynamically
	 * calculate the collection based on a per operation basis.
	 *
	 * @return the name of the collection to be used.
	 * @see Document#collection()
	 */
	@AliasFor(annotation = Document.class, attribute = "collection")
	String collection() default "";

	/**
	 * Name of the property which contains the date in each time series document. <br />
	 * Translation of property names to {@link Field#name() annotated fieldnames} will be considered during the mapping
	 * process.
	 *
	 * @return never {@literal null}.
	 */
	String timeField();

	/**
	 * The name of the field which contains metadata in each time series document. Should not be the {@literal id} nor
	 * {@link #timeField()} nor point to an {@literal array} or {@link java.util.Collection}. <br />
	 * Translation of property names to {@link Field#name() annotated fieldnames} will be considered during the mapping
	 * process.
	 *
	 * @return empty {@link String} by default.
	 */
	String metaField() default "";

	/**
	 * Select the {@link Granularity granularity} parameter to define how data in the time series collection is organized.
	 *
	 * @return {@link Granularity#DEFAULT server default} by default.
	 */
	Granularity granularity() default Granularity.DEFAULT;

	/**
	 * Defines the collation to apply when executing a query or creating indexes.
	 *
	 * @return an empty {@link String} by default.
	 * @see Document#collation()
	 */
	@AliasFor(annotation = Document.class, attribute = "collation")
	String collation() default "";

	/**
	 * Configure the timeout after which the document should expire.
	 * Defaults to an empty {@link String} for no expiry. Accepts numeric values followed by their unit of measure:
	 * <ul>
	 * <li><b>d</b>: Days</li>
	 * <li><b>h</b>: Hours</li>
	 * <li><b>m</b>: Minutes</li>
	 * <li><b>s</b>: Seconds</li>
	 * <li>Alternatively: A Spring {@literal template expression}. The expression can result in a
	 * {@link java.time.Duration} or a valid expiration {@link String} according to the already mentioned
	 * conventions.</li>
	 * </ul>
	 * Supports ISO-8601 style.
	 *
	 * <pre class="code">
	 * &#0064;TimeSeries(expireAfter = "10s") String expireAfterTenSeconds;
	 * &#0064;TimeSeries(expireAfter = "1d") String expireAfterOneDay;
	 * &#0064;TimeSeries(expireAfter = "P2D") String expireAfterTwoDays;
	 * &#0064;TimeSeries(expireAfter = "#{&#0064;mySpringBean.timeout}") String expireAfterTimeoutObtainedFromSpringBean;
	 * &#0064;TimeSeries(expireAfter = "&#36;{my.property.timeout}") String expireAfterTimeoutObtainedFromProperty;
	 * </pre>
	 *
	 * @return empty by default.
	 * @since 4.4
	 */
	String expireAfter() default "";
}

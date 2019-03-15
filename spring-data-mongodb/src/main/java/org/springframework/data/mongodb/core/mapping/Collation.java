/*
 * Copyright 2019. the original author or authors.
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

/*
 * Copyright 2019. the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.annotation.QueryAnnotation;

/**
 * Defines the collation to apply when executing the query or creating indexes.
 *
 * <pre class="code">
 * // Fixed value
 * &#64;Collation("en_US")
 * List<Entry> findAllByFixedCollation();
 *
 * // Fixed value as Document
 * &#64;Collation("{ 'locale' :  'en_US' }")
 * List<Entry> findAllByFixedJsonCollation();
 *
 * // Dynamic value as String
 * &#64;Collation("?0")
 * List<Entry> findAllByDynamicCollation(String collation);
 *
 * // Dynamic value as Document
 * &#64;Collation("{ 'locale' :  ?0 }")
 * List<Entry> findAllByDynamicJsonCollation(String collation);
 *
 * // SpEL expression
 * &#64;Collation("?#{[0]}")
 * List<Entry> findAllByDynamicSpElCollation(String collation);
 * </pre>
 *
 * @author Christoph Strobl
 * @since 2.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE, ElementType.TYPE })
@Documented
@QueryAnnotation
public @interface Collation {

	@AliasFor("collation")
	String value() default "";

	@AliasFor("value")
	String collation() default "";
}

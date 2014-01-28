/*
 * Copyright 2012-2014 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.Map;

import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;

import com.mongodb.DBObject;

/**
 * {@link PropertyAccessor} to allow entity based field access to {@link DBObject}s.
 * 
 * @author Oliver Gierke
 */
class DBObjectPropertyAccessor extends MapAccessor {

	static final MapAccessor INSTANCE = new DBObjectPropertyAccessor();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.expression.MapAccessor#getSpecificTargetClasses()
	 */
	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return new Class[] { DBObject.class };
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.expression.MapAccessor#canRead(org.springframework.expression.EvaluationContext, java.lang.Object, java.lang.String)
	 */
	@Override
	public boolean canRead(EvaluationContext context, Object target, String name) {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.expression.MapAccessor#read(org.springframework.expression.EvaluationContext, java.lang.Object, java.lang.String)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public TypedValue read(EvaluationContext context, Object target, String name) {

		Map<String, Object> source = (Map<String, Object>) target;

		Object value = source.get(name);
		return value == null ? TypedValue.NULL : new TypedValue(value);
	}
}

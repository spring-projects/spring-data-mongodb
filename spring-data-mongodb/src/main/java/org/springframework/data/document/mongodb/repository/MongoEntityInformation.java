/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.document.mongodb.repository;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import org.springframework.data.repository.support.AbstractEntityInformation;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;


/**
 * Expects the domain class to contain a field with a name out of the following
 * {@value #FIELD_NAMES}.
 * 
 * @author Oliver Gierke
 */
class MongoEntityInformation<T extends Object> extends AbstractEntityInformation<T> {

    private static final List<String> FIELD_NAMES = Arrays.asList("ID", "id", "_id");
    private Field field;


    /**
     * Creates a new {@link MongoEntityInformation}.
     * 
     * @param domainClass
     */
    public MongoEntityInformation(Class<T> domainClass) {
    	
    	super(domainClass);

        for (String name : FIELD_NAMES) {

            Field candidate = ReflectionUtils.findField(domainClass, name);

            if (candidate != null) {
                ReflectionUtils.makeAccessible(candidate);
                this.field = candidate;
                break;
            }
        }

        if (this.field == null) {
            throw new IllegalArgumentException(String.format(
                    "Given domain class %s does not contain an id property!",
                    domainClass.getName()));
        }
    }
    
    
    public String getCollectionName() {
    	
    	return StringUtils.uncapitalize(getJavaType().getSimpleName());
    }
    
    public String getIdAttribute() {
    	
    	return "_id";
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.support.IdAware#getId(java.lang.Object
     * )
     */
    public Object getId(Object entity) {

        return ReflectionUtils.getField(field, entity);
    }
}
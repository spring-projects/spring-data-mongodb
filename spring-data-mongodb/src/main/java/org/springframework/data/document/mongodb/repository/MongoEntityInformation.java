/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.data.document.mongodb.repository;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import org.springframework.data.repository.support.IdAware;
import org.springframework.data.repository.support.IsNewAware;
import org.springframework.util.ReflectionUtils;


/**
 * Expects the domain class to contain a field with a name out of the following
 * {@value #FIELD_NAMES}.
 * 
 * @author Oliver Gierke
 */
class MongoEntityInformation implements IsNewAware, IdAware {

    private final List<String> FIELD_NAMES = Arrays.asList("ID", "id", "_id");
    private Field field;


    /**
     * Creates a new {@link MongoEntityInformation}.
     * 
     * @param domainClass
     */
    public MongoEntityInformation(Class<?> domainClass) {

        for (String name : FIELD_NAMES) {

            Field field = ReflectionUtils.findField(domainClass, name);

            if (field != null) {
                ReflectionUtils.makeAccessible(field);
                this.field = field;
                break;
            }
        }

        if (this.field == null) {
            throw new IllegalArgumentException(String.format(
                    "Given domain class %s does not contain an id property!",
                    domainClass.getName()));
        }
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.support.IsNewAware#isNew(java.lang
     * .Object)
     */
    public boolean isNew(Object entity) {

        return null == ReflectionUtils.getField(field, entity);
    }


    /**
     * Returns the actual field name containing the id.
     * 
     * @return
     */
    public String getFieldName() {

        return field.getName();
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
/*
 * Copyright 2011 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.couchdb;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.springframework.http.HttpEntity;

/**
 * Matches the content of the body of an HttpEntity.
 * @author Tareq Abedrabbo
 * @since 31/01/2011
 */
public class IsBodyEqual extends TypeSafeMatcher<HttpEntity> {

    private Object object;

    public IsBodyEqual(Object object) {
        this.object = object;
    }

    @Override
    public boolean matchesSafely(HttpEntity httpEntity) {
        return httpEntity.getBody().equals(object);
    }

    public void describeTo(Description description) {
        description.appendText("body equals ").appendValue(object);
    }

    @Factory
    public static Matcher<HttpEntity> bodyEqual(Object object) {
        return new IsBodyEqual(object);
    }
}

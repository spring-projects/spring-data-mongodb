/*
 * Copyright 2012 the original author or authors.
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
package org.springframework.data.mongodb.core;

import com.mongodb.MongoException;
import com.mongodb.MongoException.Network;
import com.mongodb.MongoException.DuplicateKey;
import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import org.junit.Before;
import org.junit.Test;

import org.springframework.dao.*;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.util.Assert;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link MongoExceptionTranslator}.
 *
 * @author Michal Vich
 */
public class MongoExceptionTranslatorUnitTests {

    private MongoExceptionTranslator translator;

    @Before
    public void setUp() {
        translator = new MongoExceptionTranslator();
    }

    @Test
    public void translateDuplicateKey() {

        DuplicateKey exception = new DuplicateKey(1, "Duplicated key");
        DataAccessException translatedException = translator.translateExceptionIfPossible(exception);
        Assert.isInstanceOf(DuplicateKeyException.class, translatedException);
        assertEquals("Duplicated key", translatedException.getRootCause().getMessage());

    }

    @Test
    public void translateNetwork() {

        Network exception = new Network("IOException", new IOException("IOException"));
        DataAccessException translatedException = translator.translateExceptionIfPossible(exception);
        Assert.isInstanceOf(DataAccessResourceFailureException.class, translatedException);
        assertEquals("IOException", translatedException.getRootCause().getMessage());

    }

    @Test
    public void translateCursorNotFound() throws UnknownHostException {

        MongoException.CursorNotFound exception = new MongoException.CursorNotFound(1, new ServerAddress());
        DataAccessException translatedException = translator.translateExceptionIfPossible(exception);
        Assert.isInstanceOf(DataAccessResourceFailureException.class, translatedException);

    }

    @Test
    public void translateToDuplicateKeyException() {
        checkTranslatedMongoException(DuplicateKeyException.class, 11000);
        checkTranslatedMongoException(DuplicateKeyException.class, 11001);
    }

    @Test
    public void translateToDataAccessResourceFailureException() {
        checkTranslatedMongoException(DataAccessResourceFailureException.class, 12000);
        checkTranslatedMongoException(DataAccessResourceFailureException.class, 13440);
    }

    @Test
    public void translateToInvalidDataAccessApiUsageException() {
        checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 10003);
        checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12001);
        checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12010);
        checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12011);
        checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12012);
    }

    @Test
    public void translateToUncategorizedMongoDbException() {
        MongoException exception = new MongoException(0, "");
        DataAccessException translatedException = translator.translateExceptionIfPossible(exception);
        Assert.isInstanceOf(UncategorizedMongoDbException.class, translatedException);
    }

    @Test
    public void translateMongoInternalException() {
        MongoInternalException exception = new MongoInternalException("Internal exception");
        DataAccessException translatedException = translator.translateExceptionIfPossible(exception);
        Assert.isInstanceOf(InvalidDataAccessResourceUsageException.class, translatedException);
    }

    @Test
    public void translateUnsupportedException() {
        RuntimeException exception = new RuntimeException();
        assertNull(translator.translateExceptionIfPossible(exception));
    }

    private void checkTranslatedMongoException(Class clazz, int code) {
        try {
            translator.translateExceptionIfPossible(new MongoException(code, ""));
            fail("Must be translated to " + clazz);
        } catch (RuntimeException ex) {
            Assert.isInstanceOf(clazz, ex);
        }
    }

}

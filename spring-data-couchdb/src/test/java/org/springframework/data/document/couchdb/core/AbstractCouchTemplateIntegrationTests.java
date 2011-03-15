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

package org.springframework.data.document.couchdb.core;

import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeTrue;
import static org.springframework.http.HttpStatus.OK;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

/**
 * Base class for CouchDB integration tests. Checks whether CouchDB is available before running each test,
 * in which case the test is executed. If CouchDB is not available, tests are ignored.
 *
 * @author Tareq Abedrabbo (tareq.abedrabbo@opencredo.com)
 * @since 13/01/2011
 */

public abstract class AbstractCouchTemplateIntegrationTests {


  protected static final Log log = LogFactory.getLog(AbstractCouchTemplateIntegrationTests.class);

  protected static final RestTemplate restTemplate = new RestTemplate();

  /**
   * This methods ensures that the database is running. Otherwise, the test is ignored.
   */
  @BeforeClass
  public static void assumeDatabaseIsUpAndRunning() {
    try {
      ResponseEntity<String> responseEntity = restTemplate.getForEntity(CouchConstants.COUCHDB_URL, String.class);
      assumeTrue(responseEntity.getStatusCode().equals(OK));
      log.debug("CouchDB is running on " + CouchConstants.COUCHDB_URL +
          " with status " + responseEntity.getStatusCode());
    } catch (RestClientException e) {
      log.debug("CouchDB is not running on " + CouchConstants.COUCHDB_URL);
      assumeNoException(e);
    }
  }

  @Before
  public void setUpTestDatabase() throws Exception {
    RestTemplate template = new RestTemplate();
    template.setErrorHandler(new DefaultResponseErrorHandler() {
      @Override
      public void handleError(ClientHttpResponse response) throws IOException {
        // do nothing, error status will be handled in the switch statement
      }
    });
    ResponseEntity<String> response = template.getForEntity(CouchConstants.TEST_DATABASE_URL, String.class);
    HttpStatus statusCode = response.getStatusCode();
    switch (statusCode) {
      case NOT_FOUND:
        createNewTestDatabase();
        break;
      case OK:
        deleteExisitingTestDatabase();
        createNewTestDatabase();
        break;
      default:
        throw new IllegalStateException("Unsupported http status [" + statusCode + "]");
    }
  }

  private void deleteExisitingTestDatabase() {
    restTemplate.delete(CouchConstants.TEST_DATABASE_URL);
  }

  private void createNewTestDatabase() {
    restTemplate.put(CouchConstants.TEST_DATABASE_URL, null);
  }

  /**
   * Reads a CouchDB document and converts it to the expected type.
   */
  protected <T> T getDocument(String id, Class<T> expectedType) {
    String url = CouchConstants.TEST_DATABASE_URL + "{id}";
    return restTemplate.getForObject(url, expectedType, id);
  }

  /**
   * Writes a CouchDB document
   */
  protected String putDocument(Object document) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    HttpEntity request = new HttpEntity(document, headers);
    String id = UUID.randomUUID().toString();
    restTemplate.put(CouchConstants.TEST_DATABASE_URL + "{id}", request, id);
    return id;
  }
}

package org.springframework.data.document.mongodb.repository.config;

import org.springframework.data.document.mongodb.repository.AbstractPersonRepositoryIntegrationTests;
import org.springframework.test.context.ContextConfiguration;


/**
 * Test class using the namespace configuration to set up the repository
 * instance.
 * 
 * @author Oliver Gierke
 */
@ContextConfiguration
public class MongoNamespaceIntegrationTests extends
        AbstractPersonRepositoryIntegrationTests {

}

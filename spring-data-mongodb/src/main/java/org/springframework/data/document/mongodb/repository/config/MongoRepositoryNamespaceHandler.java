package org.springframework.data.document.mongodb.repository.config;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;


/**
 * {@link org.springframework.beans.factory.xml.NamespaceHandler} for Mongo DB
 * based repositories.
 * 
 * @author Oliver Gierke
 */
public class MongoRepositoryNamespaceHandler extends NamespaceHandlerSupport {

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.beans.factory.xml.NamespaceHandler#init()
     */
    public void init() {

        registerBeanDefinitionParser("repositories",
                new MongoRepositoryConfigDefinitionParser());
    }
}

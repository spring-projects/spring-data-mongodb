package org.springframework.data.document.mongodb.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.data.document.mongodb.config.SimpleMongoRepositoryConfiguration.MongoRepositoryConfiguration;
import org.springframework.data.repository.config.AbstractRepositoryConfigDefinitionParser;
import org.w3c.dom.Element;


/**
 * {@link org.springframework.beans.factory.xml.BeanDefinitionParser} to create
 * Mongo DB repositories from classpath scanning or manual definition.
 * 
 * @author Oliver Gierke
 */
public class MongoRepositoryConfigDefinitionParser
        extends
        AbstractRepositoryConfigDefinitionParser<SimpleMongoRepositoryConfiguration, MongoRepositoryConfiguration> {

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.config.
     * AbstractRepositoryConfigDefinitionParser
     * #getGlobalRepositoryConfigInformation(org.w3c.dom.Element)
     */
    @Override
    protected SimpleMongoRepositoryConfiguration getGlobalRepositoryConfigInformation(
            Element element) {

        return new SimpleMongoRepositoryConfiguration(element);
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.config.
     * AbstractRepositoryConfigDefinitionParser
     * #postProcessBeanDefinition(org.springframework
     * .data.repository.config.SingleRepositoryConfigInformation,
     * org.springframework.beans.factory.support.BeanDefinitionBuilder,
     * java.lang.Object)
     */
    @Override
    protected void postProcessBeanDefinition(
            MongoRepositoryConfiguration context,
            BeanDefinitionBuilder builder, Object beanSource) {

        builder.addPropertyReference("template", context.getMongoTemplateRef());
    }
}

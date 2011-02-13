/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.document.mongodb.config;

import org.springframework.data.document.mongodb.repository.MongoRepository;
import org.springframework.data.document.mongodb.repository.MongoRepositoryFactoryBean;
import org.springframework.data.repository.config.AutomaticRepositoryConfigInformation;
import org.springframework.data.repository.config.ManualRepositoryConfigInformation;
import org.springframework.data.repository.config.RepositoryConfig;
import org.springframework.data.repository.config.SingleRepositoryConfigInformation;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;


/**
 * {@link RepositoryConfig} implementation to create
 * {@link MongoRepositoryConfiguration} instances for both automatic and manual
 * configuration.
 * 
 * @author Oliver Gierke
 */
public class SimpleMongoRepositoryConfiguration
        extends
        RepositoryConfig<SimpleMongoRepositoryConfiguration.MongoRepositoryConfiguration, SimpleMongoRepositoryConfiguration> {

    private static final String MONGO_TEMPLATE_REF = "mongo-template-ref";
    private static final String DEFAULT_MONGO_TEMPLATE_REF = "mongoTemplate";


    /**
     * Creates a new {@link SimpleMongoRepositoryConfiguration} for the given
     * {@link Element}.
     * 
     * @param repositoriesElement
     * @param defaultRepositoryFactoryBeanClassName
     */
    protected SimpleMongoRepositoryConfiguration(Element repositoriesElement) {

        super(repositoriesElement, MongoRepositoryFactoryBean.class.getName());
    }


    /**
     * Returns the bean name of the {@link org.springframework.data.document.mongodb.MongoTemplate} to be referenced.
     * 
     * @return
     */
    public String getMongoTemplateRef() {

        String templateRef = getSource().getAttribute(MONGO_TEMPLATE_REF);
        return StringUtils.hasText(templateRef) ? templateRef
                : DEFAULT_MONGO_TEMPLATE_REF;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.config.GlobalRepositoryConfigInformation
     * #getAutoconfigRepositoryInformation(java.lang.String)
     */
    public MongoRepositoryConfiguration getAutoconfigRepositoryInformation(
            String interfaceName) {

        return new AutomaticMongoRepositoryConfiguration(interfaceName, this);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.config.GlobalRepositoryConfigInformation
     * #getRepositoryBaseInterface()
     */
    public Class<?> getRepositoryBaseInterface() {

        return MongoRepository.class;
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.config.RepositoryConfig#
     * createSingleRepositoryConfigInformationFor(org.w3c.dom.Element)
     */
    @Override
    protected MongoRepositoryConfiguration createSingleRepositoryConfigInformationFor(
            Element element) {

        return new ManualMongoRepositoryConfiguration(element, this);
    }

    /**
     * Simple interface for configuration values specific to Mongo repositories.
     * 
     * @author Oliver Gierke
     */
    public interface MongoRepositoryConfiguration
            extends
            SingleRepositoryConfigInformation<SimpleMongoRepositoryConfiguration> {

        String getMongoTemplateRef();
    }

    /**
     * Implements manual lookup of the additional attributes.
     * 
     * @author Oliver Gierke
     */
    private static class ManualMongoRepositoryConfiguration
            extends
            ManualRepositoryConfigInformation<SimpleMongoRepositoryConfiguration>
            implements MongoRepositoryConfiguration {

        /**
         * Creates a new {@link ManualMongoRepositoryConfiguration} for the
         * given {@link Element} and parent.
         * 
         * @param element
         * @param parent
         */
        public ManualMongoRepositoryConfiguration(Element element,
                SimpleMongoRepositoryConfiguration parent) {

            super(element, parent);
        }


        /*
         * (non-Javadoc)
         * 
         * @see org.springframework.data.document.mongodb.repository.config.
         * SimpleMongoRepositoryConfiguration
         * .MongoRepositoryConfiguration#getMongoTemplateRef()
         */
        public String getMongoTemplateRef() {

            return getAttribute(MONGO_TEMPLATE_REF);
        }
    }

    /**
     * Implements the lookup of the additional attributes during automatic
     * configuration.
     * 
     * @author Oliver Gierke
     */
    private static class AutomaticMongoRepositoryConfiguration
            extends
            AutomaticRepositoryConfigInformation<SimpleMongoRepositoryConfiguration>
            implements MongoRepositoryConfiguration {

        /**
         * Creates a new {@link AutomaticMongoRepositoryConfiguration} for the
         * given interface and parent.
         * 
         * @param interfaceName
         * @param parent
         */
        public AutomaticMongoRepositoryConfiguration(String interfaceName,
                SimpleMongoRepositoryConfiguration parent) {

            super(interfaceName, parent);
        }


        /*
         * (non-Javadoc)
         * 
         * @see org.springframework.data.document.mongodb.repository.config.
         * SimpleMongoRepositoryConfiguration
         * .MongoRepositoryConfiguration#getMongoTemplateRef()
         */
        public String getMongoTemplateRef() {

            return getParent().getMongoTemplateRef();
        }
    }
}

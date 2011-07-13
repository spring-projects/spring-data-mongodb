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

package org.springframework.data.mongodb.config;

import static org.springframework.data.mongodb.config.BeanNames.*;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.core.MongoFactoryBean;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoDbFactoryParser extends AbstractBeanDefinitionParser {

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) throws BeanDefinitionStoreException {
		String id = element.getAttribute("id");
		if (!StringUtils.hasText(id)) {
			id = DB_FACTORY;
		}
		return id;
	}

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
    BeanDefinitionBuilder dbFactoryBuilder = BeanDefinitionBuilder.genericBeanDefinition(SimpleMongoDbFactory.class);
          
    // UserCredentials
    BeanDefinitionBuilder userCredentialsBuilder = BeanDefinitionBuilder.genericBeanDefinition(UserCredentials.class);   
    String username = element.getAttribute("username");
    if (StringUtils.hasText(username)) {
      userCredentialsBuilder.addConstructorArgValue(username);
    } else {
      userCredentialsBuilder.addConstructorArgValue(null);
    }
    String password = element.getAttribute("password");
    if (StringUtils.hasText(password)) {
      userCredentialsBuilder.addConstructorArgValue(password);
    } else {
      userCredentialsBuilder.addConstructorArgValue(null);
    }
    
    // host and port    
    String host = element.getAttribute("host");
    if (!StringUtils.hasText(host)) {
      host = "localhost";
    }
    String port = element.getAttribute("port");
    if (!StringUtils.hasText(port)) {
      port = "27017";
    }
    
    // Database name
    String dbname = element.getAttribute("dbname");
    if (!StringUtils.hasText(dbname)) {
      dbname = "db";
    }
    
    // com.mongodb.Mongo object
    String mongoRef = element.getAttribute("mongo-ref");
    
    if (!StringUtils.hasText(mongoRef)) {
      //Create implicit com.mongodb.Mongo object and register under generated name
      BeanDefinitionBuilder mongoBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoFactoryBean.class);
      mongoBuilder.addPropertyValue("host", host);
      mongoBuilder.addPropertyValue("port", port);
      mongoRef = BeanDefinitionReaderUtils.registerWithGeneratedName(mongoBuilder.getBeanDefinition(), parserContext.getRegistry());      
    }
    dbFactoryBuilder.addConstructorArgValue(new RuntimeBeanReference(mongoRef));
    dbFactoryBuilder.addConstructorArgValue(dbname);
    dbFactoryBuilder.addConstructorArgValue(userCredentialsBuilder.getBeanDefinition());
    
    return dbFactoryBuilder.getRawBeanDefinition();

  }
   
}

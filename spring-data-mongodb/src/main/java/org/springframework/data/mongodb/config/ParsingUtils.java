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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.mongodb.MongoOptionsFactoryBean;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import com.mongodb.ServerAddress;

abstract class ParsingUtils {

  /**
   * Parses the mongo replica-set element.  
   * @param parserContext the parser context
   * @param element the mongo element
   * @param mongoBuilder the bean definition builder to populate
   * @return true if parsing actually occured, false otherwise
   */
  static boolean parseReplicaSet(ParserContext parserContext, Element element, BeanDefinitionBuilder mongoBuilder) {
     
    
    String replicaSetString = element.getAttribute("replica-set");
    if (StringUtils.hasText(replicaSetString)) {
      ManagedList<Object> serverAddresses = new ManagedList<Object>();
      String[] replicaSetStringArray = StringUtils.commaDelimitedListToStringArray(replicaSetString);
      for (int i = 0; i < replicaSetStringArray.length; i++) {
        String[] hostAndPort = StringUtils.delimitedListToStringArray(replicaSetStringArray[i], ":");
        BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(ServerAddress.class);
        defBuilder.addConstructorArgValue(hostAndPort[0]);
        defBuilder.addConstructorArgValue(hostAndPort[1]);
        serverAddresses.add(defBuilder.getBeanDefinition());
      }    
      if (!serverAddresses.isEmpty()) {
        mongoBuilder.addPropertyValue("replicaSetSeeds", serverAddresses);
      }
    }    
    return true;
    
  }
  /**
   * Parses the mongo:options sub-element. Populates the given attribute factory with the proper attributes.
   * 
   * @return true if parsing actually occured, false otherwise
   */
  static boolean parseMongoOptions(ParserContext parserContext, Element element, BeanDefinitionBuilder mongoBuilder) {
    Element optionsElement = DomUtils.getChildElementByTagName(element, "options");
    if (optionsElement == null)
      return false;

    BeanDefinitionBuilder optionsDefBuilder = BeanDefinitionBuilder
        .genericBeanDefinition(MongoOptionsFactoryBean.class);

    setPropertyValue(optionsElement, optionsDefBuilder, "connections-per-host", "connectionsPerHost");
    setPropertyValue(optionsElement, optionsDefBuilder, "threads-allowed-to-block-for-connection-multiplier",
        "threadsAllowedToBlockForConnectionMultiplier");
    setPropertyValue(optionsElement, optionsDefBuilder, "max-wait-time", "maxWaitTime");
    setPropertyValue(optionsElement, optionsDefBuilder, "connect-timeout", "connectTimeout");
    setPropertyValue(optionsElement, optionsDefBuilder, "socket-timeout", "socketTimeout");
    setPropertyValue(optionsElement, optionsDefBuilder, "socket-keep-alive", "socketKeepAlive");    
    setPropertyValue(optionsElement, optionsDefBuilder, "auto-connect-retry", "autoConnectRetry");
    setPropertyValue(optionsElement, optionsDefBuilder, "write-number", "writeNumber");   
    setPropertyValue(optionsElement, optionsDefBuilder, "write-timeout", "writeTimeout");
    setPropertyValue(optionsElement, optionsDefBuilder, "write-fsync", "writeFsync");
    setPropertyValue(optionsElement, optionsDefBuilder, "slave-ok", "slaveOk");   
    
    

    mongoBuilder.addPropertyValue("mongoOptions", optionsDefBuilder.getBeanDefinition());
    return true;
  }
  
  static void setPropertyValue(Element element, BeanDefinitionBuilder builder, String attrName, String propertyName) {
    String attr = element.getAttribute(attrName);
    if (StringUtils.hasText(attr)) {
      builder.addPropertyValue(propertyName, attr);
    }
  }
}

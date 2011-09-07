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
package org.springframework.data.mongodb.repository.support;

import java.util.Collections;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import org.springframework.data.mongodb.core.mapping.Document;

import com.mysema.query.annotations.QueryEmbeddable;
import com.mysema.query.annotations.QueryEmbedded;
import com.mysema.query.annotations.QueryEntities;
import com.mysema.query.annotations.QuerySupertype;
import com.mysema.query.annotations.QueryTransient;
import com.mysema.query.apt.DefaultConfiguration;
import com.mysema.query.apt.Processor;

/**
 * Annotation processor to create Querydsl query types for QueryDsl annoated classes.
 * 
 * @author Oliver Gierke
 */
@SuppressWarnings("restriction")
@SupportedAnnotationTypes({ "com.mysema.query.annotations.*", "org.springframework.data.mongodb.core.mapping.*" })
@SupportedSourceVersion(SourceVersion.RELEASE_6)
public class MongoAnnotationProcessor extends AbstractProcessor {

	@Override
	public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

		processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Running " + getClass().getSimpleName());

		DefaultConfiguration configuration = new DefaultConfiguration(roundEnv, processingEnv.getOptions(),
				Collections.<String> emptySet(), QueryEntities.class, Document.class, QuerySupertype.class,
				QueryEmbeddable.class, QueryEmbedded.class, QueryTransient.class);
		configuration.setUnknownAsEmbedded(true);

		Processor processor = new Processor(processingEnv, roundEnv, configuration);
		processor.process();
		return true;
	}
}

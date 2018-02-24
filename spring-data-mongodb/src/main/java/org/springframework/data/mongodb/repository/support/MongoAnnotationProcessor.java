/*
 * Copyright 2011-2018 the original author or authors.
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

import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.tools.Diagnostic;

import org.springframework.data.mongodb.core.mapping.Document;

import com.querydsl.apt.AbstractQuerydslProcessor;
import com.querydsl.apt.Configuration;
import com.querydsl.apt.DefaultConfiguration;
import com.querydsl.core.annotations.QueryEmbeddable;
import com.querydsl.core.annotations.QueryEmbedded;
import com.querydsl.core.annotations.QueryEntities;
import com.querydsl.core.annotations.QuerySupertype;
import com.querydsl.core.annotations.QueryTransient;
import org.springframework.lang.Nullable;

/**
 * Annotation processor to create Querydsl query types for QueryDsl annotated classes.
 *
 * @author Oliver Gierke
 */
@SupportedAnnotationTypes({ "com.mysema.query.annotations.*", "org.springframework.data.mongodb.core.mapping.*" })
@SupportedSourceVersion(SourceVersion.RELEASE_6)
public class MongoAnnotationProcessor extends AbstractQuerydslProcessor {

	/*
	 * (non-Javadoc)
	 * @see com.mysema.query.apt.AbstractQuerydslProcessor#createConfiguration(javax.annotation.processing.RoundEnvironment)
	 */
	@Override
	protected Configuration createConfiguration(@Nullable RoundEnvironment roundEnv) {

		processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Running " + getClass().getSimpleName());

		DefaultConfiguration configuration = new DefaultConfiguration(roundEnv, processingEnv.getOptions(),
				Collections.<String> emptySet(), QueryEntities.class, Document.class, QuerySupertype.class,
				QueryEmbeddable.class, QueryEmbedded.class, QueryTransient.class);
		configuration.setUnknownAsEmbedded(true);

		return configuration;
	}
}

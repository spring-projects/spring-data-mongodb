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
package org.springframework.data.mongodb.crossstore.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class Resume {

	private static final Log LOGGER = LogFactory.getLog(Resume.class);

	@Id
	private ObjectId id;

	private String education = "";

	private String jobs = "";

	public String getId() {
		return id.toString();
	}

	public String getEducation() {
		return education;
	}

	public void addEducation(String education) {
		LOGGER.debug("Adding education " + education);
		this.education = this.education + (this.education.length() > 0 ? "; " : "") + education;
	}

	public String getJobs() {
		return jobs;
	}

	public void addJob(String job) {
		LOGGER.debug("Adding job " + job);
		this.jobs = this.jobs + (this.jobs.length() > 0 ? "; " : "") + job;
	}

	@Override
	public String toString() {
		return "Resume [education=" + education + ", jobs=" + jobs + "]";
	}

}

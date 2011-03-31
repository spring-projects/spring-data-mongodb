package org.springframework.data.document.persistence.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.persistence.document.mongo.MongoDocumentBacking;

//@DocumentEntity
public class Resume {

  private static final Log LOGGER = LogFactory.getLog(Resume.class);

  private String education = "";

  private String jobs = "";

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

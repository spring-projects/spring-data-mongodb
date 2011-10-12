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

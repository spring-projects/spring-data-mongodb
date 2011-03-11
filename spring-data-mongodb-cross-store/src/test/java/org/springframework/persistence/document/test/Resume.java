package org.springframework.persistence.document.test;

import org.springframework.persistence.document.DocumentEntity;

@DocumentEntity
public class Resume {

	private String education = "";
	
	private String jobs = "";

	public String getEducation() {
		return education;
	}

	public void addEducation(String education) {
		this.education = this.education + (this.education.length() > 0 ? "; " : "") + education;
	}

	public String getJobs() {
		return jobs;
	}

	public void addJob(String job) {
		this.jobs = this.jobs + (this.jobs.length() > 0 ? "; " : "") + job;
	}

	@Override
	public String toString() {
		return "Resume [education=" + education + ", jobs=" + jobs + "]";
	}
	
}

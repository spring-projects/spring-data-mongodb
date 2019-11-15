pipeline {
	agent none

	triggers {
		pollSCM 'H/10 * * * *'
		upstream(upstreamProjects: "spring-data-commons/master", threshold: hudson.model.Result.SUCCESS)
	}

	options {
		disableConcurrentBuilds()
		buildDiscarder(logRotator(numToKeepStr: '14'))
	}

	stages {
		stage("Docker images") {
			parallel {
				stage('Publish JDK 8 + MongoDB 4.0') {
					when {
						changeset "ci/openjdk8-mongodb-4.0/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk8-with-mongodb-4.0", "ci/openjdk8-mongodb-4.0/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 8 + MongoDB 4.1') {
					when {
						changeset "ci/openjdk8-mongodb-4.1/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk8-with-mongodb-4.1", "ci/openjdk8-mongodb-4.1/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 8 + MongoDB 4.2') {
                	when {
                		changeset "ci/openjdk8-mongodb-4.2/**"
                	}
                	agent { label 'data' }
                	options { timeout(time: 30, unit: 'MINUTES') }

                	steps {
                		script {
                			def image = docker.build("springci/spring-data-openjdk8-with-mongodb-4.2", "ci/openjdk8-mongodb-4.2/")
                			docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
                				image.push()
                			}
                		}
                	}
                }
				stage('Publish JDK 11 + MongoDB 4.0') {
					when {
						changeset "ci/openjdk11-mongodb-4.0/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk11-with-mongodb-4.0", "ci/openjdk11-mongodb-4.0/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 11 + MongoDB 4.1') {
					when {
						changeset "ci/openjdk11-mongodb-4.1/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk11-with-mongodb-4.1", "ci/openjdk11-mongodb-4.1/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 11 + MongoDB 4.2') {
					when {
						changeset "ci/openjdk11-mongodb-4.2/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk11-with-mongodb-4.2", "ci/openjdk11-mongodb-4.2/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 13 + MongoDB 4.0') {
					when {
						changeset "ci/openjdk13-mongodb-4.0/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk13-with-mongodb-4.0", "ci/openjdk13-mongodb-4.0/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 13 + MongoDB 4.1') {
					when {
						changeset "ci/openjdk13-mongodb-4.1/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk13-with-mongodb-4.1", "ci/openjdk13-mongodb-4.1/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 13 + MongoDB 4.2') {
					when {
						changeset "ci/openjdk13-mongodb-4.2/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk13-with-mongodb-4.2", "ci/openjdk13-mongodb-4.2/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
			}
		}

		stage("test: baseline (jdk8)") {
			when {
				anyOf {
					branch 'master'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				docker {
					image 'springci/spring-data-openjdk8-with-mongodb-4.2:latest'
					label 'data'
					args '-v $HOME:/tmp/jenkins-home'
				}
			}
			options { timeout(time: 30, unit: 'MINUTES') }
			steps {
				sh 'rm -rf ?'
				sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
				sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
				sh 'sleep 10'
				sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
				sh 'sleep 15'
				sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw clean dependency:list test -Dsort -U -B'
			}
		}

		stage("Test other configurations") {
			when {
				anyOf {
					branch 'master'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			parallel {
				stage("test: baseline (jdk11)") {
					agent {
						docker {
							image 'springci/spring-data-openjdk11-with-mongodb-4.2:latest'
							label 'data'
							args '-v $HOME:/tmp/jenkins-home'
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
						sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
						sh 'sleep 10'
						sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
						sh 'sleep 15'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pjava11 clean dependency:list test -Dsort -U -B'
					}
				}

				stage("test: baseline (jdk13)") {
					agent {
						docker {
							image 'springci/spring-data-openjdk13-with-mongodb-4.2:latest'
							label 'data'
							args '-v $HOME:/tmp/jenkins-home'
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
						sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
						sh 'sleep 10'
						sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
						sh 'sleep 15'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pjava11 clean dependency:list test -Dsort -U -B'
					}
				}

				stage("test: mongodb 4.0 (jdk8)") {
					agent {
						docker {
							image 'springci/spring-data-openjdk8-with-mongodb-4.0:latest'
							label 'data'
							args '-v $HOME:/tmp/jenkins-home'
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
						sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
						sh 'sleep 10'
						sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
						sh 'sleep 15'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw clean dependency:list test -Dsort -U -B'
					}
				}

				stage("test: mongodb 4.0 (jdk11)") {
					agent {
						docker {
							image 'springci/spring-data-openjdk11-with-mongodb-4.0:latest'
							label 'data'
							args '-v $HOME:/tmp/jenkins-home'
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
						sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
						sh 'sleep 10'
						sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
						sh 'sleep 15'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pjava11 clean dependency:list test -Dsort -U -B'
					}
				}

				stage("test: mongodb 4.0 (jdk13)") {
					agent {
						docker {
							image 'springci/spring-data-openjdk13-with-mongodb-4.0:latest'
							label 'data'
							args '-v $HOME:/tmp/jenkins-home'
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
						sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
						sh 'sleep 10'
						sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
						sh 'sleep 15'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pjava11 clean dependency:list test -Dsort -U -B'
					}
				}

				stage("test: mongodb 4.1 (jdk8)") {
					agent {
						docker {
							image 'springci/spring-data-openjdk8-with-mongodb-4.1:latest'
							label 'data'
							args '-v $HOME:/tmp/jenkins-home'
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
						sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
						sh 'sleep 10'
						sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
						sh 'sleep 15'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw clean dependency:list test -Dsort -U -B'
					}
				}

				stage("test: mongodb 4.1 (jdk11)") {
					agent {
						docker {
							image 'springci/spring-data-openjdk11-with-mongodb-4.1:latest'
							label 'data'
							args '-v $HOME:/tmp/jenkins-home'
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
						sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
						sh 'sleep 10'
						sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
						sh 'sleep 15'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pjava11 clean dependency:list test -Dsort -U -B'
					}
				}

				stage("test: mongodb 4.1 (jdk13)") {
					agent {
						docker {
							image 'springci/spring-data-openjdk13-with-mongodb-4.1:latest'
							label 'data'
							args '-v $HOME:/tmp/jenkins-home'
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
						sh 'mongod --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
						sh 'sleep 10'
						sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
						sh 'sleep 15'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pjava11 clean dependency:list test -Dsort -U -B'
					}
				}
			}
		}

		stage('Release to artifactory') {
			when {
				anyOf {
					branch 'master'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
					label 'data'
					args '-v $HOME:/tmp/jenkins-home'
				}
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				sh 'rm -rf ?'
				sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci,artifactory ' +
						'-Dartifactory.server=https://repo.spring.io ' +
						"-Dartifactory.username=${ARTIFACTORY_USR} " +
						"-Dartifactory.password=${ARTIFACTORY_PSW} " +
						"-Dartifactory.staging-repository=libs-snapshot-local " +
						"-Dartifactory.build-name=spring-data-mongodb " +
						"-Dartifactory.build-number=${BUILD_NUMBER} " +
						'-Dmaven.test.skip=true clean deploy -U -B'
			}
		}

		stage('Publish documentation') {
			when {
				branch 'master'
			}
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
					label 'data'
					args '-v $HOME:/tmp/jenkins-home'
				}
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci,distribute ' +
						'-Dartifactory.server=https://repo.spring.io ' +
						"-Dartifactory.username=${ARTIFACTORY_USR} " +
						"-Dartifactory.password=${ARTIFACTORY_PSW} " +
						"-Dartifactory.distribution-repository=temp-private-local " +
						'-Dmaven.test.skip=true clean deploy -U -B'
			}
		}
	}

	post {
		changed {
			script {
				slackSend(
						color: (currentBuild.currentResult == 'SUCCESS') ? 'good' : 'danger',
						channel: '#spring-data-dev',
						message: "${currentBuild.fullDisplayName} - `${currentBuild.currentResult}`\n${env.BUILD_URL}")
				emailext(
						subject: "[${currentBuild.fullDisplayName}] ${currentBuild.currentResult}",
						mimeType: 'text/html',
						recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
						body: "<a href=\"${env.BUILD_URL}\">${currentBuild.fullDisplayName} is reported as ${currentBuild.currentResult}</a>")
			}
		}
	}
}

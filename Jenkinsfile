pipeline {
	agent none

	triggers {
		pollSCM 'H/10 * * * *'
		upstream(upstreamProjects: "spring-data-commons/main", threshold: hudson.model.Result.SUCCESS)
	}

	options {
		disableConcurrentBuilds()
		buildDiscarder(logRotator(numToKeepStr: '14'))
	}

	stages {
		stage("Docker images") {
			parallel {
			    stage('Publish JDK 8 + MongoDB 5.0') {
					when {
						changeset "ci/openjdk8-mongodb-5.0/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk8-with-mongodb-5.0.0", "ci/openjdk8-mongodb-5.0/")
								docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
									image.push()
								}
							}
						}
					}
				stage('Publish JDK 8 + MongoDB 4.0') {
					when {
						changeset "ci/openjdk8-mongodb-4.0/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk8-with-mongodb-4.0.23", "ci/openjdk8-mongodb-4.0/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 8 + MongoDB 4.4') {
					when {
						changeset "ci/openjdk8-mongodb-4.4/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk8-with-mongodb-4.4.4", "ci/openjdk8-mongodb-4.4/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 16 + MongoDB 4.4') {
					when {
						changeset "ci/openjdk16-mongodb-4.4/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk16-with-mongodb-4.4.4", "ci/openjdk16-mongodb-4.4/")
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
					branch 'main'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				label 'data'
			}
			options { timeout(time: 30, unit: 'MINUTES') }
			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}
			steps {
				script {
					docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
						docker.image('springci/spring-data-openjdk8-with-mongodb-4.0.23:latest').inside('-v $HOME:/tmp/jenkins-home') {
							sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
							sh 'mongod --setParameter transactionLifetimeLimitSeconds=90 --setParameter maxTransactionLockRequestTimeoutMillis=10000 --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
							sh 'sleep 10'
							sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
							sh 'sleep 15'
							sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml clean dependency:list test -Duser.name=jenkins -Dsort -U -B'
						}
					}
				}
			}
		}

		stage("Test other configurations") {
			when {
				allOf {
					branch 'issue/3696'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			parallel {
				stage("test: mongodb 4.0 (jdk8)") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
						ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
					}
					steps {
						script {
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								docker.image('springci/spring-data-openjdk8-with-mongodb-4.0.23:latest').inside('-v $HOME:/tmp/jenkins-home') {
									sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
									sh 'mongod --setParameter transactionLifetimeLimitSeconds=90 --setParameter maxTransactionLockRequestTimeoutMillis=10000 --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
									sh 'sleep 10'
									sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
									sh 'sleep 15'
									sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml clean dependency:list test -Duser.name=jenkins -Dsort -U -B'
								}
							}
						}
					}
				}

				stage("test: mongodb 4.4 (jdk8)") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
						ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
					}
					steps {
						script {
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								docker.image('springci/spring-data-openjdk8-with-mongodb-4.4.4:latest').inside('-v $HOME:/tmp/jenkins-home') {
									sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
									sh 'mongod --setParameter transactionLifetimeLimitSeconds=90 --setParameter maxTransactionLockRequestTimeoutMillis=10000 --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
									sh 'sleep 10'
									sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
									sh 'sleep 15'
									sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml clean dependency:list test -Duser.name=jenkins -Dsort -U -B'
								}
							}
						}
					}
				}

				stage("test: mongodb 5.0 (jdk8)") {
                	agent {
                		label 'data'
                	}
                	options { timeout(time: 30, unit: 'MINUTES') }
                	environment {
                		ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
                	}
                	steps {
                		script {
                			docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
                				docker.image('springci/spring-data-openjdk8-with-mongodb-5.0.0:latest').inside('-v $HOME:/tmp/jenkins-home') {
                					sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
                					sh 'mongod --setParameter transactionLifetimeLimitSeconds=90 --setParameter maxTransactionLockRequestTimeoutMillis=10000 --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
                					sh 'sleep 10'
                					sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
                					sh 'sleep 15'
                					sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml clean dependency:list test -Duser.name=jenkins -Dsort -U -B'
                				}
                			}
                		}
                	}
                }

				stage("test: baseline (jdk16)") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
						ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
					}
					steps {
						script {
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								docker.image('springci/spring-data-openjdk16-with-mongodb-4.4.4:latest').inside('-v $HOME:/tmp/jenkins-home') {
									sh 'mkdir -p /tmp/mongodb/db /tmp/mongodb/log'
									sh 'mongod --setParameter transactionLifetimeLimitSeconds=90 --setParameter maxTransactionLockRequestTimeoutMillis=10000 --dbpath /tmp/mongodb/db --replSet rs0 --fork --logpath /tmp/mongodb/log/mongod.log &'
									sh 'sleep 10'
									sh 'mongo --eval "rs.initiate({_id: \'rs0\', members:[{_id: 0, host: \'127.0.0.1:27017\'}]});"'
									sh 'sleep 15'
									sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pjava11 clean dependency:list test -Duser.name=jenkins -Dsort -U -B'
								}
							}
						}
					}
				}
			}
		}

		stage('Release to artifactory') {
			when {
				anyOf {
					branch 'main'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				label 'data'
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				script {
					docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
						docker.image('adoptopenjdk/openjdk8:latest').inside('-v $HOME:/tmp/jenkins-home') {
							sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pci,artifactory ' +
									'-Dartifactory.server=https://repo.spring.io ' +
									"-Dartifactory.username=${ARTIFACTORY_USR} " +
									"-Dartifactory.password=${ARTIFACTORY_PSW} " +
									"-Dartifactory.staging-repository=libs-snapshot-local " +
									"-Dartifactory.build-name=spring-data-mongodb " +
									"-Dartifactory.build-number=${BUILD_NUMBER} " +
									'-Dmaven.test.skip=true clean deploy -U -B'
						}
					}
				}
			}
		}

		stage('Publish documentation') {
			when {
				branch 'main'
			}
			agent {
				label 'data'
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				script {
					docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
						docker.image('adoptopenjdk/openjdk8:latest').inside('-v $HOME:/tmp/jenkins-home') {
							sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pci,distribute ' +
									'-Dartifactory.server=https://repo.spring.io ' +
									"-Dartifactory.username=${ARTIFACTORY_USR} " +
									"-Dartifactory.password=${ARTIFACTORY_PSW} " +
									"-Dartifactory.distribution-repository=temp-private-local " +
									'-Dmaven.test.skip=true clean deploy -U -B'
						}
					}
				}
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

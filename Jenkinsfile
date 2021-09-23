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
				stage('Publish JDK 17 + MongoDB 4.4') {
					when {
						changeset "ci/openjdk17-mongodb-4.4/**"
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-openjdk17-with-mongodb-4.4.4", "ci/openjdk17-mongodb-4.4/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 17 + MongoDB 5.0') {
                	when {
                		changeset "ci/openjdk17-mongodb-5.0/**"
                	}
                	agent { label 'data' }
                	options { timeout(time: 30, unit: 'MINUTES') }

                	steps {
                		script {
                			def image = docker.build("springci/spring-data-openjdk17-with-mongodb-5.0.3", "ci/openjdk17-mongodb-5.0/")
                			docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
                				image.push()
                			}
                		}
                	}
                }
			}
		}

		stage("test: baseline Mongo 5.0 (jdk17)") {
			when {
				beforeAgent(true)
				anyOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
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
						docker.image('springci/spring-data-openjdk17-with-mongodb-5.0.3:latest').inside('-v $HOME:/tmp/jenkins-home') {
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
				beforeAgent(true)
				allOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
					not { triggeredBy 'UpstreamCause' }
				}
			}
			parallel {
				stage("test: baseline MongoDB 4.4 (jdk17)") {
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
								docker.image('springci/spring-data-openjdk17-with-mongodb-4.4.4:latest').inside('-v $HOME:/tmp/jenkins-home') {
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
			}
		}

		stage('Release to artifactory') {
			when {
				beforeAgent(true)
				anyOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
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
						docker.image('openjdk:17-bullseye').inside('-v $HOME:/tmp/jenkins-home') {
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

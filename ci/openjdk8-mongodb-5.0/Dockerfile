FROM adoptopenjdk/openjdk8:latest

ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive

RUN RUN set -eux; \
	apt-get update && apt-get install -y apt-transport-https apt-utils gnupg2 ; \
	apt-key adv --keyserver hkps://keyserver.ubuntu.com:443 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4 ; \
	echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/5.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-5.0.list; \
	echo ${TZ} > /etc/timezone;

RUN apt-get update ; \
	apt-get install -y mongodb-org=5.0.0 mongodb-org-server=5.0.0 mongodb-org-shell=5.0.0 mongodb-org-mongos=5.0.0 mongodb-org-tools=5.0.0 ; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;


ARG BASE
FROM ${BASE}
# Any ARG statements before FROM are cleared.
ARG MONGODB

ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive

RUN RUN set -eux; \
	apt-get update && apt-get install -y apt-transport-https apt-utils gnupg2 ; \
	apt-key adv --keyserver hkps://keyserver.ubuntu.com:443 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4 ; \
	echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.0.list; \
	echo ${TZ} > /etc/timezone;

RUN apt-get update ; \
	apt-get install -y mongodb-org=${MONGODB} mongodb-org-server=${MONGODB} mongodb-org-shell=${MONGODB} mongodb-org-mongos=${MONGODB} mongodb-org-tools=${MONGODB} ; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;

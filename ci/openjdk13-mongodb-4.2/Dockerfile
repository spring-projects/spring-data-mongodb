FROM adoptopenjdk/openjdk13:latest

ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive

RUN set -eux; \
	apt-get update && apt-get install -y apt-transport-https apt-utils gnupg2 ; \
	apt-key adv --keyserver hkps://keyserver.ubuntu.com:443 --recv e162f504a20cdf15827f718d4b7c549a058f8b6b ; \
	echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.2.list; \
	echo ${TZ} > /etc/timezone;

RUN apt-get update ; \
	apt-get install -y mongodb-org=4.2.0 mongodb-org-server=4.2.0 mongodb-org-shell=4.2.0 mongodb-org-mongos=4.2.0 mongodb-org-tools=4.2.0 ; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;

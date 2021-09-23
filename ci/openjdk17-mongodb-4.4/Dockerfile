FROM openjdk:17-bullseye

ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive

RUN set -eux; \
	sed -i -e 's/archive.ubuntu.com/mirror.one.com/g' /etc/apt/sources.list; \
	sed -i -e 's/security.ubuntu.com/mirror.one.com/g' /etc/apt/sources.list; \
	sed -i -e 's/http/https/g' /etc/apt/sources.list ; \
	apt-get update && apt-get install -y apt-transport-https apt-utils gnupg2 ; \
	apt-key adv --keyserver hkps://keyserver.ubuntu.com:443 --recv 656408E390CFB1F5 ; \
	echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list; \
	echo ${TZ} > /etc/timezone;

RUN apt-get update ; \
	apt-get install -y mongodb-org=4.4.4 mongodb-org-server=4.4.4 mongodb-org-shell=4.4.4 mongodb-org-mongos=4.4.4 mongodb-org-tools=4.4.4 ; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;

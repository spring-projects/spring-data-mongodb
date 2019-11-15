FROM adoptopenjdk/openjdk11:latest

RUN apt-get update && apt-get install -y apt-transport-https apt-utils gnupg2

RUN apt-key adv --keyserver hkps://keyserver.ubuntu.com:443 --recv 4B7C549A058F8B6B

RUN echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.1 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.1.list

RUN apt-get update

RUN apt-get install -y mongodb-org-unstable=4.1.13 mongodb-org-unstable-server=4.1.13 mongodb-org-unstable-shell=4.1.13 mongodb-org-unstable-mongos=4.1.13 mongodb-org-unstable-tools=4.1.13

RUN apt-get clean \
 && rm -rf /var/lib/apt/lists/*
FROM adoptopenjdk/openjdk13:latest

RUN apt-get update && apt-get install -y apt-transport-https apt-utils gnupg2

RUN apt-key adv --keyserver hkps://keyserver.ubuntu.com:443 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4

RUN echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.0.list

RUN apt-get update

RUN apt-get install -y mongodb-org=4.0.9 mongodb-org-server=4.0.9 mongodb-org-shell=4.0.9 mongodb-org-mongos=4.0.9 mongodb-org-tools=4.0.9

RUN apt-get clean \
 && rm -rf /var/lib/apt/lists/*
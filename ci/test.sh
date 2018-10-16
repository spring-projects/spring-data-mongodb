#!/bin/bash

set -euo pipefail

mkdir -p /data/db
mongod &

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

rm -rf $HOME/.m2/repository/org/springframework/data/mongodb 2> /dev/null || :

cd spring-data-mongodb-github

./mvnw clean dependency:list test -P${PROFILE} -Dsort

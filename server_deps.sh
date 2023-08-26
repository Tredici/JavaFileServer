#!/bin/bash

JAR="JavaFileServer-1.0-SNAPSHOT.jar"
JDIR=target/
DIR=$(dirname $0)/$JDIR/$JAR
java -classpath "$DIR:$(find $HOME/.m2/repository/ -type f -name gson-2.10.1.jar -print -quit)" it.sssupserver.app.App "$@"

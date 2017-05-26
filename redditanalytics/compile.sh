#!/bin/bash
echo Compile Project

export JAVA_HOME=
mvn clean compile
mvn package

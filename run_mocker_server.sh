#!/usr/bin/env bash
source /etc/profile

now_dir=`pwd`

cd upstream-mocker

mvn clean  -Dmaven.test.skip=true package appassembler:assemble

cd ${now_dir}

chmod +x upstream-mocker/target/dist-mocker/bin/UpstreamMocker.sh

sh upstream-mocker/target/dist-mocker/bin/UpstreamMocker.sh
#!/bin/bash
# shellcheck disable=SC1091

USE_SONAR=false
CHECK_ONLY=false
SONAR_KEY=""
SONAR_URI="http://localhost:9000"

if [ $CHECK_ONLY == false ]
then
    python -m venv venv
    source "./venv/bin/activate"

    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt

    echo "Environment setup complete"
else
    echo "SonarQube-only!"
fi

if [ $USE_SONAR == true ]
then
    echo "SonarQube scan will now be run"
    sonar-scanner -Dsonar.projectKey=Uni-Project -Dsonar.sources=. -Dsonar.host.url=$SONAR_URI -Dsonar.login=$SONAR_KEY
fi
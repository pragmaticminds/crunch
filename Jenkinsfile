/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#!/usr/bin/env groovy
import groovy.json.JsonOutput

/**
 * Jenkinsfile for building and deployment of snapshots from master.
 */

// For slack see: http://vgarcia.me/tech/2016/01/22/Slack-notifications-in-jenkins-workflow.html
def slackChannel = "#crunch-build"

def notifySlack(text, channel, attachments) {

    //your  slack integration url
    def slackURL = 'https://hooks.slack.com/services/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'
    //from the jenkins wiki, you can updload an avatar and
    //use that one
    // Changed to an URL without & to make it work with windows
    def jenkinsIcon = 'https://cdn.apps.splunk.com/media/public/icons/51b1657a-3a54-11e7-9350-02f13bdc2585.png'

    def payload = JsonOutput.toJson([text      : text,
                                     channel   : channel,
                                     username  : "jenkins",
                                     icon_url: jenkinsIcon,
                                     attachments: attachments])

    if (isUnix()) {
        sh "curl -X POST --data-urlencode \'payload=${payload}\' ${slackURL}"
    } else {
        echo payload
        def newPayload = payload.replace("\"", "\\\"")
        echo newPayload
        bat "powershell \"Invoke-RestMethod -Uri ${slackURL} -Method POST -Body 'payload=${newPayload}'\""
    }
}

def notifyFailed(env, text, slackChannel) {
    notifySlack(text, slackChannel,
            [[
                     title: "${env.JOB_NAME} build ${env.BUILD_NUMBER}",
                     color: "danger",
                     text : """:dizzy_face: Build finished with error. 
                |${env.BUILD_URL}
                |branch: ${env.BRANCH_NAME}""".stripMargin()
             ]])
}
node('linux') {
    try {
        withMaven(
                // Maven installation declared in the Jenkins "Global Tool Configuration"
                maven: 'M3',
                // Maven settings.xml file defined with the Jenkins Config File Provider Plugin
                // Maven settings and global settings can also be defined in Jenkins Global Tools Configuration
                mavenSettingsConfig: 'nexus-sonar',
                mavenLocalRepo: '.repository') {
            stage('preparation') {
                mvnHome = tool 'M3'
            }
            stage('checkout') {
                echo "Checkout from scm"
                checkout scm
            }
            stage('compile') {
                // Run the maven build
                // sh "'${mvnHome}/bin/mvn' clean compile"
                sh 'echo $RUN_ON_JENKINS'
                sh "mvn clean compile -U compiler:testCompile net.alchim31.maven:scala-maven-plugin:3.2.2:testCompile"
//                stash includes: '**/target/**/*', name: 'source'
            }
            stage('unit tests') {
//                checkout scm
//                unstash 'source'
                try {
                    sh "mvn -Dmaven.test.failure.ignore=true test"
                } catch (Exception err) {
                    echo err
                    currentBuild.result = "FAILURE"
                }
                finally {
                    junit '**/surefire-reports/*.xml'
                }
            }

            stage('Build site') {
                when {
                    branch 'develop'
                }
                steps {
                    echo 'Building Site'
                    sh 'mvn -P${JENKINS_PROFILE} site'
                }
            }

            stage('Stage site') {
                when {
                    branch 'develop'
                }
                steps {
                    echo 'Staging Site'
                    sh 'mvn -P${JENKINS_PROFILE} site:stage'
                }
            }

            stage('Deploy site') {
                when {
                    branch 'develop'
                }
                steps {
                    echo 'Deploying Site'
                    sh 'mvn scm-publish:publish-scm'
                }
            }

            // TODO Enable
//            stage('integration tests') {
//                // Run Integration tests with failsafe plugin
//                try {
//                    sh "mvn verify -DskipUTs"
//                } catch (Exception err) {
//                    echo err
//                    currentBuild.result = "FAILURE"
//                }
//                finally {
//                    junit '**/failsafe-reports/*.xml'
//                }
//            }
        }
    } catch (Exception e) {
        //modify #build-channel to the build channel you want
        //for public channels don't forget the # (hash)
        throw e
    }

    // TODO add this in
//    withMaven(
//            // Maven installation declared in the Jenkins "Global Tool Configuration"
//            maven: 'M3',
//            // Maven settings.xml file defined with the Jenkins Config File Provider Plugin
//            // Maven settings and global settings can also be defined in Jenkins Global Tools Configuration
//            mavenSettingsConfig: 'nexus-sonar',
//            mavenLocalRepo: '.repository') {
//        stage('Package') {
//
//            sh 'mvn package -DskipTests'
//
//            // withMaven will discover the generated Maven artifacts, JUnit Surefire & FailSafe reports and FindBugs reports
//        }
//        stage('SonarQube analysis') {
//            // Run the maven build
//            withSonarQubeEnv('Default') {
//                sh "mvn sonar:sonar -Dsonar.host.url=$SONAR_HOST_URL -Dsonar.branch=${env.BRANCH_NAME}"
//            }
//            // withMaven will discover the generated Maven artifacts, JUnit Surefire & FailSafe reports and FindBugs reports
//        }
//    }
//    // No need to occupy a node
//    stage("Quality Gate"){
//        timeout(time: 1, unit: 'HOURS') { // Just in case something goes wrong, pipeline will be killed after a timeout
//            def qg = waitForQualityGate() // Reuse taskId previously collected by withSonarQubeEnv
//            if (qg.status != 'OK') {
//                error "Pipeline aborted due to quality gate failure: ${qg.status}"
//            }
//        }
//    }
//    if (env.BRANCH_NAME == 'develop') {
//        withMaven(
//                // Maven installation declared in the Jenkins "Global Tool Configuration"
//                maven: 'M3',
//                // Maven settings.xml file defined with the Jenkins Config File Provider Plugin
//                // Maven settings and global settings can also be defined in Jenkins Global Tools Configuration
//                mavenSettingsConfig: 'nexus-sonar',
//                mavenLocalRepo: '.repository') {
//            // Deploy Snapshot to Nexus
//            stage('Deploy Snapshot') {
//                sh "mvn site:site site:deploy"
//                sh "mvn deploy -DskipTests"
//            }
//        }
//
//    }
}

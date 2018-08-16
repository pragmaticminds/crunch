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

    withMaven(
            // Maven installation declared in the Jenkins "Global Tool Configuration"
            maven: 'M3',
            // Maven settings.xml file defined with the Jenkins Config File Provider Plugin
            // Maven settings and global settings can also be defined in Jenkins Global Tools Configuration
            mavenSettingsConfig: 'nexus-sonar',
            mavenLocalRepo: '.repository') {
        stage('Package') {

            sh 'mvn package -DskipTests'

            // withMaven will discover the generated Maven artifacts, JUnit Surefire & FailSafe reports and FindBugs reports
        }
        stage('SonarQube analysis') {
            // Run the maven build
            withSonarQubeEnv('Default') {
                sh "mvn sonar:sonar -Dsonar.host.url=$SONAR_HOST_URL -Dsonar.branch=${env.BRANCH_NAME}"
            }
            // withMaven will discover the generated Maven artifacts, JUnit Surefire & FailSafe reports and FindBugs reports
        }
    }
    // No need to occupy a node
    stage("Quality Gate"){
        timeout(time: 1, unit: 'HOURS') { // Just in case something goes wrong, pipeline will be killed after a timeout
            def qg = waitForQualityGate() // Reuse taskId previously collected by withSonarQubeEnv
            if (qg.status != 'OK') {
                error "Pipeline aborted due to quality gate failure: ${qg.status}"
            }
        }
    }

    if (env.BRANCH_NAME == 'master') {
        withMaven(
                // Maven installation declared in the Jenkins "Global Tool Configuration"
                maven: 'M3',
                // Maven settings.xml file defined with the Jenkins Config File Provider Plugin
                // Maven settings and global settings can also be defined in Jenkins Global Tools Configuration
                mavenSettingsConfig: 'nexus-sonar',
                mavenLocalRepo: '.repository') {
            // Deploy Snapshot to Nexus
            stage('Deploy Snapshot') {
                sh "mvn site:site site:deploy"
                sh "mvn deploy -DskipTests"
            }
        }

    }
}

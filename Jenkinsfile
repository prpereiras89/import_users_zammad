#!/usr/bin/env groovy
pipeline {
    agent any
    stages {
        stage('Deployment') {
            steps {
                sh '''
                    docker-compose build
                    docker-compose up -d
                    docker network connect dsc_dashboard_app_default import_users_app
                    docker logs import_users_app
                   '''
            }
        }
    }
    post {
        failure {
            emailext to: "${env.USER_EMAIL}",
            subject: "[ERROR] deploying import user app: ${currentBuild.fullDisplayName}",
            body: "An error occurred while deploying the dashboard."
        }
        success {
            emailext to: "${env.USER_EMAIL}",
            subject: "[SUCCESS] deploying import user app: ${currentBuild.fullDisplayName}",
            body: "All good!"
        }
    }
}


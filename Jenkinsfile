pipeline {
    agent {
        label 'testintegration'
    }

    stages {
        stage('Build') {
            steps {
                sh './gradlew build publishToMavenLocal publish --refresh-dependencies'
            }
        }
    }

    post {
        always {
            // Archive JAR artifacts
            archiveArtifacts artifacts: '**/build/libs/*.jar', fingerprint: true
            // Cleanup workspace
            deleteDir()
        }
    }
}

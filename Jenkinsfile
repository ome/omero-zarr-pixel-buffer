pipeline {
    agent {
        label 'testintegration'
    }

    environment {
        // Default credentials for testing on devspace
        MAVEN_SNAPSHOTS_REPO_URL = 'http://nexus:8081/nexus/repository/maven-internal/'
        MAVEN_USER = 'admin'
        MAVEN_PASSWORD = 'admin123'

        // Disable Gradle daemon
        GRADLE_OPTS = '-Dorg.gradle.daemon=false'
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

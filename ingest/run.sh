#!/usr/bin/env bash
mvn  package assembly:single -Dexec.mainClass=com.knowledgent.bd.ingest.IngestionJobRunner
java -cp target/ingest-1.0-SNAPSHOT-jar-with-dependencies.jar  com.knowledgent.bd.ingest.IngestionJobRunner


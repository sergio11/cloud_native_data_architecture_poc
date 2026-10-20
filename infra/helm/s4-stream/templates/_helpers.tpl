{{/*
Subsystem S4 Template Helpers
*/}}
{{- define "s4-stream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "s4-stream.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "s4-stream.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "s4-stream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
subsystem: s4-stream
{{- end }}

{{- define "s4-stream.flinkCommonEnv" -}}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ .Values.kafka.bootstrapServers | quote }}
- name: ICEBERG_CATALOG_NAME
  value: {{ .Values.icebergCatalog.name | quote }}
- name: ICEBERG_CATALOG_URI
  value: {{ .Values.icebergCatalog.uri | quote }}
- name: ICEBERG_WAREHOUSE
  # NOT the S3 location. Polaris resolves an Iceberg REST client to a catalog by
  # NAME: the request is GET /api/catalog/v1/config?warehouse=<this value> and
  # Polaris matches it against catalog names. Passing the S3 warehouse here
  # (s3://warehouse/) is what made every job fail with
  #   RESTException: Unable to find warehouse s3://warehouse/
  # while the catalog existed with exactly that base location. The S3 location
  # still belongs in icebergCatalog.warehouse, which is what Polaris is
  # bootstrapped with; this value is the catalog identifier.
  value: {{ .Values.icebergCatalog.name | quote }}
- name: S3_ENDPOINT
  value: {{ .Values.icebergCatalog.s3Endpoint | quote }}
- name: S3_ACCESS_KEY
  value: {{ .Values.icebergCatalog.s3AccessKey | quote }}
- name: S3_SECRET_KEY
  value: {{ .Values.icebergCatalog.s3SecretKey | quote }}
# The three that make the Iceberg SINK actually work, and they are not redundant
# with S3_ENDPOINT/S3_ACCESS_KEY above.
#
# Iceberg writes data files through org.apache.iceberg.aws.s3.S3FileIO, which is
# built on AWS SDK v2. That SDK's default credential chain never reads the
# catalog's s3.access-key / s3.secret-key properties; it reads
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (or a profile, or IMDS). Verified in
# the cluster: with only s3.* on the catalog, the table METADATA is created -
# that goes through the Polaris REST API - and then every data file write dies:
#
#   SdkClientException: Unable to load credentials from any of the providers in
#   the chain AwsCredentialsProviderChain(credentialsProviders=
#   [SystemPropertyCredentialsProvider(), EnvironmentVariableCredentialsProvider(),
#    WebIdentityTokenCredentialsProvider(), ProfileCredentialsProvider(...),
#    ContainerCredentialsProvider(), InstanceProfileCredentialsProvider()])
#
# The warehouse therefore filled with metadata and no .parquet at all, which is
# exactly the "writes then fails with no visible cause" symptom.
- name: AWS_ACCESS_KEY_ID
  value: {{ .Values.icebergCatalog.s3AccessKey | quote }}
- name: AWS_SECRET_ACCESS_KEY
  value: {{ .Values.icebergCatalog.s3SecretKey | quote }}
- name: AWS_REGION
  value: "us-east-1"
- name: POLARIS_OAUTH2_CREDENTIAL
  value: {{ .Values.icebergCatalog.oauth2Credential | quote }}
{{- end }}

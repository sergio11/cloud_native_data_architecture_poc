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
- name: POLARIS_OAUTH2_CREDENTIAL
  value: {{ .Values.icebergCatalog.oauth2Credential | quote }}
{{- end }}

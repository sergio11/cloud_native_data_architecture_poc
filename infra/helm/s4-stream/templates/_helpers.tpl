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
  value: {{ .Values.icebergCatalog.warehouse | quote }}
- name: S3_ENDPOINT
  value: {{ .Values.icebergCatalog.s3Endpoint | quote }}
- name: S3_ACCESS_KEY
  value: {{ .Values.icebergCatalog.s3AccessKey | quote }}
- name: S3_SECRET_KEY
  value: {{ .Values.icebergCatalog.s3SecretKey | quote }}
- name: POLARIS_OAUTH2_CREDENTIAL
  value: {{ .Values.icebergCatalog.oauth2Credential | quote }}
{{- end }}

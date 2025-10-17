{{/*
Expand the name of the chart.
*/}}
{{- define "hyperzoekt.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hyperzoekt.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "hyperzoekt.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hyperzoekt.labels" -}}
helm.sh/chart: {{ include "hyperzoekt.chart" . }}
{{ include "hyperzoekt.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hyperzoekt.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hyperzoekt.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Redis URL helper
*/}}
{{- define "hyperzoekt.redisUrl" -}}
{{- if .Values.redis.enabled }}
{{- printf "redis://%s-redis:%d" (include "hyperzoekt.fullname" .) .Values.redis.service.port }}
{{- else }}
{{- printf "redis://%s:%d" .Values.redis.external.host .Values.redis.external.port }}
{{- end }}
{{- end }}

{{/*
SurrealDB URL helper
*/}}
{{- define "hyperzoekt.surrealUrl" -}}
{{- if .Values.surrealdb.enabled }}
{{- printf "http://%s-surrealdb:%d" (include "hyperzoekt.fullname" .) .Values.surrealdb.service.port }}
{{- else }}
{{- .Values.surrealdb.external.url }}
{{- end }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "hyperzoekt.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hyperzoekt.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

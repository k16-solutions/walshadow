{{/* Chart name, truncated to the 63-character label limit */}}
{{- define "walshadow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Full name. The StatefulSet's pod is <fullname>-0 and its volume claim
data-<fullname>-0, so keep it short.
*/}}
{{- define "walshadow.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 52 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 52 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 52 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "walshadow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "walshadow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "walshadow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "walshadow.labels" -}}
helm.sh/chart: {{ include "walshadow.chart" . }}
{{ include "walshadow.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "walshadow.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "walshadow.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create is false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "walshadow.image" -}}
{{- $repo := required "image.repository is required" .Values.image.repository }}
{{- $tag := required "image.tag is required: its PostgreSQL major must equal the source's" .Values.image.tag }}
{{- printf "%s:%s" $repo $tag }}
{{- end }}

{{/* Paths inside the container. Fixed so docs and runbooks can name them. */}}
{{- define "walshadow.configPath" -}}/etc/walshadow/walshadow.toml{{- end }}
{{- define "walshadow.fragmentDir" -}}/etc/walshadow/walshadow.d{{- end }}
{{- define "walshadow.dataDir" -}}/var/lib/walshadow{{- end }}
{{- define "walshadow.controlSocket" -}}/run/walshadow/control.sock{{- end }}

{{/*
Fail early on values walshadow would refuse at startup, so a bad value fails
the helm upgrade instead of crash-looping the pod.
*/}}
{{- define "walshadow.validate" -}}
{{- if not (trim .Values.config) }}
{{- fail "config is required: the root walshadow TOML config (see config/snowflake.toml)" }}
{{- end }}
{{- range $id, $spec := .Values.tenants }}
{{- if not (regexMatch "^[a-z0-9_-]{1,63}$" $id) }}
{{- fail (printf "tenants: id %q must be 1-63 characters of [a-z0-9_-]" $id) }}
{{- end }}
{{- if eq $id "default" }}
{{- fail "tenants: id \"default\" is reserved for the implicit single-database tenant" }}
{{- end }}
{{- if not (contains (printf "[tenant.%s" $id) $spec) }}
{{- fail (printf "tenants.%s: the fragment must declare [tenant.%s]" $id $id) }}
{{- end }}
{{- end }}
{{- range $name, $_ := .Values.extraFragments }}
{{- if not (regexMatch "^[A-Za-z0-9_.-]+\\.toml$" $name) }}
{{- fail (printf "extraFragments: %q must be a plain file name ending in .toml" $name) }}
{{- end }}
{{- if or (eq $name "50-api.toml") (hasPrefix "60-tenant-" $name) }}
{{- fail (printf "extraFragments: %q is a name ctl writes; use tenants or another name" $name) }}
{{- end }}
{{- end }}
{{- with .Values.daemon.bootstrapMode }}
{{- if not (has . (list "off" "direct" "object_store")) }}
{{- fail (printf "daemon.bootstrapMode: %q is not off, direct or object_store" .) }}
{{- end }}
{{- end }}
{{- end }}

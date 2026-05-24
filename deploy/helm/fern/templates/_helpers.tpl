{{- define "fern.name" -}}
fern
{{- end -}}

{{- define "fern.labels" -}}
app.kubernetes.io/name: {{ include "fern.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

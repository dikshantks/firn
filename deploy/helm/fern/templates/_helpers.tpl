{{- define "fern.name" -}}
fern
{{- end -}}

{{- define "fern.labels" -}}
app.kubernetes.io/name: {{ include "fern.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "fern.canarySuffix" -}}
{{- if .Values.isCanary -}}-canary{{- end -}}
{{- end -}}

{{- define "fern.infraSecretName" -}}
{{- .Values.requiredInfraDependencies.name }}{{ include "fern.canarySuffix" . }}
{{- end -}}

{{- define "fern.image" -}}
{{- $registry := .registry -}}
{{- $imageName := .imageName -}}
{{- $tag := .tag -}}
{{- if $registry -}}
{{ printf "%s/%s:%s" $registry $imageName $tag }}
{{- else -}}
{{ printf "%s:%s" $imageName $tag }}
{{- end -}}
{{- end -}}

{{- define "fern.usesInfraSecret" -}}
{{- if .Values.infraSettings }}true{{- else }}false{{- end -}}
{{- end -}}

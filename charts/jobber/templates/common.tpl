{{- define "common.env" -}}
{{- if .Values.global.localDev }}
- name: PULSAR_SERVICE_URL
  value: pulsar://{{ .Release.Name }}-pulsar-standalone.pulsar.svc.cluster.local:6650
{{- else }}
- name: PULSAR_SERVICE_URL
  value: pulsar://{{ .Release.Name }}-pulsar-broker.pulsar.svc.cluster.local:6650
{{- end }}
{{- end -}}

{{- define "common.env" -}}
#for prod
# - name: PULSAR_SERVICE_URL
#   value: pulsar://{{ .Release.Name }}-pulsar-broker.pulsar.svc.cluster.local:6650
- name: PULSAR_SERVICE_URL
  value: pulsar://{{ .Release.Name }}-pulsar-standalone.pulsar.svc.cluster.local:6650
{{- end -}}
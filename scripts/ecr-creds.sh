#!/bin/bash

AWS_REGION="ap-south-1"
AWS_ACCOUNT_ID="961492333341"
SECRET_NAME="ecr-creds"
SOURCE_NAMESPACE="jobber"
DOCKER_SERVER="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "🔍 Checking AWS CLI authentication..."
aws sts get-caller-identity >/dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "❌ AWS CLI is not authenticated. Run 'aws configure' first."
  exit 1
fi

echo "🔍 Checking for existing secret '$SECRET_NAME' in namespace '$SOURCE_NAMESPACE'..."
kubectl get secret "$SECRET_NAME" -n "$SOURCE_NAMESPACE" >/dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "⚠️ Secret not found. Creating a new one..."
  kubectl create secret docker-registry "$SECRET_NAME" \
    --docker-server="$DOCKER_SERVER" \
    --docker-username=AWS \
    --docker-password="$(aws ecr get-login-password --region "$AWS_REGION")" \
    -n "$SOURCE_NAMESPACE"
else
  echo "✅ Secret '$SECRET_NAME' already exists in '$SOURCE_NAMESPACE'."
fi

echo "📦 Gathering namespaces..."
NAMESPACES=$(kubectl get ns --no-headers -o custom-columns=":metadata.name" | grep -vE 'kube-|default|local-path')

if [ -z "$NAMESPACES" ]; then
  echo "❌ No namespaces found (other than system ones)."
  exit 1
fi

echo "🧭 Found namespaces: $NAMESPACES"

for ns in $NAMESPACES; do
  echo "➡️  Propagating secret to namespace: $ns ..."
  kubectl get secret "$SECRET_NAME" -n "$SOURCE_NAMESPACE" -o yaml \
  | sed "s/namespace: $SOURCE_NAMESPACE/namespace: $ns/" \
  | kubectl apply -f - >/dev/null

  kubectl patch serviceaccount default -n "$ns" \
    -p "{\"imagePullSecrets\": [{\"name\": \"$SECRET_NAME\"}]}" >/dev/null 2>&1

  echo "✅ Applied secret + patched ServiceAccount in namespace: $ns"
done

echo -e "\n🔎 Verifying configuration..."
for ns in $NAMESPACES; do
  echo "Namespace: $ns"
  kubectl get serviceaccount default -n "$ns" -o jsonpath="{.imagePullSecrets[*].name}" | grep "$SECRET_NAME" && echo "  ✔️ ImagePullSecret OK" || echo "  ❌ Missing imagePullSecret"
done

echo -e "\n🎉 All done! Your AWS ECR credentials have been propagated across namespaces."

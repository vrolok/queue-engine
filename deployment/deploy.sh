#!/bin/bash
# Deployment script for Queue Engine with Ray

set -e

ENV=${1:-dev}
DEPLOY_DIR="$(dirname "$0")"
CONFIG_DIR="${DEPLOY_DIR}/config"

echo "Deploying Queue Engine with Ray to ${ENV} environment"

# Load environment-specific variables
if [ -f "${CONFIG_DIR}/${ENV}.env" ]; then
    echo "Loading ${ENV} environment variables"
    set -a
    source "${CONFIG_DIR}/${ENV}.env"
    set +a
else
    echo "Error: Environment file ${CONFIG_DIR}/${ENV}.env not found"
    exit 1
fi

# Function to deploy to Kubernetes
deploy_kubernetes() {
    echo "Deploying to Kubernetes cluster"
    
    # Create namespace if it doesn't exist
    kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
    
    # Apply ConfigMap
    kubectl apply -f "${DEPLOY_DIR}/kubernetes/configmap.yaml" -n ${NAMESPACE}
    
    # Apply Ray Cluster
    kubectl apply -f "${DEPLOY_DIR}/kubernetes/ray-cluster.yaml" -n ${NAMESPACE}
    
    # Apply Services
    kubectl apply -f "${DEPLOY_DIR}/kubernetes/service.yaml" -n ${NAMESPACE}
    
    # Apply Ingress if not local
    if [ "${ENV}" != "local" ]; then
        kubectl apply -f "${DEPLOY_DIR}/kubernetes/ingress.yaml" -n ${NAMESPACE}
    fi
    
    echo "Kubernetes deployment complete. Waiting for Ray cluster to start..."
    kubectl wait --for=condition=available --timeout=300s deployment/queue-engine-cluster-head -n ${NAMESPACE}
    
    # Print access information
    if [ "${ENV}" == "local" ]; then
        echo "Access the Queue Engine API at: http://localhost:8000/api/v1"
        echo "Access the Ray Dashboard at: http://localhost:8265"
        
        # Set up port-forwarding
        echo "Setting up port forwarding..."
        kubectl port-forward svc/queue-engine-service 8000:8000 8265:8265 -n ${NAMESPACE} &
        
    else
        echo "Access the Queue Engine API at: https://${API_HOST}/api"
        echo "Access the Ray Dashboard at: https://${API_HOST}/dashboard"
    fi
}

# Function to deploy using docker-compose
deploy_docker_compose() {
    echo "Deploying with docker-compose"
    
    # Create Docker network if it doesn't exist
    docker network inspect ray-network >/dev/null 2>&1 || docker network create ray-network
    
    # Create required directories for volumes
    mkdir -p "${DEPLOY_DIR}/prometheus" "${DEPLOY_DIR}/grafana/provisioning"
    
    # Deploy with docker-compose
    docker-compose -f "${DEPLOY_DIR}/docker-compose.yaml" up -d
    
    echo "Docker Compose deployment complete"
    echo "Access the Queue Engine API at: http://localhost:8000/api/v1"
    echo "Access the Ray Dashboard at: http://localhost:8265"
    echo "Access Prometheus at: http://localhost:9090"
    echo "Access Grafana at: http://localhost:3000 (admin/admin)"
}

# Function to deploy to AWS Ray cluster
deploy_ray_cluster() {
    echo "Deploying to AWS Ray cluster"
    
    # Check if ray CLI is available
    if ! command -v ray &> /dev/null; then
        echo "Ray CLI not found. Installing Ray..."
        pip install -U "ray[default]"
    fi
    
    # Start or connect to Ray cluster
    if [ "${RAY_CLUSTER_MODE}" == "create" ]; then
        echo "Creating new Ray cluster..."
        ray up -y "${DEPLOY_DIR}/ray-cluster.yaml"
    else
        echo "Connecting to existing Ray cluster at ${RAY_ADDRESS}"
        # Nothing to do here, the application will connect to RAY_ADDRESS
    fi
    
    # Deploy application
    echo "Deploying application to Ray cluster..."
    ray rsync_up "${DEPLOY_DIR}/ray-cluster.yaml" "../" "/home/ubuntu/queue-engine"
    
    # Install dependencies and start application
    ray exec "${DEPLOY_DIR}/ray-cluster.yaml" "cd /home/ubuntu/queue-engine && pip install -r requirements.txt && python -m src.main" --runtime-env-json='{"env_vars": {"RAY_ADDRESS": "auto"}}'
    
    # Get head node public IP to access services
    HEAD_IP=$(ray get-head-ip "${DEPLOY_DIR}/ray-cluster.yaml")
    
    echo "Deployment complete"
    echo "Access the Queue Engine API at: http://${HEAD_IP}:8000/api/v1"
    echo "Access the Ray Dashboard at: http://${HEAD_IP}:8265"
}

# Deploy based on deployment type
case ${DEPLOYMENT_TYPE} in
    kubernetes)
        deploy_kubernetes
        ;;
    docker-compose)
        deploy_docker_compose
        ;;
    ray-cluster)
        deploy_ray_cluster
        ;;
    *)
        echo "Error: Unknown deployment type ${DEPLOYMENT_TYPE}"
        echo "Supported types: kubernetes, docker-compose, ray-cluster"
        exit 1
        ;;
esac

echo "Deployment completed successfully!"
# GKE Domain Module
# Deploys a single domain on Kubernetes using the angzarr Helm chart
#
# Business config (portable): aggregate, process_manager, sagas, projectors
# Operational config (K8s native): scaling, resources, storage, messaging
# GKE-specific: Workload Identity support

terraform {
  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
}

check "aggregate_pm_exclusive" {
  assert {
    condition     = !(var.aggregate.enabled && var.process_manager.enabled)
    error_message = "aggregate and process_manager are mutually exclusive. A domain can be either an aggregate (command handler) or a process manager (cross-domain orchestrator), not both."
  }
}

locals {
  # Build applications.business list for aggregates/PMs
  business_apps = var.aggregate.enabled ? [
    {
      name   = "${var.domain}-aggregate"
      domain = var.domain
      image = {
        repository = var.images.logic
        tag        = "latest"
      }
      port = 50053
      resources = try(var.scaling.aggregate.resources, {
        requests = { cpu = "100m", memory = "128Mi" }
        limits   = { cpu = "1", memory = "512Mi" }
      })
      env      = [for k, v in var.aggregate.env : { name = k, value = v }]
      upcaster = { enabled = var.aggregate.upcaster.enabled }
    }
    ] : (var.process_manager.enabled ? [
      {
        name          = "${var.domain}-pm"
        domain        = var.domain
        sourceDomains = var.process_manager.source_domains
        image = {
          repository = var.images.logic
          tag        = "latest"
        }
        port = 50053
        resources = try(var.scaling.process_manager.resources, {
          requests = { cpu = "100m", memory = "128Mi" }
          limits   = { cpu = "1", memory = "512Mi" }
        })
        env = [for k, v in var.process_manager.env : { name = k, value = v }]
      }
  ] : [])

  # Build applications.sagas list
  saga_apps = [
    for name, saga in var.sagas : {
      name         = "saga-${var.domain}-${name}"
      sourceDomain = var.domain
      domain       = saga.target_domain
      image = {
        repository = lookup(var.images.saga_logic, name, var.images.logic)
        tag        = "latest"
      }
      port = 50053
      resources = try(var.scaling.sagas[name].resources, {
        requests = { cpu = "100m", memory = "128Mi" }
        limits   = { cpu = "500m", memory = "256Mi" }
      })
      env = [for k, v in saga.env : { name = k, value = v }]
    }
  ]

  # Build applications.projectors list
  projector_apps = [
    for name, projector in var.projectors : {
      name   = "projector-${var.domain}-${name}"
      domain = var.domain
      image = {
        repository = lookup(var.images.projector_logic, name, var.images.logic)
        tag        = "latest"
      }
      port = 50053
      resources = try(var.scaling.projectors[name].resources, {
        requests = { cpu = "100m", memory = "128Mi" }
        limits   = { cpu = "500m", memory = "256Mi" }
      })
      env = [for k, v in projector.env : { name = k, value = v }]
    }
  ]

  # Workload Identity annotation
  service_account_annotations = var.workload_identity.enabled && var.workload_identity.gcp_service_account != null ? {
    "iam.gke.io/gcp-service-account" = var.workload_identity.gcp_service_account
  } : {}
}

resource "helm_release" "domain" {
  name       = var.domain
  namespace  = var.namespace
  chart      = var.chart_path != null ? var.chart_path : "${path.module}/../../../../helm/angzarr"
  repository = var.chart_repository

  create_namespace = var.create_namespace
  wait             = var.wait
  timeout          = var.timeout

  # Replica count
  set {
    name  = "replicaCount"
    value = var.aggregate.enabled ? try(var.scaling.aggregate.replicas, 1) : try(var.scaling.process_manager.replicas, 1)
  }

  # Coordinator images
  set {
    name  = "images.aggregate.repository"
    value = var.images.coordinator_aggregate
  }
  set {
    name  = "images.saga.repository"
    value = var.images.coordinator_saga
  }
  set {
    name  = "images.projector.repository"
    value = var.images.coordinator_projector
  }

  # Storage configuration
  set {
    name  = "storage.type"
    value = var.storage.type
  }
  dynamic "set" {
    for_each = var.storage.type == "mongodb" ? [1] : []
    content {
      name  = "storage.mongodb.uri"
      value = var.storage.mongodb.uri
    }
  }
  dynamic "set" {
    for_each = var.storage.type == "postgres" ? [1] : []
    content {
      name  = "storage.postgres.uri"
      value = var.storage.postgres.uri
    }
  }

  # Messaging configuration
  set {
    name  = "messaging.type"
    value = var.messaging.type
  }
  dynamic "set" {
    for_each = var.messaging.type == "amqp" ? [1] : []
    content {
      name  = "messaging.amqp.enabled"
      value = "true"
    }
  }
  dynamic "set" {
    for_each = var.messaging.type == "amqp" ? [1] : []
    content {
      name  = "messaging.amqp.url"
      value = var.messaging.amqp.url
    }
  }
  dynamic "set" {
    for_each = var.messaging.type == "kafka" ? [1] : []
    content {
      name  = "messaging.kafka.enabled"
      value = "true"
    }
  }
  dynamic "set" {
    for_each = var.messaging.type == "kafka" ? [1] : []
    content {
      name  = "messaging.kafka.bootstrapServers"
      value = var.messaging.kafka.bootstrap_servers
    }
  }

  # Autoscaling
  dynamic "set" {
    for_each = var.aggregate.enabled && try(var.scaling.aggregate.max_replicas, 1) > try(var.scaling.aggregate.min_replicas, 1) ? [1] : []
    content {
      name  = "autoscaling.enabled"
      value = "true"
    }
  }
  dynamic "set" {
    for_each = var.aggregate.enabled && try(var.scaling.aggregate.max_replicas, 1) > try(var.scaling.aggregate.min_replicas, 1) ? [1] : []
    content {
      name  = "autoscaling.minReplicas"
      value = try(var.scaling.aggregate.min_replicas, 1)
    }
  }
  dynamic "set" {
    for_each = var.aggregate.enabled && try(var.scaling.aggregate.max_replicas, 1) > try(var.scaling.aggregate.min_replicas, 1) ? [1] : []
    content {
      name  = "autoscaling.maxReplicas"
      value = try(var.scaling.aggregate.max_replicas, 10)
    }
  }

  # Service account
  set {
    name  = "serviceAccount.create"
    value = var.create_service_account
  }
  dynamic "set" {
    for_each = var.service_account_name != null ? [1] : []
    content {
      name  = "serviceAccount.name"
      value = var.service_account_name
    }
  }

  # Workload Identity annotation
  dynamic "set" {
    for_each = local.service_account_annotations
    content {
      name  = "serviceAccount.annotations.${replace(set.key, ".", "\\.")}"
      value = set.value
    }
  }

  # Image pull secrets
  dynamic "set" {
    for_each = var.image_pull_secrets
    content {
      name  = "imagePullSecrets[${set.key}].name"
      value = set.value
    }
  }

  # Node selector
  dynamic "set" {
    for_each = var.node_selector
    content {
      name  = "nodeSelector.${set.key}"
      value = set.value
    }
  }

  # Logging
  set {
    name  = "logging.level"
    value = var.log_level
  }

  # Labels
  dynamic "set" {
    for_each = var.labels
    content {
      name  = "podLabels.${set.key}"
      value = set.value
    }
  }

  # Applications via values file
  values = [
    yamlencode({
      applications = {
        business   = local.business_apps
        sagas      = local.saga_apps
        projectors = local.projector_apps
      }
    })
  ]
}

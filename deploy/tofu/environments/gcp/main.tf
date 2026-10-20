# GCP Environment - Main
# Example deployment of angzarr to Google Cloud Run

terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Uncomment and configure for remote state
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "angzarr/gcp"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  labels = {
    "environment" = var.environment
    "managed-by"  = "opentofu"
    "app"         = "angzarr"
  }

  # Container images
  images = {
    grpc_gateway          = "${var.image_registry}/grpc-gateway:${var.image_tag}"
    coordinator_aggregate = "${var.image_registry}/angzarr-aggregate:${var.image_tag}"
    coordinator_saga      = "${var.image_registry}/angzarr-saga:${var.image_tag}"
    coordinator_projector = "${var.image_registry}/angzarr-projector:${var.image_tag}"
    coordinator_pm        = "${var.image_registry}/angzarr-pm:${var.image_tag}"
    stream                = "${var.image_registry}/angzarr-stream:${var.image_tag}"
  }

  # Domain-specific images
  domain_images = {
    order = {
      logic = "${var.image_registry}/agg-order:${var.image_tag}"
      saga_logic = {
        fulfillment = "${var.image_registry}/saga-order-fulfillment:${var.image_tag}"
      }
      projector_logic = {
        web = "${var.image_registry}/projector-order-web:${var.image_tag}"
      }
    }
    inventory = {
      logic           = "${var.image_registry}/agg-inventory:${var.image_tag}"
      saga_logic      = {}
      projector_logic = {}
    }
    fulfillment = {
      logic           = "${var.image_registry}/agg-fulfillment:${var.image_tag}"
      saga_logic      = {}
      projector_logic = {}
    }
  }
}

#------------------------------------------------------------------------------
# Infrastructure: Database
#------------------------------------------------------------------------------
module "cloudsql" {
  source = "../../modules/cloudsql"

  project_id        = var.project_id
  region            = var.region
  instance_name     = "angzarr-${var.environment}"
  database_name     = "angzarr"
  tier              = var.database_tier
  availability_type = var.database_ha ? "REGIONAL" : "ZONAL"
  labels            = local.labels
}

#------------------------------------------------------------------------------
# Infrastructure: Messaging
#------------------------------------------------------------------------------
module "pubsub" {
  source = "../../modules/pubsub"

  project_id        = var.project_id
  events_topic_name = "angzarr-events-${var.environment}"
  labels            = local.labels
}

#------------------------------------------------------------------------------
# Infrastructure: Stream
#------------------------------------------------------------------------------
module "infrastructure" {
  source = "../../modules/infrastructure"

  project_id = var.project_id
  region     = var.region

  stream = {
    enabled = var.enable_stream
    image   = local.images.stream
  }

  coordinator_env       = merge(module.cloudsql.coordinator_env, module.pubsub.coordinator_env)
  allow_unauthenticated = var.allow_unauthenticated
  labels                = local.labels
}

#------------------------------------------------------------------------------
# Domain: Order
#------------------------------------------------------------------------------
module "order" {
  source = "../../modules/domain"

  domain     = "order"
  project_id = var.project_id
  region     = var.region

  aggregate = {
    enabled = true
    env     = {}
  }

  sagas = {
    fulfillment = {
      target_domain = "fulfillment"
      env           = {}
    }
  }

  projectors = {
    web = {
      env = {}
    }
  }

  images = {
    grpc_gateway          = local.images.grpc_gateway
    coordinator_aggregate = local.images.coordinator_aggregate
    coordinator_saga      = local.images.coordinator_saga
    coordinator_projector = local.images.coordinator_projector
    coordinator_pm        = local.images.coordinator_pm
    logic                 = local.domain_images.order.logic
    saga_logic            = local.domain_images.order.saga_logic
    projector_logic       = local.domain_images.order.projector_logic
  }

  discovery_env   = module.registry.discovery_env
  coordinator_env = merge(module.cloudsql.coordinator_env, module.pubsub.coordinator_env)
  labels          = local.labels

  iam = {
    allow_unauthenticated = var.allow_unauthenticated
  }

  depends_on = [module.fulfillment]
}

#------------------------------------------------------------------------------
# Domain: Inventory
#------------------------------------------------------------------------------
module "inventory" {
  source = "../../modules/domain"

  domain     = "inventory"
  project_id = var.project_id
  region     = var.region

  aggregate = {
    enabled = true
    env     = {}
  }

  images = {
    grpc_gateway          = local.images.grpc_gateway
    coordinator_aggregate = local.images.coordinator_aggregate
    coordinator_saga      = local.images.coordinator_saga
    coordinator_projector = local.images.coordinator_projector
    coordinator_pm        = local.images.coordinator_pm
    logic                 = local.domain_images.inventory.logic
  }

  discovery_env   = module.registry.discovery_env
  coordinator_env = merge(module.cloudsql.coordinator_env, module.pubsub.coordinator_env)
  labels          = local.labels

  iam = {
    allow_unauthenticated = var.allow_unauthenticated
  }
}

#------------------------------------------------------------------------------
# Domain: Fulfillment
#------------------------------------------------------------------------------
module "fulfillment" {
  source = "../../modules/domain"

  domain     = "fulfillment"
  project_id = var.project_id
  region     = var.region

  aggregate = {
    enabled = true
    env     = {}
  }

  images = {
    grpc_gateway          = local.images.grpc_gateway
    coordinator_aggregate = local.images.coordinator_aggregate
    coordinator_saga      = local.images.coordinator_saga
    coordinator_projector = local.images.coordinator_projector
    coordinator_pm        = local.images.coordinator_pm
    logic                 = local.domain_images.fulfillment.logic
  }

  discovery_env   = module.registry.discovery_env
  coordinator_env = merge(module.cloudsql.coordinator_env, module.pubsub.coordinator_env)
  labels          = local.labels

  iam = {
    allow_unauthenticated = var.allow_unauthenticated
  }
}

#------------------------------------------------------------------------------
# Registry: Service Discovery
#------------------------------------------------------------------------------
module "registry" {
  source = "../../modules/registry"

  services = merge(
    module.order.discovery_entries,
    module.inventory.discovery_entries,
    module.fulfillment.discovery_entries,
  )

  stream_url = module.infrastructure.stream_url
}

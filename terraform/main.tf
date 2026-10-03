terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── Naming convention: {project}-{env}-{resource} ──
locals {
  name_prefix = "${var.project}-${var.env}"
  raw_bucket  = "${var.project}-${var.env}-raw"

  common_tags = {
    Project     = var.project
    Environment = var.env
    ManagedBy   = "terraform"
  }
}

# ── Core ──
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "ap-south-1"
}

variable "project" {
  description = "Project name — used in all resource naming"
  type        = string
  default     = "benmart"
}

variable "env" {
  description = "Environment (dev/staging/prod)"
  type        = string
  default     = "dev"
}

# ── S3 ──
variable "data_lake_layers" {
  description = "Data lake layer names — S3 bucket per layer"
  type        = list(string)
  default     = ["raw", "bronze", "silver", "gold"]
}

# ── RDS ──
variable "rds_instance_class" {
  description = "RDS instance size"
  type        = string
  default     = "db.t3.micro"
}

variable "rds_engine_version" {
  description = "MySQL engine version"
  type        = string
  default     = "8.0"
}

variable "rds_db_name" {
  description = "Initial database name"
  type        = string
  default     = "benmart"
}

variable "rds_username" {
  description = "RDS master username"
  type        = string
  default     = "admin"
}

variable "rds_password" {
  description = "RDS master password"
  type        = string
  sensitive   = true
}

variable "rds_backup_retention" {
  description = "Backup retention days — must be >= 1 for DMS CDC (enables binlog)"
  type        = number
  default     = 7
}

# ── Glue ──
variable "glue_jobs" {
  description = "Glue job definitions — name, script, workers"
  type = map(object({
    script_name = string
    workers     = number
    description = string
  }))
  default = {
    full_pipeline = {
      script_name = "glue_pipeline_job.py"
      workers     = 3
      description = "Full Bronze → Silver → Gold pipeline"
    }
    bronze = {
      script_name = "glue_bronze_job.py"
      workers     = 2
      description = "Bronze layer only"
    }
    silver = {
      script_name = "glue_silver_job.py"
      workers     = 2
      description = "Silver layer only"
    }
    gold = {
      script_name = "glue_gold_job.py"
      workers     = 2
      description = "Gold layer only"
    }
  }
}

variable "glue_version" {
  description = "AWS Glue version"
  type        = string
  default     = "5.0"
}

# ── Lambda ──
variable "lambda_functions" {
  description = "Lambda function definitions"
  type = map(object({
    handler     = string
    runtime     = string
    timeout     = number
    memory      = number
    description = string
  }))
  default = {
    api_ingestion = {
      handler     = "lambda_function.lambda_handler"
      runtime     = "python3.12"
      timeout     = 30
      memory      = 128
      description = "Fetches pricing data from API and writes to S3"
    }
  }
}

# ── SNS ──
variable "alert_email" {
  description = "Email for pipeline alerts"
  type        = string
  default     = "benlearn24@gmail.com"
}

# ── Step Functions ──
variable "pipeline_schedule" {
  description = "Cron expression for daily pipeline (UTC)"
  type        = string
  default     = "cron(30 0 * * ? *)"
}

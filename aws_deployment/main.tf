############################################
# 1) Terraform + Providers
############################################
terraform {
  required_version = ">= 1.2.0"
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

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

############################################
# 2) VPC + Subnets
############################################
data "aws_availability_zones" "available" {}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = {
    Name = "unified-vpc"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  tags = {
    Name = "unified-igw"
  }
}

# Public Subnets
resource "aws_subnet" "public_subnet1" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.3.0/24"
  map_public_ip_on_launch = true
  availability_zone       = slice(data.aws_availability_zones.available.names, 0, 2)[0]
  tags = {
    Name = "unified-public-subnet1"
  }
}
resource "aws_subnet" "public_subnet2" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.4.0/24"
  map_public_ip_on_launch = true
  availability_zone       = slice(data.aws_availability_zones.available.names, 0, 2)[1]
  tags = {
    Name = "unified-public-subnet2"
  }
}

# Private Subnets
resource "aws_subnet" "private_subnet1" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = slice(data.aws_availability_zones.available.names, 0, 2)[0]
  tags = {
    Name = "unified-private-subnet1"
  }
}
resource "aws_subnet" "private_subnet2" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = slice(data.aws_availability_zones.available.names, 0, 2)[1]
  tags = {
    Name = "unified-private-subnet2"
  }
}

resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.main.id
  tags = {
    Name = "unified-public-rt"
  }
}

resource "aws_route" "public_internet_access" {
  route_table_id         = aws_route_table.public_rt.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.igw.id
}

resource "aws_route_table_association" "public_rta1" {
  subnet_id      = aws_subnet.public_subnet1.id
  route_table_id = aws_route_table.public_rt.id
}
resource "aws_route_table_association" "public_rta2" {
  subnet_id      = aws_subnet.public_subnet2.id
  route_table_id = aws_route_table.public_rt.id
}

############################################
# 3) EKS (for Streaming)
############################################
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "19.15.3"

  cluster_name    = var.cluster_name
  cluster_version = "1.27"

  vpc_id     = aws_vpc.main.id
  subnet_ids = [
    aws_subnet.private_subnet1.id,
    aws_subnet.private_subnet2.id
  ]

  eks_managed_node_groups = {
    default = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["t3.medium"]
    }
  }
}

############################################
# 4) MSK (Kafka) for Real-Time
############################################
resource "aws_security_group" "msk_sg" {
  name        = "msk-sg"
  description = "Security group for MSK brokers"
  vpc_id      = aws_vpc.main.id

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["10.0.0.0/16"] # naive approach, or restrict to EKS node IPs
  }

  tags = {
    Name = "msk-sg"
  }
}

resource "aws_msk_configuration" "kafka_config" {
  name           = "my-kafka-config"
  kafka_versions = ["3.4.0"]
  server_properties = <<-EOF
auto.create.topics.enable = true
EOF
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "my-msk-cluster"
  kafka_version          = "3.4.0"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = [aws_subnet.private_subnet1.id, aws_subnet.private_subnet2.id]
    security_groups = [aws_security_group.msk_sg.id]
  }

  configuration_info {
    arn      = aws_msk_configuration.kafka_config.arn
    revision = aws_msk_configuration.kafka_config.latest_revision
  }

  tags = {
    Name = "my-msk-cluster"
  }
}

############################################
# 5) EMR (for Batch)
############################################
resource "aws_security_group" "emr_master_sg" {
  name        = "emr-master-sg"
  description = "EMR master security group"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name = "emr-master-sg"
  }
}

resource "aws_security_group" "emr_core_sg" {
  name        = "emr-core-sg"
  description = "EMR core security group"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name = "emr-core-sg"
  }
}

# IAM Roles for EMR
resource "aws_iam_role" "emr_service_role" {
  name = "MyEMR_DefaultRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "elasticmapreduce.amazonaws.com"
      }
    }]
  })
}
resource "aws_iam_role_policy_attachment" "emr_service_role_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2_role" {
  name = "MyEMR_EC2_DefaultRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}
resource "aws_iam_role_policy_attachment" "emr_ec2_role_policy" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_ec2_instance_profile" {
  name = "MyEMR_EC2_DefaultRole"
  role = aws_iam_role.emr_ec2_role.name
}

resource "aws_s3_bucket" "emr_logs_bucket" {
  bucket = "mentorhub-emr-logs-unique-example"
}

resource "aws_emr_cluster" "spark_cluster" {
  name          = "emr-spark-cluster"
  release_label = "emr-6.10.0"
  applications  = ["Hadoop", "Spark"]

  service_role = aws_iam_role.emr_service_role.name

  ec2_attributes {
    subnet_id                         = aws_subnet.public_subnet1.id
    instance_profile                  = aws_iam_instance_profile.emr_ec2_instance_profile.name
    emr_managed_master_security_group = aws_security_group.emr_master_sg.id
    emr_managed_slave_security_group  = aws_security_group.emr_core_sg.id
  }

  master_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 1
  }
  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 2
  }

  log_uri = "s3://${aws_s3_bucket.emr_logs_bucket.id}/"
  keep_job_flow_alive_when_no_steps = true

  # For example, if you had a bootstrap script
  # bootstrap_action {
  #   path = "s3://some-bucket/scripts/bootstrap_install_reqs.sh"
  #   name = "install-reqs"
  # }

  tags = {
    Name = "emr-spark-cluster"
  }
}


############################################
# 6) ECR + Docker Build/Push
############################################
variable "docker_image_name" {
  type    = string
  default = "mentorhub/data-code"
}

resource "aws_ecr_repository" "data_repo" {
  name = replace(var.docker_image_name, "/", "-") # ECR repo names don't allow '/'
}

# We'll use a null_resource and local-exec to do docker build/push
resource "null_resource" "docker_build_push" {
  provisioner "local-exec" {
    command = <<-EOC
      # 1) Login to ECR
      aws ecr get-login-password --region ${var.aws_region} \
        | docker login --username AWS --password-stdin ${aws_ecr_repository.data_repo.repository_url}

      # 2) Build Docker image from local Dockerfile
      docker build -t ${var.docker_image_name} /path/to/your/data/code

      # 3) Tag for ECR
      docker tag ${var.docker_image_name}:latest ${aws_ecr_repository.data_repo.repository_url}:latest

      # 4) Push
      docker push ${aws_ecr_repository.data_repo.repository_url}:latest
    EOC
  }
  depends_on = [aws_ecr_repository.data_repo]
}

output "ecr_repo_url" {
  value = aws_ecr_repository.data_repo.repository_url
}

############################################
# Variables + Outputs
############################################

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "cluster_name" {
  type    = string
  default = "my-eks-cluster"
}

output "msk_bootstrap_brokers" {
  description = "Plaintext bootstrap servers for MSK"
  value       = aws_msk_cluster.this.bootstrap_brokers
}

output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "emr_cluster_id" {
  value = aws_emr_cluster.spark_cluster.id
}
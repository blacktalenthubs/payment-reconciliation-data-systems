terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.3.0"
}

provider "aws" {
  region = "us-east-1"
}

############################################
# Networking: VPC, Subnet, IGW, Route Table
############################################

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = {
    Name = "emr-vpc"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  tags = {
    Name = "emr-igw"
  }
}

resource "aws_subnet" "public_subnet" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
  availability_zone       = "us-east-1a"

  tags = {
    Name = "emr-public-subnet"
  }
}

resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.main.id
  tags = {
    Name = "emr-public-rt"
  }
}

resource "aws_route" "public_internet_access" {
  route_table_id         = aws_route_table.public_rt.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.igw.id
}

resource "aws_route_table_association" "public_rta" {
  subnet_id      = aws_subnet.public_subnet.id
  route_table_id = aws_route_table.public_rt.id
}

########################################
# IAM Roles for EMR
########################################

resource "aws_iam_role" "emr_service_role" {
  name = "MyEMR_DefaultRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action: "sts:AssumeRole",
        Effect: "Allow",
        Principal: {
          Service: "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_role_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2_role" {
  name = "MyEMR_EC2_DefaultRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action: "sts:AssumeRole",
        Effect: "Allow",
        Principal: {
          Service: "ec2.amazonaws.com"
        }
      }
    ]
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

########################################
# Security Groups for EMR
########################################

resource "aws_security_group" "emr_master_sg" {
  name        = "emr-master-sg"
  description = "EMR master security group"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # For demo, SSH from anywhere
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

########################################
# S3 Bucket for EMR Logs
########################################

resource "aws_s3_bucket" "emr_logs_bucket" {
  bucket = "mentorhub-emr-logs-unique-name" # Replace with a globally unique name
  # No ACL set here. The bucket is private by default.
}
resource "aws_s3_bucket" "emr_scripts_bucket" {
  bucket = "mentorhub-training-emr-scripts-bucket" # Globally unique
}

########################################
# Upload Code & Requirements to S3
########################################

resource "aws_s3_object" "pyspark_script" {
  bucket = aws_s3_bucket.emr_scripts_bucket.id
  key    = "scripts/ingestions.py"
  source = "ingestions.py"
  etag   = filemd5("ingestions.py")
}

resource "aws_s3_object" "requirements_file" {
  bucket = aws_s3_bucket.emr_scripts_bucket.id
  key    = "scripts/requirements.txt"
  source = "requirements.txt"
  etag   = filemd5("requirements.txt")
}

resource "aws_s3_object" "bootstrap_install_reqs" {
  bucket = aws_s3_bucket.emr_scripts_bucket.id
  key    = "scripts/bootstrap_install_reqs.sh"
  source = "bootstrap_install_reqs.sh"
  etag   = filemd5("bootstrap_install_reqs.sh")
}



########################################
# EMR Cluster
########################################

resource "aws_emr_cluster" "spark_cluster" {
  name          = "emr-spark-cluster"
  release_label = "emr-6.10.0"
  applications  = ["Hadoop", "Spark"]

  service_role = aws_iam_role.emr_service_role.name

  ec2_attributes {
    subnet_id                         = aws_subnet.public_subnet.id
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

bootstrap_action {
  path = "s3://${aws_s3_bucket.emr_scripts_bucket.id}/scripts/bootstrap_install_reqs.sh"
  name = "mentorhub"
}

  tags = {
    Environment = "dev"
  }
}


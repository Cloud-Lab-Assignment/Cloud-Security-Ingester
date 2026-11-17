module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = local.name
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 8, 200 + i)]

  enable_nat_gateway   = true
  single_nat_gateway   = true # cost saving for a course project
  enable_dns_hostnames = true
  enable_dns_support   = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
  }
}

# Free gateway endpoints keep S3 / DynamoDB traffic off the NAT.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids
}

# ---------- Security groups ----------
resource "aws_security_group" "msk" {
  name_prefix = "${local.name}-msk-"
  vpc_id      = module.vpc.vpc_id
  description = "MSK brokers"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group_rule" "msk_from_vpc" {
  type              = "ingress"
  security_group_id = aws_security_group.msk.id
  from_port         = 9092
  to_port           = 9098
  protocol          = "tcp"
  cidr_blocks       = [var.vpc_cidr]
  description       = "Kafka (plaintext 9092, TLS 9094, SCRAM 9096) from inside the VPC"
}

resource "aws_security_group" "lambda" {
  name_prefix = "${local.name}-lambda-"
  vpc_id      = module.vpc.vpc_id
  description = "event-normalizer Lambda"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "emr_extra" {
  name_prefix = "${local.name}-emr-"
  vpc_id      = module.vpc.vpc_id
  description = "EMR Flink cluster (additional rules; EMR manages its own master/slave SGs)"

  lifecycle {
    create_before_destroy = true
  }
}

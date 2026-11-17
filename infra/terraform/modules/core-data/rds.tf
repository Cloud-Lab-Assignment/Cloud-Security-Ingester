# Incident Management: PostgreSQL 16 for incidents and the 3PC coordinator/participant logs.
# RDS for PostgreSQL 16 sets rds.force_ssl = 1 by default, so clients must use TLS
# (DATABASE_SSL=true; the image ships the RDS CA bundle).

resource "aws_db_subnet_group" "incidents" {
  name       = "${var.name_prefix}incidents"
  subnet_ids = var.db_subnet_ids
}

resource "aws_security_group" "incidents_db" {
  name        = "${var.name_prefix}incidents-db"
  description = "Postgres access for incident-management"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "incidents_db_postgres" {
  for_each = toset(var.allowed_security_group_ids)

  security_group_id            = aws_security_group.incidents_db.id
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  description                  = "Postgres from ${each.value}"
}

resource "aws_db_instance" "incidents" {
  identifier     = "${var.name_prefix}incidents"
  engine         = "postgres"
  engine_version = "16" # latest 16.x minor available in the region
  instance_class = "db.t4g.micro"

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = "incidents"
  username = "incidents"
  # The master password is generated and stored in Secrets Manager by RDS.
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.incidents.name
  vpc_security_group_ids = [aws_security_group.incidents_db.id]
  publicly_accessible    = false
  port                   = 5432

  # Assignment environment: cheap and disposable.
  multi_az                   = false
  backup_retention_period    = 1
  skip_final_snapshot        = true
  deletion_protection        = false
  auto_minor_version_upgrade = true
  apply_immediately          = true
}

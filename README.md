# Cloud Security Monitoring: core services

My part of the CS G527 Cloud Computing group assignment (scenario S25, Cloud Security Monitoring; concept: 3PC).

Services:
- **Correlation Engine**: groups related events into candidate incidents
- **Incident Management Service**: creates and updates incidents
- **Event Store**: keeps raw events and the history of correlations

The group repo is https://github.com/Cloud-Lab-Assignment/Cloud-Security-Ingester. These services go there through a pull request under `services/<name>/`.

## Start here

1. `docs/project-brief.md`: the spec. It covers the assignment, who owns what, and the upstream Kafka contracts.
2. `docs/superpowers/plans/2026-11-16-core-services.md`: the implementation plan. Follow its **Working Rules**. In short: no installs beyond the approved list, no `terraform plan`/`apply`, no actions on the group repo, and push only when asked.

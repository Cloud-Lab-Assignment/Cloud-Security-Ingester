# Cloud Security Monitoring (CS G527, scenario S25, concept: 3PC)

Eight microservices on AWS: Log Ingestion, Event Normalizer (Lambda), Detection Rules, Security Analytics (Flink on EMR),
Correlation Engine, Event Store, Incident Management, Investigation Query API.
Infrastructure is Terraform (`infra/terraform`), deployments are ArgoCD from `gitops/`, images are built by `.github/workflows/ci.yml`.

| Path | Contents |
|---|---|
| `services/` | all eight services (`event-normalizer` = Lambda, `security-analytics` = Flink job) |
| `packages/`, `proto/` | shared libraries (`@core/common`, `@core/tpc`) and the gRPC contract |
| `infra/terraform/` | platform (VPC, EKS, MSK, Lambda, EMR, ArgoCD, monitoring) + `modules/core-data` (DynamoDB, S3, RDS) |
| `gitops/` | ArgoCD Applications, Kustomize manifests, HPAs, Grafana dashboard |
| `loadtest/` | k6 load test for the HPA demo |
| `docs/DEPLOYMENT.md` | **step-by-step deployment and demo guide** |
| `docs/design/DESIGN.md` | **design document** (whole system) |
| `docs/handoff.md`, `docs/design/core-services.md` | core services contracts and detailed 3PC design (Rishit) |
| `docs/task_division.txt` | who did what |

---

# Cloud Security Monitoring: core services

My part of the CS G527 Cloud Computing group assignment (scenario S25, Cloud Security Monitoring; concept: 3PC).

Services:
- **Correlation Engine**: groups related events into candidate incidents
- **Incident Management Service**: creates and updates incidents
- **Event Store**: keeps raw events and the history of correlations

The group repo is https://github.com/Cloud-Lab-Assignment/Cloud-Security-Ingester. These services go there through a pull request under `services/<name>/`.

## Run it locally

Needs Docker with the compose plugin, Node 22, `bash`, `curl` and `jq`.

```sh
npm ci
docker compose up -d --build      # wait until `docker compose ps` shows every service healthy
npm test                          # unit tests
npm run test:integration          # end-to-end through all three services
scripts/demo-3pc.sh               # the 3PC demo (normal commit, a "no" vote, a coordinator crash)
```

Full steps, ports and settings: [docs/handoff.md, "Local run (fresh clone)"](docs/handoff.md#local-run-fresh-clone). The 3PC demo and its recorded output: [docs/handoff.md, "3PC demo"](docs/handoff.md#3pc-demo) and [scripts/demo-3pc.sh](scripts/demo-3pc.sh).

## Start here

- [docs/project-brief.md](docs/project-brief.md): the spec (assignment, who owns what, upstream Kafka contracts).
- [docs/handoff.md](docs/handoff.md): ports, env vars, topics, AWS resources, porting to the group repo, the 3PC demo.
- [docs/design/core-services.md](docs/design/core-services.md): the design-doc section (architecture, storage, 3PC, deployment, known limits).

### History

- [Implementation plan](docs/superpowers/plans/2026-11-16-core-services.md): done. Its working rules still apply: no installs beyond the approved list, no `terraform plan`/`apply`, no actions on the group repo, push only when asked.
- [Review fixes, round 1](docs/review-fixes-2026-11-16.md): done (P2-2 left as a known limit).
- [Review fixes, round 2](docs/review-fixes-round2-2026-11-16.md): done.
- [Review fixes, round 3](docs/review-fixes-round3-2026-11-16.md): done. This was the last code round; the code is frozen for the demo, the viva and the group-repo PR.

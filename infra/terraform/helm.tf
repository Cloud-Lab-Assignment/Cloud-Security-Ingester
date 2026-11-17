# Cluster add-ons installed by Terraform. Everything else on the cluster
# (the 6 app Deployments, HPAs, Services, Ingress) is delivered by ArgoCD from Git -
# no `kubectl apply` for services.

# ---------------------------------------------------------------- ArgoCD
resource "helm_release" "argocd" {
  name             = "argocd"
  namespace        = "argocd"
  create_namespace = true
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "7.3.11"

  set {
    name  = "server.service.type"
    value = "LoadBalancer"
  }

  # Run the UI over plain HTTP behind the LB (demo setup).
  set {
    name  = "configs.params.server\\.insecure"
    value = "true"
  }

  # Known admin password so no kubectl is needed to log in (username: admin).
  set_sensitive {
    name  = "configs.secret.argocdServerAdminPassword"
    value = bcrypt(var.argocd_admin_password)
  }

  lifecycle {
    # bcrypt() yields a new salt on every plan; don't re-roll the release for that.
    ignore_changes = [set_sensitive]
  }

  depends_on = [module.eks]
}

# LoadBalancer hostnames, surfaced as Terraform outputs.
data "kubernetes_service" "argocd" {
  metadata {
    name      = "argocd-server"
    namespace = "argocd"
  }
  depends_on = [helm_release.argocd]
}

data "kubernetes_service" "grafana" {
  metadata {
    name      = "monitoring-grafana"
    namespace = "monitoring"
  }
  depends_on = [helm_release.kube_prometheus_stack]
}

# Root "app-of-apps": ArgoCD tracks gitops/argocd in this repo, which in turn defines
# one Application per microservice (gitops/services/*) plus ingress/HPAs.
resource "helm_release" "argocd_root" {
  name       = "argocd-root"
  namespace  = "argocd"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argocd-apps"
  version    = "2.0.0"

  values = [yamlencode({
    applications = {
      root = {
        namespace = "argocd"
        project   = "default"
        source = {
          repoURL        = var.gitops_repo_url
          targetRevision = var.gitops_revision
          path           = var.gitops_path
          directory      = { recurse = true }
        }
        destination = {
          server    = "https://kubernetes.default.svc"
          namespace = "argocd"
        }
        syncPolicy = {
          automated   = { prune = true, selfHeal = true }
          syncOptions = ["CreateNamespace=true"]
        }
      }
    }
  })]

  depends_on = [helm_release.argocd]
}

# ---------------------------------------------------------------- Prometheus + Grafana
resource "helm_release" "kube_prometheus_stack" {
  name             = "monitoring"
  namespace        = "monitoring"
  create_namespace = true
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = "61.3.2"
  timeout          = 900

  values = [yamlencode({
    prometheus = {
      prometheusSpec = {
        # Scrape every ServiceMonitor/PodMonitor in any namespace (our apps expose /metrics).
        serviceMonitorSelectorNilUsesHelmValues = false
        podMonitorSelectorNilUsesHelmValues     = false
        retention                               = "3d"
      }
    }
    grafana = {
      adminPassword = var.grafana_admin_password
      service       = { type = "LoadBalancer" }
      # Sidecar auto-loads dashboards from ConfigMaps labelled grafana_dashboard=1
      # (ship the RPS / error-rate / latency dashboard from gitops/).
      sidecar = {
        dashboards = { enabled = true, searchNamespace = "ALL" }
      }
      additionalDataSources = [{
        name      = "Loki"
        uid       = "loki"
        type      = "loki"
        url       = "http://loki.logging.svc.cluster.local:3100"
        access    = "proxy"
        isDefault = false
      }]
    }
  })]

  depends_on = [module.eks]
}

# ---------------------------------------------------------------- Loki + Promtail (central logging)
resource "helm_release" "loki" {
  name             = "loki"
  namespace        = "logging"
  create_namespace = true
  repository       = "https://grafana.github.io/helm-charts"
  chart            = "loki-stack"
  version          = "2.10.2"

  values = [yamlencode({
    loki = {
      persistence = { enabled = true, size = "10Gi" }
    }
    promtail = { enabled = true } # DaemonSet: ships logs of every pod on every node
  })]

  depends_on = [module.eks]
}

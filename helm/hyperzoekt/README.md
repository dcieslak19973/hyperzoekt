# hyperzoekt

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: latest](https://img.shields.io/badge/AppVersion-latest-informational?style=flat-square)

A Helm chart for deploying HyperZoekt distributed code search

**Homepage:** <https://github.com/dcieslak19973/hyperzoekt>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| HyperZoekt Team | <info@hyperzoekt.dev> |  |

## Source Code

* <https://github.com/dcieslak19973/hyperzoekt>

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| admin.affinity | object | `{}` |  |
| admin.annotations | object | `{}` |  |
| admin.enabled | bool | `true` |  |
| admin.env[0].name | string | `"ZOEKT_ADMIN_USERNAME"` |  |
| admin.env[0].value | string | `"admin"` |  |
| admin.env[1].name | string | `"ZOEKT_ADMIN_PASSWORD"` |  |
| admin.env[1].value | string | `"password"` |  |
| admin.exposure.ingress.annotations | object | `{}` |  |
| admin.exposure.ingress.className | string | `""` |  |
| admin.exposure.ingress.enabled | bool | `false` |  |
| admin.exposure.ingress.host | string | `"admin.hyperzoekt.local"` |  |
| admin.exposure.ingress.path | string | `"/"` |  |
| admin.exposure.ingress.pathType | string | `"Prefix"` |  |
| admin.exposure.ingress.tls.enabled | bool | `false` |  |
| admin.exposure.ingress.tls.secretName | string | `""` |  |
| admin.exposure.serviceType | string | `"ClusterIP"` |  |
| admin.exposure.type | string | `"service"` |  |
| admin.extraEnvVars | list | `[]` |  |
| admin.image.pullPolicy | string | `"Always"` |  |
| admin.image.registry | string | `"docker.io"` |  |
| admin.image.repository | string | `"hyperzoekt"` |  |
| admin.image.tag | string | `"latest"` |  |
| admin.nodeSelector | object | `{}` |  |
| admin.replicaCount | int | `1` |  |
| admin.resources.limits.cpu | string | `"500m"` |  |
| admin.resources.limits.memory | string | `"512Mi"` |  |
| admin.resources.requests.cpu | string | `"100m"` |  |
| admin.resources.requests.memory | string | `"256Mi"` |  |
| admin.service.port | int | `7878` |  |
| admin.service.type | string | `"ClusterIP"` |  |
| admin.tolerations | list | `[]` |  |
| affinity | object | `{}` |  |
| global.imagePullSecrets | list | `[]` |  |
| global.imageRegistry | string | `""` |  |
| global.storageClass | string | `""` |  |
| indexer.affinity | object | `{}` |  |
| indexer.annotations | object | `{}` |  |
| indexer.autoscaling.enabled | bool | `true` |  |
| indexer.autoscaling.maxReplicas | int | `10` |  |
| indexer.autoscaling.minReplicas | int | `1` |  |
| indexer.autoscaling.targetCPUUtilizationPercentage | int | `80` |  |
| indexer.autoscaling.targetMemoryUtilizationPercentage | int | `80` |  |
| indexer.enabled | bool | `true` |  |
| indexer.env[0].name | string | `"ZOEKTD_BIND_ADDR"` |  |
| indexer.env[0].value | string | `"0.0.0.0:7676"` |  |
| indexer.env[1].name | string | `"ZOEKTD_ENABLE_REINDEX"` |  |
| indexer.env[1].value | string | `"true"` |  |
| indexer.env[2].name | string | `"ZOEKTD_INDEX_ONCE"` |  |
| indexer.env[2].value | string | `"false"` |  |
| indexer.extraEnvVars | list | `[]` |  |
| indexer.hostRepoPath | string | `""` |  |
| indexer.image.pullPolicy | string | `"Always"` |  |
| indexer.image.registry | string | `"docker.io"` |  |
| indexer.image.repository | string | `"hyperzoekt"` |  |
| indexer.image.tag | string | `"latest"` |  |
| indexer.nodeSelector | object | `{}` |  |
| indexer.replicaCount | int | `1` |  |
| indexer.resources.limits.cpu | string | `"1000m"` |  |
| indexer.resources.limits.memory | string | `"1Gi"` |  |
| indexer.resources.requests.cpu | string | `"500m"` |  |
| indexer.resources.requests.memory | string | `"512Mi"` |  |
| indexer.tolerations | list | `[]` |  |
| ingress.annotations | object | `{}` |  |
| ingress.className | string | `""` |  |
| ingress.enabled | bool | `false` |  |
| ingress.hosts[0].host | string | `"hyperzoekt.local"` |  |
| ingress.hosts[0].paths[0].path | string | `"/admin"` |  |
| ingress.hosts[0].paths[0].pathType | string | `"Prefix"` |  |
| ingress.hosts[0].paths[0].service.name | string | `"hyperzoekt-admin"` |  |
| ingress.hosts[0].paths[0].service.port.number | int | `7878` |  |
| ingress.hosts[0].paths[1].path | string | `"/search"` |  |
| ingress.hosts[0].paths[1].pathType | string | `"Prefix"` |  |
| ingress.hosts[0].paths[1].service.name | string | `"hyperzoekt-search"` |  |
| ingress.hosts[0].paths[1].service.port.number | int | `8080` |  |
| ingress.hosts[0].paths[2].path | string | `"/mcp"` |  |
| ingress.hosts[0].paths[2].pathType | string | `"Prefix"` |  |
| ingress.hosts[0].paths[2].service.name | string | `"hyperzoekt-mcp"` |  |
| ingress.hosts[0].paths[2].service.port.number | int | `7979` |  |
| ingress.tls | list | `[]` |  |
| mcp.affinity | object | `{}` |  |
| mcp.annotations | object | `{}` |  |
| mcp.enabled | bool | `true` |  |
| mcp.env[0].name | string | `"ZOEKTD_BIND_ADDR"` |  |
| mcp.env[0].value | string | `"0.0.0.0:7979"` |  |
| mcp.exposure.ingress.annotations | object | `{}` |  |
| mcp.exposure.ingress.className | string | `""` |  |
| mcp.exposure.ingress.enabled | bool | `false` |  |
| mcp.exposure.ingress.host | string | `"mcp.hyperzoekt.local"` |  |
| mcp.exposure.ingress.path | string | `"/"` |  |
| mcp.exposure.ingress.pathType | string | `"Prefix"` |  |
| mcp.exposure.ingress.tls.enabled | bool | `false` |  |
| mcp.exposure.ingress.tls.secretName | string | `""` |  |
| mcp.exposure.serviceType | string | `"ClusterIP"` |  |
| mcp.exposure.type | string | `"service"` |  |
| mcp.extraEnvVars | list | `[]` |  |
| mcp.image.pullPolicy | string | `"Always"` |  |
| mcp.image.registry | string | `"docker.io"` |  |
| mcp.image.repository | string | `"hyperzoekt"` |  |
| mcp.image.tag | string | `"latest"` |  |
| mcp.nodeSelector | object | `{}` |  |
| mcp.replicaCount | int | `1` |  |
| mcp.resources.limits.cpu | string | `"500m"` |  |
| mcp.resources.limits.memory | string | `"512Mi"` |  |
| mcp.resources.requests.cpu | string | `"100m"` |  |
| mcp.resources.requests.memory | string | `"256Mi"` |  |
| mcp.service.port | int | `7979` |  |
| mcp.service.type | string | `"ClusterIP"` |  |
| mcp.tolerations | list | `[]` |  |
| nodeSelector | object | `{}` |  |
| podSecurityContext | object | `{}` |  |
| redis.annotations | object | `{}` |  |
| redis.enabled | bool | `true` |  |
| redis.external.host | string | `""` |  |
| redis.external.password | string | `""` |  |
| redis.external.port | int | `6379` |  |
| redis.image.pullPolicy | string | `"IfNotPresent"` |  |
| redis.image.registry | string | `"docker.io"` |  |
| redis.image.repository | string | `"redis"` |  |
| redis.image.tag | string | `"alpine"` |  |
| redis.persistence.accessModes[0] | string | `"ReadWriteOnce"` |  |
| redis.persistence.enabled | bool | `false` |  |
| redis.persistence.size | string | `"8Gi"` |  |
| redis.resources.limits.cpu | string | `"500m"` |  |
| redis.resources.limits.memory | string | `"512Mi"` |  |
| redis.resources.requests.cpu | string | `"100m"` |  |
| redis.resources.requests.memory | string | `"128Mi"` |  |
| redis.service.port | int | `6379` |  |
| redis.service.type | string | `"ClusterIP"` |  |
| search.affinity | object | `{}` |  |
| search.annotations | object | `{}` |  |
| search.enabled | bool | `true` |  |
| search.env[0].name | string | `"ZOEKTD_BIND_ADDR"` |  |
| search.env[0].value | string | `"0.0.0.0:8080"` |  |
| search.exposure.ingress.annotations | object | `{}` |  |
| search.exposure.ingress.className | string | `""` |  |
| search.exposure.ingress.enabled | bool | `false` |  |
| search.exposure.ingress.host | string | `"search.hyperzoekt.local"` |  |
| search.exposure.ingress.path | string | `"/"` |  |
| search.exposure.ingress.pathType | string | `"Prefix"` |  |
| search.exposure.ingress.tls.enabled | bool | `false` |  |
| search.exposure.ingress.tls.secretName | string | `""` |  |
| search.exposure.serviceType | string | `"ClusterIP"` |  |
| search.exposure.type | string | `"service"` |  |
| search.extraEnvVars | list | `[]` |  |
| search.image.pullPolicy | string | `"Always"` |  |
| search.image.registry | string | `"docker.io"` |  |
| search.image.repository | string | `"hyperzoekt"` |  |
| search.image.tag | string | `"latest"` |  |
| search.nodeSelector | object | `{}` |  |
| search.replicaCount | int | `1` |  |
| search.resources.limits.cpu | string | `"500m"` |  |
| search.resources.limits.memory | string | `"512Mi"` |  |
| search.resources.requests.cpu | string | `"100m"` |  |
| search.resources.requests.memory | string | `"256Mi"` |  |
| search.service.port | int | `8080` |  |
| search.service.type | string | `"ClusterIP"` |  |
| search.tolerations | list | `[]` |  |
| securityContext | object | `{}` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| surrealdb.annotations | object | `{}` |  |
| surrealdb.enabled | bool | `true` |  |
| surrealdb.external.database | string | `"repos"` |  |
| surrealdb.external.namespace | string | `"zoekt"` |  |
| surrealdb.external.password | string | `"root"` |  |
| surrealdb.external.url | string | `""` |  |
| surrealdb.external.username | string | `"root"` |  |
| surrealdb.image.pullPolicy | string | `"IfNotPresent"` |  |
| surrealdb.image.registry | string | `"docker.io"` |  |
| surrealdb.image.repository | string | `"surrealdb/surrealdb"` |  |
| surrealdb.image.tag | string | `"v2.3.1"` |  |
| surrealdb.persistence.accessModes[0] | string | `"ReadWriteOnce"` |  |
| surrealdb.persistence.enabled | bool | `false` |  |
| surrealdb.persistence.size | string | `"8Gi"` |  |
| surrealdb.resources.limits.cpu | string | `"500m"` |  |
| surrealdb.resources.limits.memory | string | `"512Mi"` |  |
| surrealdb.resources.requests.cpu | string | `"100m"` |  |
| surrealdb.resources.requests.memory | string | `"128Mi"` |  |
| surrealdb.service.port | int | `8000` |  |
| surrealdb.service.type | string | `"ClusterIP"` |  |
| tolerations | list | `[]` |  |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)

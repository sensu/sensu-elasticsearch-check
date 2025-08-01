# Sensu Elasticsearch Check
## Overview
check-es-cluster-health is a Sensu check plugin built using the [Sensu Plugin SDK][2] to monitor Elasticsearch cluster health, node status, and file descriptor usage. This plugin provides comprehensive monitoring capabilities for Elasticsearch clusters through Sensu's monitoring pipeline.



## Table of Contents
- [Configuration](#configuration)
  - [Command-line Arguments](#Command-line-arguments)
  - [Environment Variables](#environment-variables)
- [Sensu Check Definition](#sensu-check-definition)
- [Examples](#examples)
- [Output Examples](#output-examples)
- [Exit Codes](#exit-codes)

## Configuration

### Command-line-arguments

| Argument               | Shorthand | Default   | Description                                       |
|------------------------|-----------|-----------|-------------------------------------------------|
| --host                 | -H        | localhost | Elasticsearch host                              |
| --port                 | -p        | 9200      | Elasticsearch port                              |
| --scheme               | -s        | http      | Elasticsearch connection scheme (http/https)    |
| --user                 | -u        |           | Elasticsearch connection user                    |
| --password             | -P        |           | Elasticsearch connection password                |
| --timeout              | -t        | 30        | Elasticsearch query timeout in seconds           |
| --status-timeout       | -T        | 0         | Time to wait for cluster status to be green (s) |
| --level                | -l        |           | Level of detail (cluster, indices, shards)       |
| --local                | -L        | false     | Return local information only                    |
| --index                | -i        |           | Comma-separated list of indices to check         |
| --alert-status         | -a        |           | Only alert for specific status (RED/YELLOW/GREEN)|
| --master-only          | -m        | false     | Only run on master node                           |
| --check-fd             | -f        | false     | Check file descriptor usage                       |
| --fd-critical          | -c        | 90        | Critical percentage of FD usage                   |
| --fd-warning           | -w        | 80        | Warning percentage of FD usage                    |
| --cert-file            | -C        |           | Cert file to use                                  |
| --insecure-skip-verify | -k        | false     | Skip SSL certificate verification                 |
| --check-nodes          | -n        |           | Check node status (local/all)                     |

### Environment Variables

All parameters can also be set via environment variables prefixed with `CHECK_ES_`:

| Argument               | Environment Variable           |
|------------------------|-------------------------------|
| --host                 | CHECK_ES_HOST                 |
| --port                 | CHECK_ES_PORT                 |
| --scheme               | CHECK_ES_SCHEME               |
| --user                 | CHECK_ES_USER                 |
| --password             | CHECK_ES_PASSWORD             |
| --timeout              | CHECK_ES_TIMEOUT              |
| --status-timeout       | CHECK_ES_STATUS_TIMEOUT       |
| --level                | CHECK_ES_LEVEL                |
| --local                | CHECK_ES_LOCAL                |
| --index                | CHECK_ES_INDEX                |
| --alert-status         | CHECK_ES_ALERT_STATUS         |
| --master-only          | CHECK_ES_MASTER_ONLY          |
| --check-fd             | CHECK_ES_FD                   |
| --fd-critical          | CHECK_ES_FD_CRITICAL          |
| --fd-warning           | CHECK_ES_FD_WARNING           |
| --cert-file            | CHECK_ES_CERT_FILE            |
| --insecure-skip-verify | CHECK_ES_INSECURE_SKIP_VERIFY |
| --check-nodes          | CHECK_ES_NODES                |


## Sensu Check Definition
```
---
type: CheckConfig
api_version: core/v2
metadata:
  name: check-es-cluster-health
  namespace: default
spec:
  command: >-
    check-es-cluster-health
    --host elasticsearch.example.com
    --port 9200
    --scheme https
    --user admin
    --password secret
    --check-fd
    --fd-warning 80
    --fd-critical 90
  subscriptions:
    - elasticsearch
  interval: 60
  publish: true
  runtime_assets:
    - sensu-check-es-cluster-health
  timeout: 30
  handlers:
    - email
    - slack
```


## Examples

### Basic cluster health check

`./check-es-cluster-health --host elasticsearch.example.com --port 9200
`

### Cluster health with authentication

`./check-es-cluster-health --host elasticsearch.example.com --port 9200 --user admin --password secret --scheme https
`

### Check file descriptors with custom thresholds

`./check-es-cluster-health --check-fd --fd-warning 70 --fd-critical 90
`

### Only alert on RED status

`./check-es-cluster-health --alert-status RED
`

### Check all nodes status

`./check-es-cluster-health --check-nodes all
`

## Output Examples

### Healthy cluster
`- OK: Cluster status is green`
### Warning status
`- WARNING: Cluster status is yellow`
### Critical status
`- CRITICAL: Cluster status is red`
### File descriptor warning
`- WARNING: fd usage 85.0% exceeds 80% (8500/10000)`

## Exit Codes

| Code | Status   | Description                                         |
|-------|----------|---------------------------------------------------|
| 0     | OK       | Cluster is healthy                                 |
| 1     | Warning  | Cluster is in warning state (yellow)               |
| 2     | Critical | Cluster is in critical state (red) or other issue  |
| 3     | Unknown  | Unable to determine cluster status                  |

## Usage examples
```
Sensu check for Elasticsearch cluster health and file descriptors

Usage:
check-es-cluster-health [flags]
check-es-cluster-health [command]

Available Commands:
completion  Generate the autocompletion script for the specified shell
help        Help about any command
version     Print the version number of this plugin

Flags:
-a, --alert-status string    Only alert when status matches given RED/YELLOW/GREEN or if blank all statuses
-C, --cert-file string       Cert file to use
-f, --check-fd               Check file descriptor usage
-n, --check-nodes string     Check node status
-c, --fd-critical int        Critical percentage of FD usage (default 90)
-w, --fd-warning int         Warning percentage of FD usage (default 80)
-h, --help                   help for check-es-cluster-health
-H, --host string            Elasticsearch host (default "localhost")
-i, --index string           Comma separated list of indices to check health for
-k, --insecure-skip-verify   Skip SSL certificate verification
-l, --level string           Level of detail to check returned information (cluster, indices, shards)
-L, --local                  Return local information, do not retrieve the state from master node
-m, --master-only            Use master Elasticsearch server only
-P, --password string        Elasticsearch connection password
-p, --port int               Elasticsearch port (default 9200)
-s, --scheme string          Elasticsearch connection scheme (http/https) (default "http")
-T, --status-timeout int     Time to wait for cluster status to be green in seconds
-t, --timeout int            Elasticsearch query timeout in seconds (default 30)
-u, --user string            Elasticsearch connection user

Use "check-es-cluster-health [command] --help" for more information about a command.

```

## Installation from source

The preferred way of installing and deploying this plugin is to use it as an Asset. If you would
like to compile and install the plugin from source or contribute to it, download the latest version
or create an executable script from this source.

From the local path of the `sensu-elasticsearch-check` repository:

```
go build
```

## Additional notes

## Contributing

For more information about contributing to this plugin, see [Contributing][1].

[1]: https://github.com/sensu/sensu-go/blob/master/CONTRIBUTING.md
[2]: https://github.com/sensu/sensu-plugin-sdk
[3]: https://github.com/sensu-plugins/community/blob/master/PLUGIN_STYLEGUIDE.md
[4]: https://docs.sensu.io/sensu-go/latest/reference/checks/
[5]: https://github.com/sensu/check-plugin-template/blob/master/main.go
[6]: https://bonsai.sensu.io/
[7]: https://github.com/sensu/sensu-plugin-tool

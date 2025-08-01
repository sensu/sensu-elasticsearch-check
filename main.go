package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"github.com/sensu/sensu-go/types"
	"github.com/sensu/sensu-plugin-sdk/sensu"
)

// Config represents the check plugin config
type Config struct {
	sensu.PluginConfig
	Host               string
	Port               int
	Scheme             string
	User               string
	Password           string
	Timeout            int
	StatusTimeout      int
	Level              string
	Local              bool
	Index              string
	AlertStatus        string
	MasterOnly         bool
	CheckFD            bool
	FDCritical         int
	FDWarning          int
	CertFile           string
	InsecureSkipVerify bool
	CheckNodes         string
}

// ClusterHealthResponse represents the Elasticsearch cluster health response
type ClusterHealthResponse struct {
	Status                      string  `json:"status"`
	ClusterName                 string  `json:"cluster_name"`
	TimedOut                    bool    `json:"timed_out"`
	NumberOfNodes               int     `json:"number_of_nodes"`
	NumberOfDataNodes           int     `json:"number_of_data_nodes"`
	ActivePrimaryShards         int     `json:"active_primary_shards"`
	ActiveShards                int     `json:"active_shards"`
	RelocatingShards            int     `json:"relocating_shards"`
	InitializingShards          int     `json:"initializing_shards"`
	UnassignedShards            int     `json:"unassigned_shards"`
	DelayedUnassignedShards     int     `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks        int     `json:"number_of_pending_tasks"`
	NumberOfInFlightFetch       int     `json:"number_of_in_flight_fetch"`
	TaskMaxWaitingInQueueMs     int     `json:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercentAsNumber float64 `json:"active_shards_percent_as_number"`
}

// NodesStatsResponse represents the Elasticsearch nodes stats response
type NodesStatsResponse struct {
	Nodes map[string]struct {
		Process struct {
			MaxFileDescriptors  int `json:"max_file_descriptors"`
			OpenFileDescriptors int `json:"open_file_descriptors"`
		} `json:"process"`
	} `json:"nodes"`
	ClusterNodes struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_nodes"`
}

// ClusterStateResponse represents the Elasticsearch cluster state response
type ClusterStateResponse struct {
	MasterNode string `json:"master_node"`
}

// NodeInfoResponse represents the Elasticsearch node info response
type NodeInfoResponse struct {
	Nodes map[string]struct {
		Name string `json:"name"`
	} `json:"nodes"`
}

var (
	plugin = Config{
		PluginConfig: sensu.PluginConfig{
			Name:     "check-es-cluster-health",
			Short:    "Sensu check for Elasticsearch cluster health and file descriptors",
			Keyspace: "sensu.io/plugins/check-es-cluster-health/config",
		},
	}

	options = []sensu.ConfigOption{
		&sensu.PluginConfigOption[string]{
			Path:      "host",
			Env:       "CHECK_ES_HOST",
			Argument:  "host",
			Shorthand: "H",
			Default:   "localhost",
			Usage:     "Elasticsearch host",
			Value:     &plugin.Host,
		},
		&sensu.PluginConfigOption[int]{
			Path:      "port",
			Env:       "CHECK_ES_PORT",
			Argument:  "port",
			Shorthand: "p",
			Default:   9200,
			Usage:     "Elasticsearch port",
			Value:     &plugin.Port,
		},
		&sensu.PluginConfigOption[string]{
			Path:      "scheme",
			Env:       "CHECK_ES_SCHEME",
			Argument:  "scheme",
			Shorthand: "s",
			Default:   "http",
			Usage:     "Elasticsearch connection scheme (http/https)",
			Value:     &plugin.Scheme,
		},
		&sensu.PluginConfigOption[string]{
			Path:      "user",
			Env:       "CHECK_ES_USER",
			Argument:  "user",
			Shorthand: "u",
			Default:   "",
			Usage:     "Elasticsearch connection user",
			Value:     &plugin.User,
		},
		&sensu.PluginConfigOption[string]{
			Path:      "password",
			Env:       "CHECK_ES_PASSWORD",
			Argument:  "password",
			Shorthand: "P",
			Default:   "",
			Usage:     "Elasticsearch connection password",
			Value:     &plugin.Password,
		},
		&sensu.PluginConfigOption[int]{
			Path:      "timeout",
			Env:       "CHECK_ES_TIMEOUT",
			Argument:  "timeout",
			Shorthand: "t",
			Default:   30,
			Usage:     "Elasticsearch query timeout in seconds",
			Value:     &plugin.Timeout,
		},
		&sensu.PluginConfigOption[int]{
			Path:      "status-timeout",
			Env:       "CHECK_ES_STATUS_TIMEOUT",
			Argument:  "status-timeout",
			Shorthand: "T",
			Default:   0,
			Usage:     "Time to wait for cluster status to be green in seconds",
			Value:     &plugin.StatusTimeout,
		},
		&sensu.PluginConfigOption[string]{
			Path:      "level",
			Env:       "CHECK_ES_LEVEL",
			Argument:  "level",
			Shorthand: "l",
			Default:   "",
			Usage:     "Level of detail to check returned information (cluster, indices, shards)",
			Value:     &plugin.Level,
		},
		&sensu.PluginConfigOption[bool]{
			Path:      "local",
			Env:       "CHECK_ES_LOCAL",
			Argument:  "local",
			Shorthand: "L",
			Default:   false,
			Usage:     "Return local information, do not retrieve the state from master node",
			Value:     &plugin.Local,
		},
		&sensu.PluginConfigOption[string]{
			Path:      "index",
			Env:       "CHECK_ES_INDEX",
			Argument:  "index",
			Shorthand: "i",
			Default:   "",
			Usage:     "Comma separated list of indices to check health for",
			Value:     &plugin.Index,
		},
		&sensu.PluginConfigOption[string]{
			Path:      "alert-status",
			Env:       "CHECK_ES_ALERT_STATUS",
			Argument:  "alert-status",
			Shorthand: "a",
			Default:   "",
			Usage:     "Only alert when status matches given RED/YELLOW/GREEN or if blank all statuses",
			Value:     &plugin.AlertStatus,
		},
		&sensu.PluginConfigOption[bool]{
			Path:      "master-only",
			Env:       "CHECK_ES_MASTER_ONLY",
			Argument:  "master-only",
			Shorthand: "m",
			Default:   false,
			Usage:     "Use master Elasticsearch server only",
			Value:     &plugin.MasterOnly,
		},
		&sensu.PluginConfigOption[bool]{
			Path:      "check-fd",
			Env:       "CHECK_ES_FD",
			Argument:  "check-fd",
			Shorthand: "f",
			Default:   false,
			Usage:     "Check file descriptor usage",
			Value:     &plugin.CheckFD,
		},
		&sensu.PluginConfigOption[int]{
			Path:      "fd-critical",
			Env:       "CHECK_ES_FD_CRITICAL",
			Argument:  "fd-critical",
			Shorthand: "c",
			Default:   90,
			Usage:     "Critical percentage of FD usage",
			Value:     &plugin.FDCritical,
		},
		&sensu.PluginConfigOption[int]{
			Path:      "fd-warning",
			Env:       "CHECK_ES_FD_WARNING",
			Argument:  "fd-warning",
			Shorthand: "w",
			Default:   80,
			Usage:     "Warning percentage of FD usage",
			Value:     &plugin.FDWarning,
		},
		&sensu.PluginConfigOption[string]{
			Path:      "cert-file",
			Env:       "CHECK_ES_CERT_FILE",
			Argument:  "cert-file",
			Shorthand: "C",
			Default:   "",
			Usage:     "Cert file to use",
			Value:     &plugin.CertFile,
		},
		&sensu.PluginConfigOption[bool]{
			Path:      "insecure-skip-verify",
			Env:       "CHECK_ES_INSECURE_SKIP_VERIFY",
			Argument:  "insecure-skip-verify",
			Shorthand: "k",
			Default:   false,
			Usage:     "Skip SSL certificate verification",
			Value:     &plugin.InsecureSkipVerify,
		},
		&sensu.PluginConfigOption[string]{
			Path:      "check-nodes",
			Env:       "CHECK_ES_NODES",
			Argument:  "check-nodes",
			Shorthand: "n",
			Default:   "",
			Usage:     "Check node status",
			Value:     &plugin.CheckNodes,
		},
	}
)

func main() {
	check := sensu.NewCheck(&plugin.PluginConfig, options, checkArgs, executeCheck, false)
	check.Execute()
}

func checkArgs(_ *types.Event) (int, error) {
	// Validate alert status if provided
	if plugin.AlertStatus != "" {
		validStatuses := []string{"RED", "YELLOW", "GREEN"}
		valid := false
		for _, status := range validStatuses {
			if strings.ToUpper(plugin.AlertStatus) == status {
				valid = true
				break
			}
		}
		if !valid {
			return sensu.CheckStateCritical, fmt.Errorf("invalid alert-status: %s, must be one of RED, YELLOW, GREEN", plugin.AlertStatus)
		}
	}

	// Validate FD thresholds if checking FDs
	if plugin.CheckFD {
		if plugin.FDWarning <= 0 || plugin.FDWarning >= 100 {
			return sensu.CheckStateCritical, fmt.Errorf("fd-warning must be between 1 and 99")
		}
		if plugin.FDCritical <= 0 || plugin.FDCritical >= 100 {
			return sensu.CheckStateCritical, fmt.Errorf("fd-critical must be between 1 and 99")
		}
		if plugin.FDWarning >= plugin.FDCritical {
			return sensu.CheckStateCritical, fmt.Errorf("fd-warning must be less than fd-critical")
		}
	}

	// Set default scheme to https if authentication is provided
	if plugin.Scheme == "http" && (plugin.User != "" || plugin.CertFile != "") {
		plugin.Scheme = "https"
	}
	// Validate node check options
	if plugin.CheckNodes != "" && plugin.CheckNodes != "local" && plugin.CheckNodes != "all" {
		return sensu.CheckStateCritical,
			fmt.Errorf("invalid check-nodes value: %s, must be 'local' or 'all'", plugin.CheckNodes)
	}

	return sensu.CheckStateOK, nil
}

func executeCheck(_ *types.Event) (int, error) {
	// Create Elasticsearch client
	es, err := createESClient()
	if err != nil {
		return sensu.CheckStateCritical, fmt.Errorf("failed to create Elasticsearch client: %v", err)
	}

	// Check if we should only run on master
	if plugin.MasterOnly {
		isMaster, err := isMasterNode(es)
		if err != nil {
			return sensu.CheckStateCritical, fmt.Errorf("failed to check master status: %v", err)
		}
		if !isMaster {
			fmt.Println("OK: Not the master node")
			return sensu.CheckStateOK, nil
		}
	}

	finalStatus := sensu.CheckStateOK

	// Check cluster health if not specifically checking nodes only
	if plugin.CheckNodes == "" {
		status, err := checkClusterHealth(es)
		if err != nil {
			return status, err
		}
		if status > finalStatus {
			finalStatus = status
		}
	}

	// Check file descriptors if enabled
	if plugin.CheckFD {
		fdStatus, err := checkFileDescriptors(es)
		if err != nil {
			return fdStatus, err
		}
		if fdStatus > finalStatus {
			finalStatus = fdStatus
		}
	}

	// Check node status if enabled
	if plugin.CheckNodes != "" {
		nodeStatus, err := checkNodeStatus(es)
		if err != nil {
			return nodeStatus, err
		}
		if nodeStatus > finalStatus {
			finalStatus = nodeStatus
		}
	}

	return finalStatus, nil
}

func createESClient() (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			fmt.Sprintf("%s://%s:%d", plugin.Scheme, plugin.Host, plugin.Port),
		},
		Username: plugin.User,
		Password: plugin.Password,
	}

	// Configure TLS if needed
	if plugin.Scheme == "https" {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: plugin.InsecureSkipVerify,
		}

		if plugin.CertFile != "" {
			certPool, err := x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("failed to load system cert pool: %v", err)
			}
			cert, err := os.ReadFile(plugin.CertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read cert file: %v", err)
			}
			if ok := certPool.AppendCertsFromPEM(cert); !ok {
				return nil, fmt.Errorf("failed to append cert")
			}
			tlsConfig.RootCAs = certPool
		}

		transport := &http.Transport{
			TLSClientConfig: tlsConfig,
		}
		cfg.Transport = transport
	}

	return elasticsearch.NewClient(cfg)
}

func checkClusterHealth(es *elasticsearch.Client) (int, error) {
	req := esapi.ClusterHealthRequest{
		Level:         plugin.Level,
		Local:         &plugin.Local,
		Index:         strings.Split(plugin.Index, ","),
		WaitForStatus: "green",
		Timeout:       time.Duration(plugin.StatusTimeout) * time.Second,
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return sensu.CheckStateCritical, fmt.Errorf("cluster health API error: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("failed to close response body: %v", err)
		}
	}(res.Body)

	if res.IsError() {
		return sensu.CheckStateCritical, fmt.Errorf("cluster health API error: %s", res.String())
	}

	var health ClusterHealthResponse
	if err := json.NewDecoder(res.Body).Decode(&health); err != nil {
		return sensu.CheckStateCritical, fmt.Errorf("failed to decode response: %v", err)
	}

	switch strings.ToLower(health.Status) {
	case "green":
		fmt.Printf("OK: Cluster status is %s\n", health.Status)
		return sensu.CheckStateOK, nil
	case "yellow":
		if plugin.AlertStatus == "" || strings.ToUpper(plugin.AlertStatus) == "YELLOW" {
			fmt.Printf("WARNING: Cluster status is %s\n", health.Status)
			return sensu.CheckStateWarning, nil
		}
		fmt.Printf("OK: Not alerting on yellow status\n")
		return sensu.CheckStateOK, nil
	case "red":
		if plugin.AlertStatus == "" || strings.ToUpper(plugin.AlertStatus) == "RED" {
			fmt.Printf("CRITICAL: Cluster status is %s\n", health.Status)
			return sensu.CheckStateCritical, nil
		}
		fmt.Printf("OK: Not alerting on red status\n")
		return sensu.CheckStateOK, nil
	default:
		return sensu.CheckStateUnknown, fmt.Errorf("unknown cluster health status: %s", health.Status)
	}
}

func isMasterNode(es *elasticsearch.Client) (bool, error) {
	// Get cluster state to find master node
	req := esapi.ClusterStateRequest{
		Metric: []string{"master_node"},
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return false, fmt.Errorf("failed to get cluster state: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("failed to close response body: %v", err)
		}
	}(res.Body)

	if res.IsError() {
		return false, fmt.Errorf("cluster state API error: %s", res.String())
	}

	var clusterState ClusterStateResponse
	if err := json.NewDecoder(res.Body).Decode(&clusterState); err != nil {
		return false, fmt.Errorf("failed to decode cluster state: %v", err)
	}

	// Get local node info
	reqInfo := esapi.NodesInfoRequest{
		NodeID: []string{"_local"},
	}
	resInfo, err := reqInfo.Do(context.Background(), es)
	if err != nil {
		return false, fmt.Errorf("failed to get local node info: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("failed to close response body: %v", err)
		}
	}(resInfo.Body)

	if resInfo.IsError() {
		return false, fmt.Errorf("nodes info API error: %s", resInfo.String())
	}

	var nodeInfo NodeInfoResponse
	if err := json.NewDecoder(resInfo.Body).Decode(&nodeInfo); err != nil {
		return false, fmt.Errorf("failed to decode node info: %v", err)
	}

	// Compare master node ID with local node ID
	for nodeID := range nodeInfo.Nodes {
		return nodeID == clusterState.MasterNode, nil
	}

	return false, fmt.Errorf("no local node found")
}

func checkNodeStatus(es *elasticsearch.Client) (int, error) {
	res, err := es.Nodes.Stats(
		es.Nodes.Stats.WithMetric("process"),
		es.Nodes.Stats.WithContext(context.Background()),
	)
	if err != nil {
		return sensu.CheckStateCritical, fmt.Errorf("nodes stats API error: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("failed to close response body: %v", err)
		}
	}(res.Body)

	if res.IsError() {
		return sensu.CheckStateCritical, fmt.Errorf("nodes stats API error: %s", res.String())
	}

	var stats NodesStatsResponse
	if err := json.NewDecoder(res.Body).Decode(&stats); err != nil {
		return sensu.CheckStateCritical, fmt.Errorf("failed to decode node stats: %v", err)
	}

	switch plugin.CheckNodes {
	case "all":
		if stats.ClusterNodes.Total == stats.ClusterNodes.Successful {
			fmt.Printf("OK: All %d nodes are alive\n", stats.ClusterNodes.Total)
			return sensu.CheckStateOK, nil
		}
		fmt.Printf("CRITICAL: %d of %d nodes are alive (%d failed)\n",
			stats.ClusterNodes.Successful, stats.ClusterNodes.Total, stats.ClusterNodes.Failed)
		return sensu.CheckStateCritical, nil
	case "local":
		fallthrough
	default:
		if len(stats.Nodes) > 0 {
			fmt.Println("OK: Local node is alive")
			return sensu.CheckStateOK, nil
		}
		return sensu.CheckStateCritical, fmt.Errorf("no node stats found")
	}
}

func checkFileDescriptors(es *elasticsearch.Client) (int, error) {
	res, err := es.Nodes.Stats(
		es.Nodes.Stats.WithNodeID("_local"),
		es.Nodes.Stats.WithMetric("process"),
		es.Nodes.Stats.WithContext(context.Background()),
	)
	if err != nil {
		return sensu.CheckStateCritical, fmt.Errorf("nodes stats API error: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("failed to close response body: %v", err)
		}
	}(res.Body)

	if res.IsError() {
		return sensu.CheckStateCritical, fmt.Errorf("nodes stats API error: %s", res.String())
	}

	var stats NodesStatsResponse
	if err := json.NewDecoder(res.Body).Decode(&stats); err != nil {
		return sensu.CheckStateCritical, fmt.Errorf("failed to decode stats response: %v", err)
	}

	for _, node := range stats.Nodes {
		maxFD := node.Process.MaxFileDescriptors
		openFD := node.Process.OpenFileDescriptors
		if maxFD == 0 {
			return sensu.CheckStateUnknown, fmt.Errorf("failed to get max file descriptors")
		}

		usedPercent := (float64(openFD) / float64(maxFD)) * 100

		if usedPercent >= float64(plugin.FDCritical) {
			fmt.Printf("CRITICAL: fd usage %.1f%% exceeds %d%% (%d/%d)\n", usedPercent, plugin.FDCritical, openFD, maxFD)
			return sensu.CheckStateCritical, nil
		} else if usedPercent >= float64(plugin.FDWarning) {
			fmt.Printf("WARNING: fd usage %.1f%% exceeds %d%% (%d/%d)\n", usedPercent, plugin.FDWarning, openFD, maxFD)
			return sensu.CheckStateWarning, nil
		} else {
			fmt.Printf("OK: fd usage at %.1f%% (%d/%d)\n", usedPercent, openFD, maxFD)
			return sensu.CheckStateOK, nil
		}
	}

	return sensu.CheckStateUnknown, fmt.Errorf("no node stats found")
}

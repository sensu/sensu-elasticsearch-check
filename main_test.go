package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/sensu/sensu-plugin-sdk/sensu"
)

// Mock data structures
type MockClusterHealthResponse struct {
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

type MockNodesStatsResponse struct {
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

type MockClusterStateResponse struct {
	MasterNode string `json:"master_node"`
}

type MockNodeInfoResponse struct {
	Nodes map[string]struct {
		Name string `json:"name"`
	} `json:"nodes"`
}

// Mock Elasticsearch server
func createMockESServer(responses map[string]interface{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set Elasticsearch-like headers to fool the client detection
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Server", "Elasticsearch/8.0.0")

		path := r.URL.Path
		method := r.Method

		// Handle root endpoint for client detection
		if path == "/" && method == "GET" {
			response := map[string]interface{}{
				"name":         "mock-node",
				"cluster_name": "mock-cluster",
				"cluster_uuid": "mock-uuid",
				"version": map[string]interface{}{
					"number":         "8.0.0",
					"build_flavor":   "default",
					"build_type":     "docker",
					"build_hash":     "mock-hash",
					"build_date":     "2022-01-01T00:00:00.000Z",
					"build_snapshot": false,
					"lucene_version": "9.0.0",
				},
				"tagline": "You Know, for Search",
			}
			json.NewEncoder(w).Encode(response)
			return
		}

		// Simulate invalid JSON response if "invalid_json" flag is set for this endpoint
		if invalidEndpoints, ok := responses["invalid_json"].(map[string]bool); ok {
			if invalidEndpoints[path] {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte("{this is not valid JSON")) // deliberately malformed
				return
			}
		}

		// Handle different endpoints
		switch {
		case strings.Contains(path, "_cluster/health"):
			if health, ok := responses["cluster_health"]; ok {
				json.NewEncoder(w).Encode(health)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "health endpoint error"})
			}
		case strings.Contains(path, "_cluster/state"):
			if state, ok := responses["cluster_state"]; ok {
				json.NewEncoder(w).Encode(state)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "state endpoint error"})
			}
		case strings.Contains(path, "_nodes/stats") || strings.Contains(path, "_nodes/_local/stats"):
			if stats, ok := responses["nodes_stats"]; ok {
				json.NewEncoder(w).Encode(stats)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "stats endpoint error"})
			}
		case strings.Contains(path, "_nodes/_local") || strings.Contains(path, "_nodes/info"):
			if info, ok := responses["node_info"]; ok {
				json.NewEncoder(w).Encode(info)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "info endpoint error"})
			}
		default:
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "endpoint not found"})
		}
	}))
}

// Helper function to create test config
func createTestConfig() *Config {
	return &Config{
		PluginConfig: sensu.PluginConfig{
			Name:     "check-es-cluster-health",
			Short:    "Test config",
			Keyspace: "test",
		},
		Host:               "localhost",
		Port:               9200,
		Scheme:             "http",
		User:               "",
		Password:           "",
		Timeout:            30,
		StatusTimeout:      0,
		Level:              "",
		Local:              false,
		Index:              "",
		AlertStatus:        "",
		MasterOnly:         false,
		CheckFD:            false,
		FDCritical:         90,
		FDWarning:          80,
		CertFile:           "",
		InsecureSkipVerify: false,
		CheckNodes:         "",
	}
}

// Test argument validation
func TestCheckArgs(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		expectedStatus int
		expectedError  string
	}{
		{
			name:           "Valid default config",
			config:         createTestConfig(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Invalid alert status",
			config: func() *Config {
				c := createTestConfig()
				c.AlertStatus = "INVALID"
				return c
			}(),
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "invalid alert-status: INVALID, must be one of RED, YELLOW, GREEN",
		},
		{
			name: "Valid alert status GREEN",
			config: func() *Config {
				c := createTestConfig()
				c.AlertStatus = "GREEN"
				return c
			}(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Valid alert status YELLOW",
			config: func() *Config {
				c := createTestConfig()
				c.AlertStatus = "YELLOW"
				return c
			}(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Valid alert status RED",
			config: func() *Config {
				c := createTestConfig()
				c.AlertStatus = "RED"
				return c
			}(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Invalid FD warning - too low",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 0
				return c
			}(),
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "fd-warning must be between 1 and 99",
		},
		{
			name: "Invalid FD warning - too high",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 100
				return c
			}(),
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "fd-warning must be between 1 and 99",
		},
		{
			name: "Invalid FD critical - too low",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDCritical = 0
				return c
			}(),
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "fd-critical must be between 1 and 99",
		},
		{
			name: "Invalid FD critical - too high",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDCritical = 100
				return c
			}(),
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "fd-critical must be between 1 and 99",
		},
		{
			name: "Warning threshold higher than critical",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 95
				c.FDCritical = 90
				return c
			}(),
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "fd-warning must be less than fd-critical",
		},
		{
			name: "Warning threshold equal to critical",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 90
				c.FDCritical = 90
				return c
			}(),
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "fd-warning must be less than fd-critical",
		},
		{
			name: "Valid FD thresholds",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 70
				c.FDCritical = 90
				return c
			}(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Auto-upgrade to HTTPS with user",
			config: func() *Config {
				c := createTestConfig()
				c.User = "testuser"
				c.Scheme = "http"
				return c
			}(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Auto-upgrade to HTTPS with cert file",
			config: func() *Config {
				c := createTestConfig()
				c.CertFile = "/path/to/cert"
				c.Scheme = "http"
				return c
			}(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Invalid check-nodes value",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "invalid"
				return c
			}(),
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "invalid check-nodes value: invalid, must be 'local' or 'all'",
		},
		{
			name: "Valid check-nodes local",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "local"
				return c
			}(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Valid check-nodes all",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "all"
				return c
			}(),
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set global plugin config for testing
			plugin = *tt.config

			status, err := checkArgs(nil)

			if status != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, status)
			}

			if tt.expectedError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("Expected error containing '%s', got %v", tt.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})
	}
}

// Helper function to create ES client for testing
func createTestESClient(serverURL string) (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{serverURL},
		// Disable sniffing and health checks that might interfere with mocking
		DisableRetry:         true,
		MaxRetries:           0,
		DiscoverNodesOnStart: false,
		// Set a short timeout for tests
		Transport: &http.Transport{
			ResponseHeaderTimeout: 1 * time.Second,
		},
	}
	return elasticsearch.NewClient(cfg)
}

// Test cluster health checking
func TestCheckClusterHealth(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		mockResponse   interface{}
		expectedStatus int
		expectedOutput string
	}{
		{
			name:   "Green cluster status",
			config: createTestConfig(),
			mockResponse: MockClusterHealthResponse{
				Status:      "green",
				ClusterName: "test-cluster",
			},
			expectedStatus: sensu.CheckStateOK,
			expectedOutput: "OK: Cluster status is green",
		},
		{
			name:   "Yellow cluster status with no alert filter",
			config: createTestConfig(),
			mockResponse: MockClusterHealthResponse{
				Status:      "yellow",
				ClusterName: "test-cluster",
			},
			expectedStatus: sensu.CheckStateWarning,
			expectedOutput: "WARNING: Cluster status is yellow",
		},
		{
			name: "Yellow cluster status with yellow alert filter",
			config: func() *Config {
				c := createTestConfig()
				c.AlertStatus = "YELLOW"
				return c
			}(),
			mockResponse: MockClusterHealthResponse{
				Status:      "yellow",
				ClusterName: "test-cluster",
			},
			expectedStatus: sensu.CheckStateWarning,
			expectedOutput: "WARNING: Cluster status is yellow",
		},
		{
			name: "Yellow cluster status with red alert filter",
			config: func() *Config {
				c := createTestConfig()
				c.AlertStatus = "RED"
				return c
			}(),
			mockResponse: MockClusterHealthResponse{
				Status:      "yellow",
				ClusterName: "test-cluster",
			},
			expectedStatus: sensu.CheckStateOK,
			expectedOutput: "OK: Not alerting on yellow status",
		},
		{
			name:   "Red cluster status with no alert filter",
			config: createTestConfig(),
			mockResponse: MockClusterHealthResponse{
				Status:      "red",
				ClusterName: "test-cluster",
			},
			expectedStatus: sensu.CheckStateCritical,
			expectedOutput: "CRITICAL: Cluster status is red",
		},
		{
			name: "Red cluster status with red alert filter",
			config: func() *Config {
				c := createTestConfig()
				c.AlertStatus = "RED"
				return c
			}(),
			mockResponse: MockClusterHealthResponse{
				Status:      "red",
				ClusterName: "test-cluster",
			},
			expectedStatus: sensu.CheckStateCritical,
			expectedOutput: "CRITICAL: Cluster status is red",
		},
		{
			name: "Red cluster status with yellow alert filter",
			config: func() *Config {
				c := createTestConfig()
				c.AlertStatus = "YELLOW"
				return c
			}(),
			mockResponse: MockClusterHealthResponse{
				Status:      "red",
				ClusterName: "test-cluster",
			},
			expectedStatus: sensu.CheckStateOK,
			expectedOutput: "OK: Not alerting on red status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock server
			responses := map[string]interface{}{
				"cluster_health": tt.mockResponse,
			}
			server := createMockESServer(responses)
			defer server.Close()

			// Create Elasticsearch client pointing to mock server
			es, err := createTestESClient(server.URL)
			if err != nil {
				t.Fatalf("Failed to create ES client: %v", err)
			}

			// Set global plugin config
			plugin = *tt.config

			// Test cluster health check
			status, err := checkClusterHealth(es)

			if status != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, status)
			}

			if err != nil && tt.expectedStatus != sensu.CheckStateUnknown {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// Test file descriptor checking
func TestCheckFileDescriptors(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		mockResponse   MockNodesStatsResponse
		expectedStatus int
		expectedOutput string
	}{
		{
			name: "FD usage below warning threshold",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 80
				c.FDCritical = 90
				return c
			}(),
			mockResponse: MockNodesStatsResponse{
				Nodes: map[string]struct {
					Process struct {
						MaxFileDescriptors  int `json:"max_file_descriptors"`
						OpenFileDescriptors int `json:"open_file_descriptors"`
					} `json:"process"`
				}{
					"node1": {
						Process: struct {
							MaxFileDescriptors  int `json:"max_file_descriptors"`
							OpenFileDescriptors int `json:"open_file_descriptors"`
						}{
							MaxFileDescriptors:  1000,
							OpenFileDescriptors: 700, // 70%
						},
					},
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedOutput: "OK: fd usage at 70.0% (700/1000)",
		},
		{
			name: "FD usage at warning threshold",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 80
				c.FDCritical = 90
				return c
			}(),
			mockResponse: MockNodesStatsResponse{
				Nodes: map[string]struct {
					Process struct {
						MaxFileDescriptors  int `json:"max_file_descriptors"`
						OpenFileDescriptors int `json:"open_file_descriptors"`
					} `json:"process"`
				}{
					"node1": {
						Process: struct {
							MaxFileDescriptors  int `json:"max_file_descriptors"`
							OpenFileDescriptors int `json:"open_file_descriptors"`
						}{
							MaxFileDescriptors:  1000,
							OpenFileDescriptors: 850, // 85%
						},
					},
				},
			},
			expectedStatus: sensu.CheckStateWarning,
			expectedOutput: "WARNING: fd usage 85.0% exceeds 80%",
		},
		{
			name: "FD usage at critical threshold",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 80
				c.FDCritical = 90
				return c
			}(),
			mockResponse: MockNodesStatsResponse{
				Nodes: map[string]struct {
					Process struct {
						MaxFileDescriptors  int `json:"max_file_descriptors"`
						OpenFileDescriptors int `json:"open_file_descriptors"`
					} `json:"process"`
				}{
					"node1": {
						Process: struct {
							MaxFileDescriptors  int `json:"max_file_descriptors"`
							OpenFileDescriptors int `json:"open_file_descriptors"`
						}{
							MaxFileDescriptors:  1000,
							OpenFileDescriptors: 950, // 95%
						},
					},
				},
			},
			expectedStatus: sensu.CheckStateCritical,
			expectedOutput: "CRITICAL: fd usage 95.0% exceeds 90%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock server
			responses := map[string]interface{}{
				"nodes_stats": tt.mockResponse,
			}
			server := createMockESServer(responses)
			defer server.Close()

			// Create Elasticsearch client
			es, err := createTestESClient(server.URL)
			if err != nil {
				t.Fatalf("Failed to create ES client: %v", err)
			}

			// Set global plugin config
			plugin = *tt.config

			// Test file descriptor check
			status, err := checkFileDescriptors(es)

			if status != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, status)
			}

			if err != nil && tt.expectedStatus != sensu.CheckStateUnknown {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// Test node status checking
func TestCheckNodeStatus(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		mockResponse   MockNodesStatsResponse
		expectedStatus int
		expectedOutput string
	}{
		{
			name: "All nodes alive",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "all"
				return c
			}(),
			mockResponse: MockNodesStatsResponse{
				ClusterNodes: struct {
					Total      int `json:"total"`
					Successful int `json:"successful"`
					Failed     int `json:"failed"`
				}{
					Total:      3,
					Successful: 3,
					Failed:     0,
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedOutput: "OK: All 3 nodes are alive",
		},
		{
			name: "Some nodes failed",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "all"
				return c
			}(),
			mockResponse: MockNodesStatsResponse{
				ClusterNodes: struct {
					Total      int `json:"total"`
					Successful int `json:"successful"`
					Failed     int `json:"failed"`
				}{
					Total:      3,
					Successful: 2,
					Failed:     1,
				},
			},
			expectedStatus: sensu.CheckStateCritical,
			expectedOutput: "CRITICAL: 2 of 3 nodes are alive (1 failed)",
		},
		{
			name: "Local node alive",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "local"
				return c
			}(),
			mockResponse: MockNodesStatsResponse{
				Nodes: map[string]struct {
					Process struct {
						MaxFileDescriptors  int `json:"max_file_descriptors"`
						OpenFileDescriptors int `json:"open_file_descriptors"`
					} `json:"process"`
				}{
					"local-node": {
						Process: struct {
							MaxFileDescriptors  int `json:"max_file_descriptors"`
							OpenFileDescriptors int `json:"open_file_descriptors"`
						}{
							MaxFileDescriptors:  1000,
							OpenFileDescriptors: 500,
						},
					},
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedOutput: "OK: Local node is alive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock server
			responses := map[string]interface{}{
				"nodes_stats": tt.mockResponse,
			}
			server := createMockESServer(responses)
			defer server.Close()

			// Create Elasticsearch client
			es, err := createTestESClient(server.URL)
			if err != nil {
				t.Fatalf("Failed to create ES client: %v", err)
			}

			// Set global plugin config
			plugin = *tt.config

			// Test node status check
			status, err := checkNodeStatus(es)

			if status != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, status)
			}

			if err != nil && tt.expectedStatus != sensu.CheckStateUnknown {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// Test master node detection
func TestIsMasterNode(t *testing.T) {
	tests := []struct {
		name          string
		clusterState  MockClusterStateResponse
		nodeInfo      MockNodeInfoResponse
		expectedRes   bool
		expectedError string
	}{
		{
			name: "Is master node",
			clusterState: MockClusterStateResponse{
				MasterNode: "master-node-id",
			},
			nodeInfo: MockNodeInfoResponse{
				Nodes: map[string]struct {
					Name string `json:"name"`
				}{
					"master-node-id": {Name: "master-node"},
				},
			},
			expectedRes:   true,
			expectedError: "",
		},
		{
			name: "Is not master node",
			clusterState: MockClusterStateResponse{
				MasterNode: "master-node-id",
			},
			nodeInfo: MockNodeInfoResponse{
				Nodes: map[string]struct {
					Name string `json:"name"`
				}{
					"data-node-id": {Name: "data-node"},
				},
			},
			expectedRes:   false,
			expectedError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock server
			responses := map[string]interface{}{
				"cluster_state": tt.clusterState,
				"node_info":     tt.nodeInfo,
			}
			server := createMockESServer(responses)
			defer server.Close()

			// Create Elasticsearch client
			cfg := elasticsearch.Config{
				Addresses: []string{server.URL},
			}
			es, err := elasticsearch.NewClient(cfg)
			if err != nil {
				t.Fatalf("Failed to create ES client: %v", err)
			}

			// Test master node detection
			isMaster, err := isMasterNode(es)

			if isMaster != tt.expectedRes {
				t.Errorf("Expected %v, got %v", tt.expectedRes, isMaster)
			}

			if tt.expectedError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("Expected error containing '%s', got %v", tt.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})
	}
}

// Test Elasticsearch client creation
func TestCreateESClient(t *testing.T) {

	tests := []struct {
		name          string
		config        *Config
		setupFunc     func()
		cleanupFunc   func()
		expectError   bool
		errorContains string
	}{
		{
			name:   "Basic HTTP client",
			config: createTestConfig(),
			setupFunc: func() {
				plugin = *createTestConfig()
			},
			cleanupFunc: func() {},
			expectError: false,
		},
		{
			name: "HTTPS client with auth",
			config: func() *Config {
				c := createTestConfig()
				c.Scheme = "https"
				c.User = "testuser"
				c.Password = "testpass"
				c.InsecureSkipVerify = true
				return c
			}(),
			setupFunc: func() {
				c := createTestConfig()
				c.Scheme = "https"
				c.User = "testuser"
				c.Password = "testpass"
				c.InsecureSkipVerify = true
				plugin = *c
			},
			cleanupFunc: func() {},
			expectError: false,
		},
		{
			name: "HTTPS with invalid certificate file",
			config: func() *Config {
				c := createTestConfig()
				c.Scheme = "https"
				c.CertFile = "/nonexistent/cert.pem"
				return c
			}(),
			setupFunc: func() {
				c := createTestConfig()
				c.Scheme = "https"
				c.CertFile = "/nonexistent/cert.pem"
				plugin = *c
			},
			cleanupFunc:   func() {},
			expectError:   true,
			errorContains: "failed to read cert file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupFunc()
			defer tt.cleanupFunc()

			client, err := createESClient()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got none")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing '%s', got '%v'", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if client == nil {
					t.Errorf("Expected client to be created")
				}
			}
		})
	}
}

// Test full execution flow
func TestExecuteCheck(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		mockResponses  map[string]interface{}
		expectedStatus int
		expectedError  string
	}{
		{
			name:   "Successful health check only",
			config: createTestConfig(),
			mockResponses: map[string]interface{}{
				"cluster_health": MockClusterHealthResponse{
					Status:      "green",
					ClusterName: "test-cluster",
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Master only check - is master",
			config: func() *Config {
				c := createTestConfig()
				c.MasterOnly = true
				return c
			}(),
			mockResponses: map[string]interface{}{
				"cluster_state": MockClusterStateResponse{
					MasterNode: "master-node-id",
				},
				"node_info": MockNodeInfoResponse{
					Nodes: map[string]struct {
						Name string `json:"name"`
					}{
						"master-node-id": {Name: "master-node"},
					},
				},
				"cluster_health": MockClusterHealthResponse{
					Status:      "green",
					ClusterName: "test-cluster",
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Master only check - not master",
			config: func() *Config {
				c := createTestConfig()
				c.MasterOnly = true
				return c
			}(),
			mockResponses: map[string]interface{}{
				"cluster_state": MockClusterStateResponse{
					MasterNode: "master-node-id",
				},
				"node_info": MockNodeInfoResponse{
					Nodes: map[string]struct {
						Name string `json:"name"`
					}{
						"data-node-id": {Name: "data-node"},
					},
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Health and FD check",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 80
				c.FDCritical = 90
				return c
			}(),
			mockResponses: map[string]interface{}{
				"cluster_health": MockClusterHealthResponse{
					Status:      "green",
					ClusterName: "test-cluster",
				},
				"nodes_stats": MockNodesStatsResponse{
					Nodes: map[string]struct {
						Process struct {
							MaxFileDescriptors  int `json:"max_file_descriptors"`
							OpenFileDescriptors int `json:"open_file_descriptors"`
						} `json:"process"`
					}{
						"node1": {
							Process: struct {
								MaxFileDescriptors  int `json:"max_file_descriptors"`
								OpenFileDescriptors int `json:"open_file_descriptors"`
							}{
								MaxFileDescriptors:  1000,
								OpenFileDescriptors: 700,
							},
						},
					},
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Health OK, FD critical - should return critical",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				c.FDWarning = 80
				c.FDCritical = 90
				return c
			}(),
			mockResponses: map[string]interface{}{
				"cluster_health": MockClusterHealthResponse{
					Status:      "green",
					ClusterName: "test-cluster",
				},
				"nodes_stats": MockNodesStatsResponse{
					Nodes: map[string]struct {
						Process struct {
							MaxFileDescriptors  int `json:"max_file_descriptors"`
							OpenFileDescriptors int `json:"open_file_descriptors"`
						} `json:"process"`
					}{
						"node1": {
							Process: struct {
								MaxFileDescriptors  int `json:"max_file_descriptors"`
								OpenFileDescriptors int `json:"open_file_descriptors"`
							}{
								MaxFileDescriptors:  1000,
								OpenFileDescriptors: 950, // 95% - critical
							},
						},
					},
				},
			},
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "",
		},
		{
			name: "Node check only - all nodes",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "all"
				return c
			}(),
			mockResponses: map[string]interface{}{
				"nodes_stats": MockNodesStatsResponse{
					ClusterNodes: struct {
						Total      int `json:"total"`
						Successful int `json:"successful"`
						Failed     int `json:"failed"`
					}{
						Total:      3,
						Successful: 3,
						Failed:     0,
					},
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
		{
			name: "Node check only - local node",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "local"
				return c
			}(),
			mockResponses: map[string]interface{}{
				"nodes_stats": MockNodesStatsResponse{
					Nodes: map[string]struct {
						Process struct {
							MaxFileDescriptors  int `json:"max_file_descriptors"`
							OpenFileDescriptors int `json:"open_file_descriptors"`
						} `json:"process"`
					}{
						"local-node": {
							Process: struct {
								MaxFileDescriptors  int `json:"max_file_descriptors"`
								OpenFileDescriptors int `json:"open_file_descriptors"`
							}{
								MaxFileDescriptors:  1000,
								OpenFileDescriptors: 500,
							},
						},
					},
				},
			},
			expectedStatus: sensu.CheckStateOK,
			expectedError:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock server
			server := createMockESServer(tt.mockResponses)
			defer server.Close()

			// Parse server URL to get host and port
			serverURL := strings.TrimPrefix(server.URL, "http://")
			parts := strings.Split(serverURL, ":")
			host := parts[0]
			port := 80
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &port)
			}

			// Update config with mock server details
			tt.config.Host = host
			tt.config.Port = port
			tt.config.Scheme = "http"

			// Set global plugin config
			plugin = *tt.config

			// Execute check
			status, err := executeCheck(nil)

			if status != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, status)
			}

			if tt.expectedError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("Expected error containing '%s', got %v", tt.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})
	}
}

// Test error handling scenarios
func TestErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		mockResponses  map[string]interface{}
		serverError    bool
		expectedStatus int
		expectedError  string
	}{
		{
			name:          "Cluster health API error",
			config:        createTestConfig(),
			mockResponses: map[string]interface{}{
				// No cluster_health response - will trigger error
			},
			serverError:    true,
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "cluster health API error",
		},
		{
			name: "Nodes stats API error for FD check",
			config: func() *Config {
				c := createTestConfig()
				c.CheckFD = true
				return c
			}(),
			mockResponses: map[string]interface{}{
				"cluster_health": MockClusterHealthResponse{
					Status: "green",
				},
				// No nodes_stats response - will trigger error
			},
			serverError:    true,
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "nodes stats API error",
		},
		{
			name: "Master check cluster state error",
			config: func() *Config {
				c := createTestConfig()
				c.MasterOnly = true
				return c
			}(),
			mockResponses: map[string]interface{}{
				// No cluster_state response - will trigger error
			},
			serverError:    true,
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "failed to check master status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock server that returns errors for missing responses
			server := createMockESServer(tt.mockResponses)
			defer server.Close()

			// Parse server URL
			serverURL := strings.TrimPrefix(server.URL, "http://")
			parts := strings.Split(serverURL, ":")
			host := parts[0]
			port := 80
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &port)
			}

			// Update config
			tt.config.Host = host
			tt.config.Port = port
			tt.config.Scheme = "http"

			// Set global plugin config
			plugin = *tt.config

			// Execute check
			status, err := executeCheck(nil)

			if status != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, status)
			}

			if tt.expectedError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("Expected error containing '%s', got %v", tt.expectedError, err)
				}
			}
		})
	}
}

// Test edge cases and boundary conditions
func TestEdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		mockResponses  map[string]interface{}
		expectedStatus int
		description    string
	}{

		{
			name:   "Unknown cluster status",
			config: createTestConfig(),
			mockResponses: map[string]interface{}{
				"cluster_health": MockClusterHealthResponse{
					Status: "unknown",
				},
			},
			expectedStatus: sensu.CheckStateUnknown,
			description:    "Should return unknown for unrecognized cluster status",
		},
		{
			name: "All nodes check with no nodes",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "all"
				return c
			}(),
			mockResponses: map[string]interface{}{
				"nodes_stats": MockNodesStatsResponse{
					ClusterNodes: struct {
						Total      int `json:"total"`
						Successful int `json:"successful"`
						Failed     int `json:"failed"`
					}{
						Total:      0,
						Successful: 0,
						Failed:     0,
					},
				},
			},
			expectedStatus: sensu.CheckStateOK,
			description:    "Should return OK when no nodes to check",
		},
		{
			name: "Local node check with empty nodes map",
			config: func() *Config {
				c := createTestConfig()
				c.CheckNodes = "local"
				return c
			}(),
			mockResponses: map[string]interface{}{
				"nodes_stats": MockNodesStatsResponse{
					Nodes: map[string]struct {
						Process struct {
							MaxFileDescriptors  int `json:"max_file_descriptors"`
							OpenFileDescriptors int `json:"open_file_descriptors"`
						} `json:"process"`
					}{},
				},
			},
			expectedStatus: sensu.CheckStateCritical,
			description:    "Should return critical when no local node found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock server
			server := createMockESServer(tt.mockResponses)
			defer server.Close()

			// Parse server URL
			serverURL := strings.TrimPrefix(server.URL, "http://")
			parts := strings.Split(serverURL, ":")
			host := parts[0]
			port := 80
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &port)
			}

			// Update config
			tt.config.Host = host
			tt.config.Port = port
			tt.config.Scheme = "http"

			// Set global plugin config
			plugin = *tt.config

			// Execute check
			status, err := executeCheck(nil)

			if status != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d - %s", tt.expectedStatus, status, tt.description)
			}

			// For unknown states, we expect an error
			if tt.expectedStatus == sensu.CheckStateUnknown && err == nil {
				t.Errorf("Expected error for unknown state - %s", tt.description)
			}
		})
	}
}

// Test timeout and connection scenarios
func TestTimeoutAndConnection(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		serverFunc     func() *httptest.Server
		expectedStatus int
		expectedError  string
	}{
		{
			name: "Server connection refused",
			config: func() *Config {
				c := createTestConfig()
				c.Host = "nonexistent-host"
				c.Port = 9999
				return c
			}(),
			serverFunc: func() *httptest.Server {
				// Return nil - no server
				return nil
			},
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "cluster health API error",
		},
		{
			name:   "Server returns 500 error",
			config: createTestConfig(),
			serverFunc: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"error": "internal server error"}`))
				}))
			},
			expectedStatus: sensu.CheckStateCritical,
			expectedError:  "cluster health API error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var server *httptest.Server
			if tt.serverFunc != nil {
				server = tt.serverFunc()
				if server != nil {
					defer server.Close()

					// Parse server URL
					serverURL := strings.TrimPrefix(server.URL, "http://")
					parts := strings.Split(serverURL, ":")
					host := parts[0]
					port := 80
					if len(parts) > 1 {
						fmt.Sscanf(parts[1], "%d", &port)
					}

					tt.config.Host = host
					tt.config.Port = port
					tt.config.Scheme = "http"
				}
			}

			// Set global plugin config
			plugin = *tt.config

			// Execute check
			status, err := executeCheck(nil)

			if status != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, status)
			}

			if tt.expectedError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("Expected error containing '%s', got %v", tt.expectedError, err)
				}
			}
		})
	}
}

// Benchmark tests
func BenchmarkCheckClusterHealth(b *testing.B) {
	// Create mock server
	responses := map[string]interface{}{
		"cluster_health": MockClusterHealthResponse{
			Status:      "green",
			ClusterName: "test-cluster",
		},
	}
	server := createMockESServer(responses)
	defer server.Close()

	// Create Elasticsearch client
	es, err := createTestESClient(server.URL)
	if err != nil {
		b.Fatalf("Failed to create ES client: %v", err)
	}

	// Set plugin config
	plugin = *createTestConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := checkClusterHealth(es)
		if err != nil {
			b.Fatalf("Benchmark failed: %v", err)
		}
	}
}

func BenchmarkCheckFileDescriptors(b *testing.B) {
	// Create mock server

	responses := map[string]interface{}{
		"nodes_stats": MockNodesStatsResponse{
			Nodes: map[string]struct {
				Process struct {
					MaxFileDescriptors  int `json:"max_file_descriptors"`
					OpenFileDescriptors int `json:"open_file_descriptors"`
				} `json:"process"`
			}{
				"node1": {
					Process: struct {
						MaxFileDescriptors  int `json:"max_file_descriptors"`
						OpenFileDescriptors int `json:"open_file_descriptors"`
					}{
						MaxFileDescriptors:  1000,
						OpenFileDescriptors: 700,
					},
				},
			},
		},
	}
	server := createMockESServer(responses)
	defer server.Close()

	// Create Elasticsearch client
	es, err := createTestESClient(server.URL)
	if err != nil {
		b.Fatalf("Failed to create ES client: %v", err)
	}

	// Set plugin config
	config := createTestConfig()
	config.CheckFD = true
	plugin = *config

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := checkFileDescriptors(es)
		if err != nil {
			b.Fatalf("Benchmark failed: %v", err)
		}
	}
}

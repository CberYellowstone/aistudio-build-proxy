package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// --- Constants ---
const (
	clientIDKey           = contextKey("clientID")
	wsPath                = "/v1/ws"
	proxyListenAddr       = ":5345"
	wsReadTimeout         = 65 * time.Second
	proxyRequestTimeout   = 600 * time.Second
	healthCheckInterval   = 60 * time.Second
	healthCheckReqTimeout = 15 * time.Second
	initialRespTimeout    = 60 * time.Second
)

// --- Error Types ---
var (
	ErrConnectionLost     = errors.New("websocket connection lost")
	ErrConnectionTimeout  = errors.New("request timeout")
	ErrNoAvailableClient  = errors.New("no available client")
	ErrClientUnhealthy    = errors.New("client connection is currently unhealthy")
	ErrRequestWriteFailed = errors.New("failed to write request to client websocket")
	ErrRateLimited        = errors.New("client is rate limited")
	ErrInitialTimeout     = errors.New("client initial response timeout")
)

// --- Context Keys ---
type contextKey string

const requestStateKey = contextKey("requestState")

// --- Shared Request State ---
// Used to pass state between middleware handlers.
type requestState struct {
	ClientID string
}

// --- Prometheus Metrics ---
var (
	activeConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gpa_proxy_websocket_connections_active",
			Help: "Number of currently active WebSocket connections. Value is always 1 per clientID.",
		},
		[]string{"clientID"},
	)
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gpa_proxy_http_requests_total",
			Help: "Total number of HTTP requests handled.",
		},
		[]string{"method", "path", "status", "clientID"},
	)
	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gpa_proxy_http_request_duration_seconds",
			Help:    "Histogram of HTTP request durations.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "clientID"},
	)
	onlineClientsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "gpa_proxy_online_clients_total",
			Help: "Total number of currently online (connected) clients.",
		},
	)
	clientInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gpa_proxy_client_info",
			Help: "Info about online clients. A value of 1 indicates the client is online.",
		},
		[]string{"clientID"},
	)
)

var nextClientIndex uint32

// --- 1. Connection Management ---

type UserConnection struct {
	Conn           *websocket.Conn
	ClientID       string
	LastActive     time.Time
	writeMutex     sync.Mutex
	Disconnect     chan struct{}
	ConnID         string
	FailedAttempts int
	IsHealthy      bool
	LastFailure    time.Time
}

func (uc *UserConnection) safeWriteJSON(v interface{}) error {
	uc.writeMutex.Lock()
	defer uc.writeMutex.Unlock()
	return uc.Conn.WriteJSON(v)
}

func (uc *UserConnection) safeClose() {
	uc.writeMutex.Lock()
	defer uc.writeMutex.Unlock()
	select {
	case <-uc.Disconnect:
	default:
		close(uc.Disconnect)
	}
}

type ClientConnections struct {
	sync.Mutex
	Connection *UserConnection
}

type ConnectionPool struct {
	sync.RWMutex
	Clients map[string]*ClientConnections
}

var globalPool = &ConnectionPool{
	Clients: make(map[string]*ClientConnections),
}

func (p *ConnectionPool) AddConnection(clientID string, conn *websocket.Conn) *UserConnection {
	userConn := &UserConnection{
		Conn:       conn,
		ClientID:   clientID,
		LastActive: time.Now(),
		Disconnect: make(chan struct{}),
		ConnID:     uuid.New().String(),
		IsHealthy:  true,
	}

	p.Lock()
	defer p.Unlock()

	clientConns, exists := p.Clients[clientID]
	if exists && clientConns.Connection != nil {
		log.Info().Str("clientID", clientID).Str("oldConnID", clientConns.Connection.ConnID).Str("newConnID", userConn.ConnID).Msg("Replacing existing connection for client")
		clientConns.Connection.safeClose()
		clientConns.Connection.Conn.Close()
	} else if !exists {
		clientConns = &ClientConnections{}
		p.Clients[clientID] = clientConns
		onlineClientsTotal.Inc()
		clientInfo.WithLabelValues(clientID).Set(1)
	}

	clientConns.Lock()
	clientConns.Connection = userConn
	clientConns.Unlock()

	log.Info().Str("clientID", clientID).Str("connID", userConn.ConnID).Msg("WebSocket client connected and session is active")
	activeConnections.WithLabelValues(clientID).Set(1)
	return userConn
}

func (p *ConnectionPool) RemoveConnection(uc *UserConnection) {
	p.Lock()
	defer p.Unlock()

	clientConns, exists := p.Clients[uc.ClientID]
	if !exists {
		return
	}

	clientConns.Lock()
	defer clientConns.Unlock()

	if clientConns.Connection != nil && clientConns.Connection.ConnID == uc.ConnID {
		delete(p.Clients, uc.ClientID)
		clientConns.Connection = nil
		log.Info().Str("clientID", uc.ClientID).Str("connID", uc.ConnID).Msg("WebSocket client disconnected, removing client from pool")
		onlineClientsTotal.Dec()
		clientInfo.WithLabelValues(uc.ClientID).Set(0)
		activeConnections.DeleteLabelValues(uc.ClientID)
	}
}

func (p *ConnectionPool) GetConnection(clientID string) (*UserConnection, error) {
	p.RLock()
	clientConns, exists := p.Clients[clientID]
	p.RUnlock()

	if !exists {
		return nil, ErrNoAvailableClient
	}
	clientConns.Lock()
	defer clientConns.Unlock()

	if clientConns.Connection == nil {
		return nil, ErrNoAvailableClient
	}
	if !clientConns.Connection.IsHealthy {
		return nil, ErrClientUnhealthy
	}
	return clientConns.Connection, nil
}

func (p *ConnectionPool) getClientList() []string {
	p.RLock()
	defer p.RUnlock()
	clients := make([]string, 0, len(p.Clients))
	for id := range p.Clients {
		clients = append(clients, id)
	}
	sort.Strings(clients)
	return clients
}

// --- 2. WebSocket Handling ---

type WSMessage struct {
	ID      string                 `json:"id"`
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

var pendingRequests sync.Map

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	authToken := r.URL.Query().Get("auth_token")
	if authToken == "" {
		http.Error(w, "Unauthorized: missing auth_token", http.StatusUnauthorized)
		return
	}
	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		clientID = "client-" + uuid.NewString()
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to upgrade to WebSocket")
		return
	}
	userConn := globalPool.AddConnection(clientID, conn)
	go readPump(userConn)
}

func readPump(uc *UserConnection) {
	defer func() {
		uc.safeClose()
		globalPool.RemoveConnection(uc)
		uc.Conn.Close()
	}()

	uc.Conn.SetReadDeadline(time.Now().Add(wsReadTimeout))

	for {
		_, message, err := uc.Conn.ReadMessage()
		if err != nil {
			log.Warn().Err(err).Str("clientID", uc.ClientID).Msg("WebSocket read error")
			break
		}
		uc.LastActive = time.Now()
		// Reset read deadline on any received message (application-layer heartbeat)
		uc.Conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Error().Err(err).Msg("Error unmarshalling WebSocket message")
			continue
		}
		switch msg.Type {
		case "ping":
			_ = uc.safeWriteJSON(map[string]string{"type": "pong", "id": msg.ID})
		case "http_response", "stream_start", "stream_chunk", "stream_end", "error":
			if ch, ok := pendingRequests.Load(msg.ID); ok {
				ch.(chan *WSMessage) <- &msg
			} else {
				// Suppress warnings for timed-out health checks to avoid log spam.
				if !strings.HasPrefix(msg.ID, "healthcheck-") {
					log.Warn().Str("requestID", msg.ID).Msg("Received response for unknown or timed-out request")
				}
			}
		}
	}
}

// --- 3. HTTP Middleware Chain ---

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	size       int
}

func NewLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{w, http.StatusOK, 0}
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	size, err := lrw.ResponseWriter.Write(b)
	lrw.size += size
	return size, err
}

func (lrw *loggingResponseWriter) Flush() {
	if flusher, ok := lrw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := uuid.NewString()

		// Create a shared state for this request
		state := &requestState{ClientID: "unknown"}
		ctx := context.WithValue(r.Context(), "requestID", reqID)
		ctx = context.WithValue(ctx, requestStateKey, state)
		r = r.WithContext(ctx)

		log.Info().Str("type", "access_start").Str("method", r.Method).Str("path", r.URL.Path).Str("remote_addr", r.RemoteAddr).Str("requestID", reqID).Msg("Received new HTTP request")

		lrw := NewLoggingResponseWriter(w)
		next.ServeHTTP(lrw, r)

		duration := time.Since(start)
		// ClientID is now read from the shared state, which may have been updated by inner middleware.
		clientID := state.ClientID

		log.Info().
			Str("type", "access_finish").
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", lrw.statusCode).
			Int("size_bytes", lrw.size).
			Float64("duration_ms", float64(duration.Nanoseconds())/1e6).
			Str("requestID", reqID).
			Str("clientID", clientID).
			Msg("Handled HTTP request")

		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(lrw.statusCode), clientID).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path, clientID).Observe(duration.Seconds())
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")

		// Reflect the requested headers for preflight requests, effectively allowing any header
		if requestedHeaders := r.Header.Get("Access-Control-Request-Headers"); requestedHeaders != "" {
			w.Header().Set("Access-Control-Allow-Headers", requestedHeaders)
		} else {
			// Fallback to a default set if the request header is not present
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, x-goog-api-key")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func authMiddleware(next http.Handler, secretKey string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if secretKey == "" {
			next.ServeHTTP(w, r)
			return
		}
		if auth := r.Header.Get("Authorization"); strings.TrimPrefix(auth, "Bearer ") == secretKey {
			next.ServeHTTP(w, r)
			return
		}
		if key := r.Header.Get("x-goog-api-key"); key == secretKey {
			next.ServeHTTP(w, r)
			return
		}
		if key := r.URL.Query().Get("key"); key == secretKey {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "Unauthorized: Missing or invalid API key.", http.StatusUnauthorized)
	})
}

// --- 4. Core Proxy Logic ---

func clientSelectionMiddleware(next func(http.ResponseWriter, *http.Request, []byte) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID, _ := r.Context().Value("requestID").(string)
		state, _ := r.Context().Value(requestStateKey).(*requestState)

		// Read the body ONCE before the retry loop.
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		clientList := globalPool.getClientList()
		if len(clientList) == 0 {
			http.Error(w, "Service Unavailable: No active client connected", http.StatusServiceUnavailable)
			return
		}

		startIdx := atomic.AddUint32(&nextClientIndex, 1) - 1
		var lastErr error
		for i := 0; i < len(clientList); i++ {
			clientID := clientList[(startIdx+uint32(i))%uint32(len(clientList))]

			// Update the shared state instead of creating a new context for the logger to see.
			if state != nil {
				state.ClientID = clientID
			}

			// Pass the pre-read body to the handler.
			err := next(w, r, bodyBytes)

			if err == nil {
				return // Success
			}

			lastErr = err
			// Check for retryable errors. These are errors that are safe to retry on another client
			// without causing side effects.
			if errors.Is(err, ErrRequestWriteFailed) || errors.Is(err, ErrClientUnhealthy) || errors.Is(err, ErrRateLimited) || errors.Is(err, ErrInitialTimeout) {
				log.Warn().Err(err).Str("requestID", reqID).Str("clientID", clientID).Msg("Request attempt failed, transferring to next client.")
				// Mark client as unhealthy immediately upon failure
				if clientConns, exists := globalPool.Clients[clientID]; exists {
					clientConns.Lock()
					if clientConns.Connection != nil {
						markAsUnhealthy(clientConns.Connection, "request_fail", err)
					}
					clientConns.Unlock()
				}
				continue // Retry with next client
			} else {
				// Non-retryable error, break the loop and return the error
				break
			}
		}

		// If all clients failed
		log.Error().Err(lastErr).Str("requestID", reqID).Int("totalAttempts", len(clientList)).Msg("Failed to send request after trying all available clients")
		http.Error(w, "Bad Gateway: All available client connections failed", http.StatusBadGateway)
	})
}

func handleProxyRequest(w http.ResponseWriter, r *http.Request, bodyBytes []byte) error {
	reqID, _ := r.Context().Value("requestID").(string)
	state, _ := r.Context().Value(requestStateKey).(*requestState)
	clientID := state.ClientID // Get clientID from the shared state

	headers := make(map[string][]string)
	for k, v := range r.Header {
		if k != "Connection" && k != "Keep-Alive" && k != "Proxy-Authenticate" && k != "Proxy-Authorization" && k != "Te" && k != "Trailers" && k != "Transfer-Encoding" && k != "Upgrade" {
			headers[k] = v
		}
	}

	requestPayload := WSMessage{
		ID:   reqID,
		Type: "http_request",
		Payload: map[string]interface{}{
			"method":  r.Method,
			"url":     "https://generativelanguage.googleapis.com" + r.URL.String(),
			"headers": headers,
			"body":    string(bodyBytes),
		},
	}

	conn, err := globalPool.GetConnection(clientID)
	if err != nil {
		return err // Retryable
	}

	if err := conn.safeWriteJSON(requestPayload); err != nil {
		conn.IsHealthy = false
		conn.FailedAttempts++
		conn.LastFailure = time.Now()
		return ErrRequestWriteFailed // Retryable
	}

	conn.FailedAttempts = 0
	respChan := make(chan *WSMessage, 10)
	pendingRequests.Store(reqID, respChan)
	defer pendingRequests.Delete(reqID)

	return processWebSocketResponse(w, r, respChan, conn, reqID) // Final, non-retryable part
}

func processWebSocketResponse(w http.ResponseWriter, r *http.Request, respChan chan *WSMessage, conn *UserConnection, requestID string) error {
	ctx, cancel := context.WithTimeout(r.Context(), proxyRequestTimeout)
	defer cancel()
	flusher, _ := w.(http.Flusher)
	headersSet := false
	initialTimeout := time.NewTimer(initialRespTimeout)
	defer initialTimeout.Stop()

	for {
		select {
		case msg, ok := <-respChan:
			if !ok {
				if !headersSet {
					http.Error(w, "Internal Server Error: Response channel closed unexpectedly", http.StatusInternalServerError)
				}
				return nil
			}

			// Stop the initial timer as soon as we get any valid message for this request
			initialTimeout.Stop()

			switch msg.Type {
			case "http_response":
				if !headersSet {
					setResponseHeaders(w, msg.Payload)
					writeStatusCode(w, msg.Payload)
					headersSet = true
				}
				writeBody(w, msg.Payload)
				return nil
			case "stream_start":
				if !headersSet {
					setResponseHeaders(w, msg.Payload)
					writeStatusCode(w, msg.Payload)
					headersSet = true
					if flusher != nil {
						flusher.Flush()
					}
				}
			case "stream_chunk":
				if !headersSet {
					w.WriteHeader(http.StatusOK)
					headersSet = true
				}
				writeBody(w, msg.Payload)
				if flusher != nil {
					flusher.Flush()
				}
			case "stream_end":
				if !headersSet {
					w.WriteHeader(http.StatusOK)
				}
				return nil
			case "error":
				statusCode := http.StatusBadGateway
				if code, ok := msg.Payload["status"].(float64); ok {
					statusCode = int(code)
				}

				if statusCode == http.StatusTooManyRequests {
					return ErrRateLimited // This is a retryable error
				}

				if !headersSet {
					errMsg := "Bad Gateway: Client reported an error"
					if details, ok := msg.Payload["details"].(string); ok {
						errMsg = details
					}
					setResponseHeaders(w, msg.Payload)
					http.Error(w, errMsg, statusCode)
				}
				return nil // This is a final, non-retryable error
			}
		case <-conn.Disconnect:
			// Do not write to the response here. Let the middleware handle the retry.
			return ErrConnectionLost
		case <-ctx.Done():
			if err := conn.safeWriteJSON(WSMessage{ID: requestID, Type: "http_request_cancel"}); err != nil {
				log.Error().Err(err).Str("requestID", requestID).Msg("Failed to send cancel signal")
			}
			// Do not write to the response here. Let the middleware handle the retry.
			return ErrConnectionTimeout
		case <-initialTimeout.C:
			// Do not write to the response here. Let the middleware handle the retry.
			return ErrInitialTimeout
		}
	}
}

// --- 5. Health Checking ---

func forceHealthCheck(pool *ConnectionPool) {
	pool.RLock()
	clientIDs := make([]string, 0, len(pool.Clients))
	for id := range pool.Clients {
		clientIDs = append(clientIDs, id)
	}
	pool.RUnlock()

	var wg sync.WaitGroup
	for _, clientID := range clientIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			checkClientHealth(pool, id)
		}(clientID)
	}
	wg.Wait()
}

func handleForceHealthCheck(w http.ResponseWriter, r *http.Request) {
	log.Info().Msg("Manual health check triggered via API.")
	forceHealthCheck(globalPool)

	statusReport := make(map[string]bool)
	globalPool.RLock()
	for id, clientConns := range globalPool.Clients {
		clientConns.Lock()
		if clientConns.Connection != nil {
			statusReport[id] = clientConns.Connection.IsHealthy
		} else {
			statusReport[id] = false
		}
		clientConns.Unlock()
	}
	globalPool.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(statusReport); err != nil {
		log.Error().Err(err).Msg("Failed to write health check status response.")
		http.Error(w, "Failed to encode status report", http.StatusInternalServerError)
	}
}

func startHealthChecker(pool *ConnectionPool, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		forceHealthCheck(pool)
	}
}

func checkClientHealth(pool *ConnectionPool, clientID string) {
	clientConns, exists := pool.Clients[clientID]
	if !exists {
		return
	}
	clientConns.Lock()
	uc := clientConns.Connection
	clientConns.Unlock()

	if uc == nil {
		return
	}

	healthCheckID := "healthcheck-" + uuid.NewString()
	requestPayload := WSMessage{
		ID:   healthCheckID,
		Type: "http_request",
		Payload: map[string]interface{}{
			"method": "GET",
			"url":    "https://generativelanguage.googleapis.com/v1beta/models?key=GEMINI_API_KEY&pageSize=1",
		},
	}

	respChan := make(chan *WSMessage, 10)
	pendingRequests.Store(healthCheckID, respChan)
	defer pendingRequests.Delete(healthCheckID)

	if err := uc.safeWriteJSON(requestPayload); err != nil {
		markAsUnhealthy(uc, "write_failure", err)
		return
	}

	select {
	case msg := <-respChan:
		// End-to-end health check: a client is healthy only if it can get a 200 OK response from the upstream.
		// This handles both regular and streaming responses, as the client implementation
		// is expected to include the status in the 'stream_start' message as well.
		if msg.Type == "http_response" || msg.Type == "stream_start" {
			if status, ok := msg.Payload["status"].(float64); ok && int(status) == http.StatusOK {
				markAsHealthy(uc)
			} else {
				// Received a response, but it's not 200 OK.
				markAsUnhealthy(uc, "bad_status", errors.New("health check returned non-200 status"))
			}
		} else {
			// Received an unexpected response type (e.g., 'error', 'stream_chunk').
			errMsg := "unexpected response type for health check: " + msg.Type
			markAsUnhealthy(uc, "bad_response_type", errors.New(errMsg))
		}
	case <-time.After(healthCheckReqTimeout):
		markAsUnhealthy(uc, "timeout", errors.New("health check request timed out"))
	case <-uc.Disconnect:
		// Connection already closed, no need to mark as unhealthy
	}
}

func markAsUnhealthy(uc *UserConnection, reason string, err error) {
	if uc.IsHealthy {
		log.Warn().Err(err).Str("clientID", uc.ClientID).Str("reason", reason).Msg("Health check failed, marking as unhealthy.")
	}
	uc.IsHealthy = false
	uc.FailedAttempts++
	uc.LastFailure = time.Now()
}

func markAsHealthy(uc *UserConnection) {
	if !uc.IsHealthy {
		log.Info().Str("clientID", uc.ClientID).Msg("Connection passed health check and is now marked as healthy.")
	}
	uc.IsHealthy = true
	uc.FailedAttempts = 0
}

// --- Helper Functions ---

func setResponseHeaders(w http.ResponseWriter, payload map[string]interface{}) {
	if headers, ok := payload["headers"].(map[string]interface{}); ok {
		for key, value := range headers {
			if values, ok := value.([]interface{}); ok {
				for _, v := range values {
					if strV, ok := v.(string); ok {
						w.Header().Add(key, strV)
					}
				}
			} else if strV, ok := value.(string); ok {
				w.Header().Set(key, strV)
			}
		}
	}
}

func writeStatusCode(w http.ResponseWriter, payload map[string]interface{}) {
	if status, ok := payload["status"].(float64); ok {
		w.WriteHeader(int(status))
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func writeBody(w http.ResponseWriter, payload map[string]interface{}) {
	var bodyData []byte
	if body, ok := payload["body"].(string); ok {
		bodyData = []byte(body)
	} else if data, ok := payload["data"].(string); ok {
		bodyData = []byte(data)
	}
	if len(bodyData) > 0 {
		_, _ = w.Write(bodyData)
	}
}

// --- Main Application ---

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	proxyAuthKey := os.Getenv("PROXY_AUTH_KEY")
	if proxyAuthKey != "" {
		log.Info().Msg("Proxy authentication ENABLED.")
	} else {
		log.Info().Msg("Proxy authentication DISABLED.")
	}

	mux := http.NewServeMux()
	mux.HandleFunc(wsPath, handleWebSocket)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthcheck", handleForceHealthCheck)

	selectionHandler := clientSelectionMiddleware(handleProxyRequest)
	authedHandler := authMiddleware(selectionHandler, proxyAuthKey)
	corsHandler := corsMiddleware(authedHandler)
	finalHandler := loggingMiddleware(corsHandler)
	mux.Handle("/", finalHandler)

	server := &http.Server{
		Addr:    proxyListenAddr,
		Handler: mux,
	}

	go startHealthChecker(globalPool, healthCheckInterval)

	go func() {
		log.Info().Str("address", server.Addr).Msg("Starting server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Could not start server")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("Shutdown signal received, starting graceful shutdown...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal().Err(err).Msg("Server forced to shutdown")
	}
	log.Info().Msg("Server exiting gracefully")
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
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
	wsPath              = "/v1/ws"
	proxyListenAddr     = ":5345"
	wsReadTimeout       = 90 * time.Second // 延长以适应服务端ping的间隔
	proxyRequestTimeout = 600 * time.Second
	healthCheckInterval = 30 * time.Second
	pingWriteTimeout    = 5 * time.Second
	maxRequestRetries   = 3
	maxFailedAttempts   = 3 // 超过3次失败则熔断
)

// --- Error Types ---
var (
	ErrConnectionLost    = errors.New("websocket connection lost")
	ErrConnectionTimeout = errors.New("request timeout")
	ErrNoAvailableClient = errors.New("no available client")
)

// --- Prometheus Metrics ---
var (
	activeConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gpa_proxy_websocket_connections_active",
			Help: "Number of currently active WebSocket connections.",
		},
		[]string{"clientID"},
	)
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gpa_proxy_http_requests_total",
			Help: "Total number of HTTP requests handled.",
		},
		[]string{"method", "path", "status"},
	)
	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gpa_proxy_http_request_duration_seconds",
			Help:    "Histogram of HTTP request durations.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
)

// --- 1. 连接管理与负载均衡 ---

// UserConnection 存储单个WebSocket连接及其元数据
type UserConnection struct {
	Conn           *websocket.Conn
	ClientID       string // 用于区分不同浏览器实例的标识符
	LastActive     time.Time
	writeMutex     sync.Mutex    // 保护对此单个连接的并发写入
	Disconnect     chan struct{} // 断开信号，关闭时广播给所有等待者
	ConnID         string        // 连接唯一标识符，用于日志追踪
	FailedAttempts int           // 连续失败次数
	IsHealthy      bool          // 健康状态标志
	LastFailure    time.Time     // 最后一次失败的时间戳
}

// safeWriteJSON 线程安全地向单个WebSocket连接写入JSON
func (uc *UserConnection) safeWriteJSON(v interface{}) error {
	uc.writeMutex.Lock()
	defer uc.writeMutex.Unlock()
	return uc.Conn.WriteJSON(v)
}

// safeClose 安全关闭断开信号channel
func (uc *UserConnection) safeClose() {
	uc.writeMutex.Lock()
	defer uc.writeMutex.Unlock()

	select {
	case <-uc.Disconnect:
		// Channel已经关闭
	default:
		close(uc.Disconnect)
	}
}

// ClientConnections 维护单个客户端的所有连接和负载均衡状态
type ClientConnections struct {
	sync.Mutex
	Connections []*UserConnection
	NextIndex   int // 用于在同级健康的连接中实现轮询
}

// ConnectionPool 全局连接池，并发安全
type ConnectionPool struct {
	sync.RWMutex
	Clients map[string]*ClientConnections
}

var globalPool = &ConnectionPool{
	Clients: make(map[string]*ClientConnections),
}

// AddConnection 将新连接添加到池中
func (p *ConnectionPool) AddConnection(clientID string, conn *websocket.Conn) *UserConnection {
	userConn := &UserConnection{
		Conn:           conn,
		ClientID:       clientID,
		LastActive:     time.Now(),
		Disconnect:     make(chan struct{}), // 初始化断开信号channel
		ConnID:         uuid.New().String(), // 生成唯一连接ID
		FailedAttempts: 0,                   // 初始化失败次数为0
		IsHealthy:      true,                // 初始状态为健康
	}

	p.Lock()
	defer p.Unlock()

	clientConns, exists := p.Clients[clientID]
	if !exists {
		clientConns = &ClientConnections{
			Connections: make([]*UserConnection, 0),
		}
		p.Clients[clientID] = clientConns
	}

	clientConns.Lock()
	clientConns.Connections = append(clientConns.Connections, userConn)
	clientConns.Unlock()

	log.Info().
		Str("clientID", clientID).
		Str("connID", userConn.ConnID).
		Int("totalConnections", len(clientConns.Connections)).
		Msg("WebSocket client connected")

	activeConnections.WithLabelValues(clientID).Inc()
	return userConn
}

// RemoveConnection 从池中移除连接
func (p *ConnectionPool) RemoveConnection(clientID string, conn *websocket.Conn) {
	p.Lock()
	defer p.Unlock()

	clientConns, exists := p.Clients[clientID]
	if !exists {
		return
	}

	clientConns.Lock()
	defer clientConns.Unlock()

	// 使用更高效的"交换并截断"方式删除元素
	lastIdx := len(clientConns.Connections) - 1
	for i, uc := range clientConns.Connections {
		if uc.Conn == conn {
			clientConns.Connections[i] = clientConns.Connections[lastIdx]
			clientConns.Connections = clientConns.Connections[:lastIdx]
			log.Info().
				Str("clientID", clientID).
				Str("connID", uc.ConnID).
				Int("remainingConnections", len(clientConns.Connections)).
				Msg("WebSocket client disconnected")
			break
		}
	}

	if len(clientConns.Connections) == 0 {
		delete(p.Clients, clientID)
		log.Info().Str("clientID", clientID).Msg("No connections left for client, removing client from pool")
		activeConnections.DeleteLabelValues(clientID)
	}
}

// GetConnection 使用分级轮询策略为客户端选择一个连接
func (p *ConnectionPool) GetConnection(clientID string) (*UserConnection, error) {
	p.RLock()
	clientConns, exists := p.Clients[clientID]
	p.RUnlock()

	if !exists {
		return nil, ErrNoAvailableClient
	}

	clientConns.Lock()
	defer clientConns.Unlock()

	var healthyConnections []*UserConnection
	for _, uc := range clientConns.Connections {
		if uc.IsHealthy {
			healthyConnections = append(healthyConnections, uc)
		}
	}

	if len(healthyConnections) == 0 {
		return nil, ErrNoAvailableClient
	}

	// 1. 找到最小的失败次数
	minFails := -1
	for _, uc := range healthyConnections {
		if minFails == -1 || uc.FailedAttempts < minFails {
			minFails = uc.FailedAttempts
		}
	}

	// 2. 构建最佳候选池
	var bestCandidates []*UserConnection
	for _, uc := range healthyConnections {
		if uc.FailedAttempts == minFails {
			bestCandidates = append(bestCandidates, uc)
		}
	}

	// 3. 在最佳候选池中进行轮询
	if len(bestCandidates) == 0 {
		// 理论上不应该发生，因为 healthyConnections 不为空
		return nil, ErrNoAvailableClient
	}

	idx := clientConns.NextIndex % len(bestCandidates)
	selectedConn := bestCandidates[idx]
	clientConns.NextIndex = (clientConns.NextIndex + 1) % len(bestCandidates)

	return selectedConn, nil
}

// --- 2. WebSocket 消息结构 & 待处理请求 ---

// WSMessage 是前后端之间通信的基本结构
type WSMessage struct {
	ID      string                 `json:"id"`
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

var pendingRequests sync.Map

// --- 3. WebSocket 处理器和心跳 ---

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	authToken := r.URL.Query().Get("auth_token")
	if authToken == "" {
		log.Warn().Msg("WebSocket connection failed: missing auth_token")
		http.Error(w, "Unauthorized: missing auth_token", http.StatusUnauthorized)
		return
	}

	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		clientID = "client-" + uuid.NewString()
		log.Warn().Str("assignedClientID", clientID).Msg("WebSocket connection missing client_id, assigned a new one")
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
		// 关键：先发送断开信号，再清理连接
		uc.safeClose() // 通知所有等待该连接的goroutine
		globalPool.RemoveConnection(uc.ClientID, uc.Conn)
		uc.Conn.Close()
		log.Info().Str("clientID", uc.ClientID).Str("connID", uc.ConnID).Msg("readPump closed for client")
	}()

	uc.Conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
	// 浏览器收到ping会自动回pong，这里设置一个PongHandler来刷新ReadDeadline
	uc.Conn.SetPongHandler(func(string) error {
		uc.Conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		return nil
	})

	for {
		_, message, err := uc.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Warn().Err(err).Str("clientID", uc.ClientID).Msg("WebSocket read error")
			} else {
				log.Info().Err(err).Str("clientID", uc.ClientID).Msg("WebSocket closed")
			}
			break
		}

		// 收到任何消息都可认为连接是活跃的，刷新超时
		uc.Conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		uc.LastActive = time.Now()

		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Error().Err(err).Msg("Error unmarshalling WebSocket message")
			continue
		}

		switch msg.Type {
		case "ping":
			err := uc.safeWriteJSON(map[string]string{"type": "pong", "id": msg.ID})
			if err != nil {
				log.Error().Err(err).Str("clientID", uc.ClientID).Msg("Error sending pong")
				return
			}
		case "http_response", "stream_start", "stream_chunk", "stream_end", "error":
			if ch, ok := pendingRequests.Load(msg.ID); ok {
				respChan := ch.(chan *WSMessage)
				select {
				case respChan <- &msg:
				default:
					log.Warn().Str("requestID", msg.ID).Str("msgType", msg.Type).Msg("Response channel full, dropping message")
				}
			} else {
				log.Warn().Str("requestID", msg.ID).Msg("Received response for unknown or timed-out request")
			}
		default:
			log.Warn().Str("msgType", msg.Type).Str("clientID", uc.ClientID).Msg("Received unknown message type from client")
		}
	}
}

// --- 4. HTTP 反向代理与 WS 隧道 (含重试逻辑) ---

// loggingResponseWriter is a wrapper around http.ResponseWriter to capture status code and response size
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

// Flush a loggingResponseWriter to implement http.Flusher
func (lrw *loggingResponseWriter) Flush() {
	if flusher, ok := lrw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := uuid.NewString()
		// 将requestID注入到请求上下文中，以便后续处理器可以获取
		ctx := context.WithValue(r.Context(), "requestID", reqID)
		r = r.WithContext(ctx)

		lrw := NewLoggingResponseWriter(w)
		next.ServeHTTP(lrw, r)

		log.Info().
			Str("type", "access_log").
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("remote_addr", r.RemoteAddr).
			Int("status", lrw.statusCode).
			Int("size_bytes", lrw.size).
			Float64("duration_ms", float64(time.Since(start).Nanoseconds())/1e6).
			Str("requestID", reqID).
			Msg("Handled HTTP request")

		// Record metrics
		duration := time.Since(start)
		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(lrw.statusCode)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration.Seconds())
	})
}

func handleProxyRequest(w http.ResponseWriter, r *http.Request) {
	reqID, _ := r.Context().Value("requestID").(string)
	hlog := log.With().Str("requestID", reqID).Logger()

	clientID, err := authenticateHTTPRequest(r)
	if err != nil {
		hlog.Warn().Err(err).Msg("Proxy authentication failed")
		http.Error(w, "Proxy authentication failed: "+err.Error(), http.StatusUnauthorized)
		return
	}
	respChan := make(chan *WSMessage, 10)
	pendingRequests.Store(reqID, respChan)
	defer pendingRequests.Delete(reqID)

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

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

	var selectedConn *UserConnection
	var requestSent bool

	for i := 0; i < maxRequestRetries; i++ {
		conn, err := globalPool.GetConnection(clientID)
		if err != nil {
			hlog.Warn().Err(err).Int("attempt", i+1).Str("clientID", clientID).Msg("Error getting connection for client")
			if i == maxRequestRetries-1 { // 最后一次尝试失败
				http.Error(w, "Service Unavailable: No active client connected", http.StatusServiceUnavailable)
				return
			}
			time.Sleep(500 * time.Millisecond) // 等待一下再重试
			continue
		}

		if err := conn.safeWriteJSON(requestPayload); err != nil {
			hlog.Error().Err(err).Int("attempt", i+1).Str("clientID", clientID).Str("connID", conn.ConnID).Msg("Failed to send request over WebSocket, increasing failure count")

			conn.FailedAttempts++
			conn.LastFailure = time.Now()
			if conn.FailedAttempts >= maxFailedAttempts {
				conn.IsHealthy = false
				hlog.Warn().Str("connID", conn.ConnID).Msg("Circuit breaker triggered. Marked as unhealthy")
			}
			continue
		}

		conn.FailedAttempts = 0 // 请求成功，重置失败计数
		selectedConn = conn
		requestSent = true
		hlog.Info().Str("clientID", clientID).Str("connID", conn.ConnID).Int("attempt", i+1).Msg("Request sent successfully")
		break
	}

	if !requestSent {
		hlog.Error().Str("clientID", clientID).Int("attempts", maxRequestRetries).Msg("Failed to send request after multiple attempts")
		http.Error(w, "Bad Gateway: All available client connections failed", http.StatusBadGateway)
		return
	}

	// 传递选中的连接给响应处理函数
	if err := processWebSocketResponse(w, r, respChan, selectedConn, reqID); err != nil {
		// 处理特定错误类型
		if errors.Is(err, ErrConnectionLost) {
			log.Warn().Str("requestID", reqID).Str("connID", selectedConn.ConnID).Msg("Connection lost during processing")
			// 连接丢失错误已在processWebSocketResponse中处理HTTP响应
		}
		// 其他错误已在processWebSocketResponse中处理
	}
}

func processWebSocketResponse(w http.ResponseWriter, r *http.Request, respChan chan *WSMessage,
	conn *UserConnection, requestID string) error {
	ctx, cancel := context.WithTimeout(r.Context(), proxyRequestTimeout)
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Warn().Str("requestID", requestID).Msg("ResponseWriter does not support flushing, streaming will be buffered")
	}

	headersSet := false
	for {
		select {
		case msg, ok := <-respChan:
			if !ok {
				if !headersSet {
					http.Error(w, "Internal Server Error: Response channel closed unexpectedly", http.StatusInternalServerError)
				}
				return nil
			}

			switch msg.Type {
			case "http_response":
				if headersSet {
					log.Warn().Str("requestID", requestID).Msg("Received http_response after headers were already set. Ignoring.")
					return nil
				}
				setResponseHeaders(w, msg.Payload)
				writeStatusCode(w, msg.Payload)
				writeBody(w, msg.Payload)
				return nil

			case "stream_start":
				if headersSet {
					log.Warn().Str("requestID", requestID).Msg("Received stream_start after headers were already set. Ignoring.")
					continue
				}
				setResponseHeaders(w, msg.Payload)
				writeStatusCode(w, msg.Payload)
				headersSet = true
				if flusher != nil {
					flusher.Flush()
				}

			case "stream_chunk":
				if !headersSet {
					log.Warn().Str("requestID", requestID).Msg("Received stream_chunk before stream_start. Using default 200 OK.")
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
				if !headersSet {
					errMsg := "Bad Gateway: Client reported an error"
					if details, ok := msg.Payload["details"].(string); ok && details != "" {
						errMsg = details // 优先使用浏览器传来的详细错误
					} else if payloadErr, ok := msg.Payload["error"].(string); ok {
						errMsg = payloadErr
					}

					statusCode := http.StatusBadGateway
					if code, ok := msg.Payload["status"].(float64); ok {
						statusCode = int(code) // 使用浏览器传来的原始状态码
					}

					// 将原始的 Content-Type 等头信息也透传回去
					setResponseHeaders(w, msg.Payload)
					http.Error(w, errMsg, statusCode)

				} else {
					log.Error().Str("requestID", requestID).Interface("payload", msg.Payload).Msg("Error received from client after stream started")
				}
				return nil

			default:
				log.Error().Str("requestID", requestID).Str("msgType", msg.Type).Msg("Received unexpected message type while waiting for response")
			}

		case <-conn.Disconnect:
			// 连接断开信号
			log.Warn().Str("requestID", requestID).Str("clientID", conn.ClientID).Str("connID", conn.ConnID).Msg("WebSocket connection lost during processing")
			if !headersSet {
				// 还没发送响应头，可以返回错误状态码
				http.Error(w, "Bad Gateway: WebSocket connection lost", http.StatusBadGateway)
			} else {
				// 已经开始发送响应，记录日志但无法改变HTTP状态
				log.Warn().Str("requestID", requestID).Msg("Stream interrupted due to connection loss")
			}
			return ErrConnectionLost

		case <-ctx.Done():
			// 超时或客户端断开连接处理
			// 在关闭响应之前，向浏览器客户端发送取消信令
			cancelMessage := WSMessage{
				ID:   requestID,
				Type: "http_request_cancel",
			}
			if err := conn.safeWriteJSON(cancelMessage); err != nil {
				log.Error().Err(err).Str("requestID", requestID).Msg("Failed to send cancel signal to client")
			} else {
				log.Info().Str("requestID", requestID).Msg("Sent cancel signal to client")
			}

			if !headersSet {
				log.Error().Str("requestID", requestID).Dur("timeout", proxyRequestTimeout).Msg("Gateway Timeout - no response from client")
				http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
			} else {
				log.Error().Str("requestID", requestID).Dur("timeout", proxyRequestTimeout).Msg("Gateway Timeout - stream incomplete")
			}
			return ErrConnectionTimeout
		}
	}
}

// --- 5. 主动健康检查 ---

func startHealthChecker(pool *ConnectionPool, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Info().Dur("interval", interval).Msg("Proactive health checker started")

	for range ticker.C {
		var connectionsToRemove []*UserConnection

		pool.RLock()
		for _, userConns := range pool.Clients {
			userConns.Lock()
			for _, uc := range userConns.Connections {
				if err := uc.Conn.SetWriteDeadline(time.Now().Add(pingWriteTimeout)); err != nil {
					connectionsToRemove = append(connectionsToRemove, uc)
					continue
				}

				if err := uc.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					connectionsToRemove = append(connectionsToRemove, uc)
				} else {
					// Ping成功，检查是否可以恢复健康状态
					if !uc.IsHealthy {
						uc.IsHealthy = true
						uc.FailedAttempts = 0
						log.Info().Str("connID", uc.ConnID).Str("clientID", uc.ClientID).Msg("Connection passed health check and is now marked as healthy")
					}
				}

				uc.Conn.SetWriteDeadline(time.Time{})
			}
			userConns.Unlock()
		}
		pool.RUnlock()

		if len(connectionsToRemove) > 0 {
			log.Warn().Int("count", len(connectionsToRemove)).Msg("Health checker found dead connections to remove")
			for _, uc := range connectionsToRemove {
				log.Info().Str("clientID", uc.ClientID).Str("connID", uc.ConnID).Msg("Health checker removing dead connection")
				uc.safeClose() // 确保断开信号被发送
				pool.RemoveConnection(uc.ClientID, uc.Conn)
			}
		}
	}
}

// --- 辅助函数 ---

func setResponseHeaders(w http.ResponseWriter, payload map[string]interface{}) {
	headers, ok := payload["headers"].(map[string]interface{})
	if !ok {
		return
	}
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
	}
	if data, ok := payload["data"].(string); ok {
		bodyData = []byte(data)
	}
	if len(bodyData) > 0 {
		w.Write(bodyData)
	}
}

func authenticateHTTPRequest(r *http.Request) (string, error) {
	// 为HTTP请求选择一个可用的客户端ID
	// 这是一个简化策略：如果只有一个客户端，就用它。如果有多个，就返回一个错误，提示需要更明确的路由规则。
	// 在更复杂的系统中，可以基于请求头或路径来选择客户端。
	globalPool.RLock()
	defer globalPool.RUnlock()

	if len(globalPool.Clients) == 0 {
		return "", ErrNoAvailableClient
	}

	// 简单起见，我们只取第一个客户端
	// 注意：map的迭代顺序是不保证的，所以这实际上是随机选择了一个
	for clientID := range globalPool.Clients {
		return clientID, nil
	}

	return "", errors.New("could not determine a client to route to")
}

// --- 主函数 (含优雅退出) ---

func main() {
	// zerolog时间戳默认是Unix时间格式，改为更易读的格式
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	mux := http.NewServeMux()
	mux.HandleFunc(wsPath, handleWebSocket)
	mux.Handle("/metrics", promhttp.Handler()) // Expose the registered metrics
	mux.Handle("/", loggingMiddleware(http.HandlerFunc(handleProxyRequest)))

	server := &http.Server{
		Addr:    proxyListenAddr,
		Handler: mux,
	}

	go startHealthChecker(globalPool, healthCheckInterval)

	go func() {
		log.Info().Str("address", server.Addr).Msg("Starting server")
		log.Info().Str("ws_endpoint", wsPath).Msg("WebSocket endpoint available")
		log.Info().Str("http_proxy", "/").Msg("HTTP proxy available")
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

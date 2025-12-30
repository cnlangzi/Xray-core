package grpc

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"strconv"
	"time"

	c "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/store"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/grpc/encoding"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func Dial(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
	errors.LogInfo(ctx, "creating connection to ", dest)

	conn, err := dialgRPC(ctx, dest, streamSettings)
	if err != nil {
		return nil, errors.New("failed to dial gRPC").Base(err)
	}
	return stat.Connection(conn), nil
}

type dialerConf struct {
	net.Destination
	*internet.MemoryStreamConfig
}

// generateCacheKey creates a content-based cache key for gRPC connections
func (d dialerConf) generateCacheKey() string {
	// Create a string representation of the key components
	keyStr := fmt.Sprintf("%s:%s:%s:%s:%s",
		d.Address.String(),
		d.Port.String(),
		d.Network.String(),
		d.ProtocolName,
		d.SecurityType,
	)

	// Add protocol-specific settings hash if available
	if grpcSettings, ok := d.ProtocolSettings.(*Config); ok {
		keyStr += fmt.Sprintf(":%s:%s:%t:%d:%d:%t",
			grpcSettings.ServiceName,
			grpcSettings.Authority,
			grpcSettings.MultiMode,
			grpcSettings.IdleTimeout,
			grpcSettings.HealthCheckTimeout,
			grpcSettings.PermitWithoutStream,
		)
	}

	// Add security settings hash
	if d.SecuritySettings != nil {
		if tlsConfig, ok := d.SecuritySettings.(*tls.Config); ok {
			keyStr += fmt.Sprintf(":tls:%s:%s", tlsConfig.ServerName, tlsConfig.Fingerprint)
		}
	}

	// Generate MD5 hash of the key string
	hash := md5.Sum([]byte(keyStr))
	return fmt.Sprintf("%x", hash)
}

var (
	ClientConnIdleTimeout = 30 * time.Second
	ReadBufSize           = 2 * 1024
	WriteBufSize          = 2 * 1024
	ConnWindowSize        = 32 * 1024
)

func init() {
	timeout := os.Getenv("XRAY-GRPC-IDLE-TIMEOUT")
	if timeout != "" {
		d, err := time.ParseDuration(timeout)
		if err == nil && d > 0 {
			ClientConnIdleTimeout = d
		}
	}

	buf := os.Getenv("XRAY-GRPC-READ-BUF-SIZE")
	if buf != "" {
		i, err := strconv.Atoi(buf)
		if err == nil && i > 0 {
			ReadBufSize = i
		}
	}

	buf = os.Getenv("XRAY-GRPC-WRITE-BUF-SIZE")
	if buf != "" {
		i, err := strconv.Atoi(buf)
		if err == nil && i > 0 {
			WriteBufSize = i
		}
	}

	buf = os.Getenv("XRAY-GRPC-CONN-WINDOWS-SIZE")
	if buf != "" {
		i, err := strconv.Atoi(buf)
		if err == nil && i > 0 {
			ConnWindowSize = i
		}
	}
}

func dialgRPC(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (net.Conn, error) {
	grpcSettings := streamSettings.ProtocolSettings.(*Config)

	conn, err := getGrpcClient(ctx, dest, streamSettings)
	if err != nil {
		return nil, errors.New("Cannot dial gRPC").Base(err)
	}

	client := encoding.NewGRPCServiceClient(conn)
	if grpcSettings.MultiMode {
		errors.LogDebug(ctx, "using gRPC multi mode service name: `"+grpcSettings.getServiceName()+"` stream name: `"+grpcSettings.getTunMultiStreamName()+"`")
		grpcService, err := client.(encoding.GRPCServiceClientX).TunMultiCustomName(ctx, grpcSettings.getServiceName(), grpcSettings.getTunMultiStreamName())
		if err != nil {
			return nil, errors.New("Cannot dial gRPC").Base(err)
		}

		return encoding.NewMultiHunkConn(grpcService, nil), nil
	}

	errors.LogDebug(ctx, "using gRPC tun mode service name: `"+grpcSettings.getServiceName()+"` stream name: `"+grpcSettings.getTunStreamName()+"`")
	grpcService, err := client.(encoding.GRPCServiceClientX).TunCustomName(ctx, grpcSettings.getServiceName(), grpcSettings.getTunStreamName())
	if err != nil {
		return nil, errors.New("Cannot dial gRPC").Base(err)
	}

	return encoding.NewHunkConn(grpcService, nil), nil
}

func getGrpcClient(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (*grpc.ClientConn, error) {
	// Try to get store from features
	var st *store.Store
	if instance := core.FromContext(ctx); instance != nil {
		if s := instance.GetFeature(store.Type()); s != nil {
			st = s.(*store.Store)
		}
	}

	tlsConfig := tls.ConfigFromStreamSettings(streamSettings)
	realityConfig := reality.ConfigFromStreamSettings(streamSettings)
	sockopt := streamSettings.SocketSettings
	grpcSettings := streamSettings.ProtocolSettings.(*Config)

	// Generate cache key once for both Get and Put operations
	key := dialerConf{dest, streamSettings}.generateCacheKey()

	// If store is available, try to get stored connection
	if st != nil {
		if it, found := st.Get(key); found {
			if conn, ok := it.(*grpc.ClientConn); ok {
				state := conn.GetState()
				// Only reuse if connection is healthy
				if state != connectivity.Shutdown && state != connectivity.TransientFailure {
					return conn, nil
				}
				// Connection is unhealthy, will be replaced by new connection below
			}
		}
	}

	// Create new connection
	conn, err := createGrpcConnection(ctx, dest, tlsConfig, realityConfig, sockopt, grpcSettings)
	if err != nil {
		return nil, err
	}

	// Store connection if store is available
	if st != nil {
		st.Put(key, conn)
	}

	return conn, nil
}

// createGrpcConnection creates a new gRPC client connection
func createGrpcConnection(
	ctx context.Context,
	dest net.Destination,
	tlsConfig *tls.Config,
	realityConfig *reality.Config,
	sockopt *internet.SocketConfig,
	grpcSettings *Config,
) (*grpc.ClientConn, error) {

	dialOptions := []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  500 * time.Millisecond,
				Multiplier: 1.5,
				Jitter:     0.2,
				MaxDelay:   19 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithReadBufferSize(ReadBufSize),
		grpc.WithWriteBufferSize(WriteBufSize),
		grpc.WithInitialConnWindowSize(int32(ConnWindowSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second, // Send ping if no Activity within 10 seconds
			Timeout:             3 * time.Second,  // Timeout for waiting ping ack
			PermitWithoutStream: true,             // Send ping even without active streams
		}),
		grpc.WithContextDialer(func(gctx context.Context, s string) (net.Conn, error) {
			select {
			case <-gctx.Done():
				return nil, gctx.Err()
			default:
			}

			rawHost, rawPort, err := net.SplitHostPort(s)
			if err != nil {
				return nil, err
			}
			if len(rawPort) == 0 {
				rawPort = "443"
			}
			port, err := net.PortFromString(rawPort)
			if err != nil {
				return nil, err
			}
			address := net.ParseAddress(rawHost)

			gctx = c.ContextWithID(gctx, c.IDFromContext(ctx))
			gctx = session.ContextWithOutbounds(gctx, session.OutboundsFromContext(ctx))
			gctx = session.ContextWithTimeoutOnly(gctx, true)

			c, err := internet.DialSystem(gctx, net.TCPDestination(address, port), sockopt)
			if err == nil {
				if tlsConfig != nil {
					config := tlsConfig.GetTLSConfig()
					if config.ServerName == "" && address.Family().IsDomain() {
						config.ServerName = address.Domain()
					}
					if fingerprint := tls.GetFingerprint(tlsConfig.Fingerprint); fingerprint != nil {
						return tls.UClient(c, config, fingerprint), nil
					} else { // Fallback to normal gRPC TLS
						return tls.Client(c, config), nil
					}
				}
				if realityConfig != nil {
					return reality.UClient(c, realityConfig, gctx, dest)
				}
			}
			return c, err
		}),
	}

	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if ClientConnIdleTimeout > 0 {
		dialOptions = append(dialOptions, grpc.WithIdleTimeout(ClientConnIdleTimeout))
	}

	authority := ""
	if grpcSettings.Authority != "" {
		authority = grpcSettings.Authority
	} else if tlsConfig != nil && tlsConfig.ServerName != "" {
		authority = tlsConfig.ServerName
	} else if realityConfig == nil && dest.Address.Family().IsDomain() {
		authority = dest.Address.Domain()
	}
	dialOptions = append(dialOptions, grpc.WithAuthority(authority))

	if grpcSettings.IdleTimeout > 0 || grpcSettings.HealthCheckTimeout > 0 || grpcSettings.PermitWithoutStream {
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * time.Duration(grpcSettings.IdleTimeout),
			Timeout:             time.Second * time.Duration(grpcSettings.HealthCheckTimeout),
			PermitWithoutStream: grpcSettings.PermitWithoutStream,
		}))
	}

	if grpcSettings.InitialWindowsSize > 0 {
		dialOptions = append(dialOptions, grpc.WithInitialWindowSize(grpcSettings.InitialWindowsSize))
	}

	if grpcSettings.UserAgent != "" {
		dialOptions = append(dialOptions, grpc.WithUserAgent(grpcSettings.UserAgent))
	}

	var grpcDestHost string
	if dest.Address.Family().IsDomain() {
		grpcDestHost = dest.Address.Domain()
	} else {
		grpcDestHost = dest.Address.IP().String()
	}

	conn, err := grpc.Dial(net.JoinHostPort(grpcDestHost, dest.Port.String()),
		dialOptions...,
	)

	return conn, err
}

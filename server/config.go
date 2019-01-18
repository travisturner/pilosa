// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/toml"
	"github.com/pkg/errors"
	"github.com/uber/jaeger-client-go"
)

// TLSConfig contains TLS configuration
type TLSConfig struct {
	// CertificatePath contains the path to the certificate (.crt or .pem file)
	CertificatePath string `toml:"certificate"`
	// CertificateKeyPath contains the path to the certificate key (.key file)
	CertificateKeyPath string `toml:"key"`
	// SkipVerify disables verification for self-signed certificates
	SkipVerify bool `toml:"skip-verify"`
}

// Config represents the configuration for the command.
type Config struct {
	// DataDir is the directory where Pilosa stores both indexed data and
	// running state such as cluster topology information.
	DataDir string `toml:"data-dir"`

	// Bind is the host:port on which Pilosa will listen.
	Bind string `toml:"bind"` // TODO (2.0): Remove this as it has been deprecated.

	// ListenAddr is the address the server is listening on.
	ListenAddr string `toml:"listen-addr"`

	// AdvertiseAddr is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that Addr is listening on.
	AdvertiseAddr string `toml:"advertise-addr"`

	// MaxWritesPerRequest limits the number of mutating commands that can be in
	// a single request to the server. This includes Set, Clear,
	// SetRowAttrs & SetColumnAttrs.
	MaxWritesPerRequest int `toml:"max-writes-per-request"`

	// LogPath configures where Pilosa will write logs.
	LogPath string `toml:"log-path"`

	// Verbose toggles verbose logging which can be useful for debugging.
	Verbose bool `toml:"verbose"`

	// HTTP Handler options
	Handler struct {
		// CORS Allowed Origins
		AllowedOrigins []string `toml:"allowed-origins"`
	} `toml:"handler"`

	// TLS
	TLS TLSConfig `toml:"tls"`

	Cluster struct {
		// Disabled controls whether clustering functionality is enabled.
		Disabled      bool          `toml:"disabled"`
		Coordinator   bool          `toml:"coordinator"`
		ReplicaN      int           `toml:"replicas"`
		Hosts         []string      `toml:"hosts"`
		LongQueryTime toml.Duration `toml:"long-query-time"`
	} `toml:"cluster"`

	// Gossip config is based around memberlist.Config.
	Gossip gossip.Config `toml:"gossip"`

	Translation struct {
		MapSize int `toml:"map-size"`
		// DEPRECATED: Translation config supports translation store replication.
		PrimaryURL string `toml:"primary-url"`
	} `toml:"translation"`

	AntiEntropy struct {
		Interval toml.Duration `toml:"interval"`
	} `toml:"anti-entropy"`

	Metric struct {
		// Service can be statsd, expvar, or none.
		Service string `toml:"service"`
		// Host tells the statsd client where to write.
		Host         string        `toml:"host"`
		PollInterval toml.Duration `toml:"poll-interval"`
		// Diagnostics toggles sending some limited diagnostic information to
		// Pilosa's developers.
		Diagnostics bool `toml:"diagnostics"`
	} `toml:"metric"`

	Tracing struct {
		// SamplerType is the type of sampler to use.
		SamplerType string `toml:"sampler-type"`
		// SamplerParam is the parameter passed to the tracing sampler.
		// Its meaning is dependent on the type of sampler.
		SamplerParam float64 `toml:"sampler-param"`
		// AgentHostPort is the host:port of the local agent.
		AgentHostPort string `toml:"agent-host-port"`
	} `toml:"tracing"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		DataDir:    "~/.pilosa",
		ListenAddr: ":10101",
		// AdvertiseAddr: "",
		MaxWritesPerRequest: 5000,
		// LogPath: "",
		// Verbose: false,
		TLS: TLSConfig{},
	}

	// Cluster config.
	c.Cluster.Disabled = false
	// c.Cluster.Coordinator = false
	c.Cluster.ReplicaN = 1
	c.Cluster.Hosts = []string{}
	c.Cluster.LongQueryTime = toml.Duration(time.Minute)

	// Gossip config.
	c.Gossip.Port = "14000"
	// c.Gossip.Seeds = []string{}
	// c.Gossip.Key = ""
	c.Gossip.StreamTimeout = toml.Duration(10 * time.Second)
	c.Gossip.SuspicionMult = 4
	c.Gossip.PushPullInterval = toml.Duration(30 * time.Second)
	c.Gossip.ProbeInterval = toml.Duration(1 * time.Second)
	c.Gossip.ProbeTimeout = toml.Duration(500 * time.Millisecond)
	c.Gossip.Interval = toml.Duration(200 * time.Millisecond)
	c.Gossip.Nodes = 3
	c.Gossip.ToTheDeadTime = toml.Duration(30 * time.Second)

	// AntiEntropy config.
	c.AntiEntropy.Interval = toml.Duration(10 * time.Minute)

	// Metric config.
	c.Metric.Service = "none"
	// c.Metric.Host = ""
	c.Metric.PollInterval = toml.Duration(0 * time.Minute)
	c.Metric.Diagnostics = true

	// Tracing config.
	c.Tracing.SamplerType = jaeger.SamplerTypeRemote
	c.Tracing.SamplerParam = 0.001

	return c
}

// TODO: update the following to be pilosa-specific

// ValidateAddrs controls the address fields in the Config object
// and "fills in" the blanks:
// - the host part of Addr and HTTPAddr is resolved to an IP address
//   if specified (it stays blank if blank to mean "all addresses").
// - the host part of AdvertiseAddr is filled in if blank, either
//   from Addr if non-empty or os.Hostname(). It is also checked
//   for resolvability.
// - non-numeric port numbers are resolved to numeric.
//
// The addresses fields must be guaranteed by the caller to either be
// completely empty, or have both a host part and a port part
// separated by a colon. In the latter case either can be empty to
// indicate it's left unspecified.
func (cfg *Config) ValidateAddrs(ctx context.Context) error {
	// TODO (2.0): Deprecate Bind.
	if cfg.Bind != "" {
		cfg.ListenAddr = cfg.Bind
	}

	// Validate the advertise address.
	advScheme, advHost, advPort, err := validateAdvertiseAddr(ctx, cfg.AdvertiseAddr, cfg.ListenAddr)
	if err != nil {
		return errors.Wrapf(err, "validating advertise address")
	}
	cfg.AdvertiseAddr = schemeHostPortString(advScheme, advHost, advPort)

	// Validate the listen address.
	listenScheme, listenHost, listenPort, err := validateListenAddr(ctx, cfg.ListenAddr)
	if err != nil {
		return errors.Wrap(err, "validating listen address")
	}
	cfg.ListenAddr = schemeHostPortString(listenScheme, listenHost, listenPort)

	return nil
}

// validateAdvertiseAddr validates and normalizes an address suitable
// for use in gossiping - for use by other nodes. This ensures
// that if the "host" part is empty, it gets filled in with
// the configured listen address if any, or the canonical host name.
// Returns scheme, host, port as strings.
func validateAdvertiseAddr(ctx context.Context, advAddr, listenAddr string) (string, string, string, error) {
	listenScheme, listenHost, listenPort, err := getListenAddr(listenAddr)
	if err != nil {
		return "", "", "", errors.Wrap(err, "getting listen address")
	}

	advScheme, advHostPort := getScheme(advAddr)
	advHost, advPort := "", ""
	if advHostPort != "" {
		var err error
		advHost, advPort, err = net.SplitHostPort(advHostPort)
		if err != nil {
			return "", "", "", err
		}
	}
	// If no advertise scheme was specified, use the one from
	// the listen address.
	if advScheme == "" {
		advScheme = listenScheme
	}
	// If there was no port number, reuse the one from the listen
	// address.
	if advPort == "" || advPort == "0" {
		advPort = listenPort
	}
	// Resolve non-numeric to numeric.
	portNumber, err := net.DefaultResolver.LookupPort(ctx, "tcp", advPort)
	if err != nil {
		return "", "", "", err
	}
	advPort = strconv.Itoa(portNumber)

	// If the advertise host is empty, then we have two cases.
	if advHost == "" {
		if listenHost == "0.0.0.0" {
			advHost = getOutboundIP().String()
		} else {
			advHost = listenHost
		}
	}
	return advScheme, advHost, advPort, nil
}

// Get preferred outbound ip of this machine
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// validateListenAddr validates and normalizes an address suitable for
// use with net.Listen(). This accepts an empty "host" part as "listen
// on all interfaces" and resolves host names to IP addresses.
// Returns scheme, host, port as strings.
func validateListenAddr(ctx context.Context, addr string) (string, string, string, error) {
	scheme, host, port, err := getListenAddr(addr)
	if err != nil {
		return "", "", "", errors.Wrap(err, "getting listen address")
	}
	rHost, rPort, err := resolveAddr(ctx, host, port)
	if err != nil {
		return "", "", "", errors.Wrap(err, "resolving address")
	}
	return scheme, rHost, rPort, nil
}

// getScheme returns two strings: the scheme and the hostPort.
func getScheme(addr string) (string, string) {
	parts := strings.SplitN(addr, "://", 2)
	if len(parts) == 1 {
		return "", addr
	}
	return parts[0], parts[1]
}

func schemeHostPortString(scheme, host, port string) string {
	var addr string
	if scheme != "" {
		addr += fmt.Sprintf("%s://", scheme)
	}
	addr += net.JoinHostPort(host, port)
	return addr
}

// getListenAddr returns scheme, host, port as strings.
func getListenAddr(addr string) (string, string, string, error) {
	scheme, hostPort := getScheme(addr)
	host, port := "", ""
	if hostPort != "" {
		var err error
		host, port, err = net.SplitHostPort(hostPort)
		if err != nil {
			return "", "", "", err
		}
	}
	if port == "" {
		port = "10101"
	}
	return scheme, host, port, nil
}

// resolveAddr resolves non-numeric references to numeric references.
func resolveAddr(ctx context.Context, host, port string) (string, string, error) {
	resolver := net.DefaultResolver

	// Resolve the port number. This may translate service names
	// e.g. "postgresql" to a numeric value.
	portNumber, err := resolver.LookupPort(ctx, "tcp", port)
	if err != nil {
		return "", "", errors.Wrap(err, "looking up port")
	}
	port = strconv.Itoa(portNumber)

	// Resolve the address.
	if host == "" {
		// Keep empty. This means "listen on all addresses".
		return host, port, nil
	}

	addr, err := LookupAddr(ctx, resolver, host)
	if err != nil {
		return "", "", errors.Wrap(err, "looking up address")
	}
	return addr, port, nil
}

// LookupAddr resolves the given address/host to an IP address. If
// multiple addresses are resolved, it returns the first IPv4 address
// available if there is one, otherwise the first address.
func LookupAddr(ctx context.Context, resolver *net.Resolver, host string) (string, error) {
	// Resolve the IP address or hostname to an IP address.
	addrs, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", errors.Wrap(err, "looking up IP addresses")
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("cannot resolve %q to an address", host)
	}

	// TODO(knz): the remainder function can be changed to return all
	// resolved addresses once the server is taught to listen on
	// multiple interfaces. #5816

	// LookupIPAddr() can return a mix of IPv6 and IPv4
	// addresses. Return the first IPv4 address if possible.
	for _, addr := range addrs {
		if ip := addr.IP.To4(); ip != nil {
			return ip.String(), nil
		}
	}

	// No IPv4 address, return the first resolved address instead.
	return addrs[0].String(), nil
}

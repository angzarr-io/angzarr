// Package routing sends each REST request's gRPC call to the aggregate
// that serves the request's domain.
//
// The aggregate-scoped REST routes carry the domain in the path
// (`/v1/ch/{domain}/...`, `/v1/query/{domain}/...`). DomainAnnotator copies
// it into the outgoing gRPC metadata, and Router — a grpc.ClientConnInterface
// handed to the generated gateway clients — dials the aggregate for that
// domain: the target template with `{domain}` substituted (one Service per
// domain, e.g. `{domain}-aggregate:1310`), or a single fallback target.
package routing

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// DomainMetadataKey carries the request's domain from the HTTP path to the
// router.
const DomainMetadataKey = "x-angzarr-domain"

// DomainPlaceholder is replaced by the domain in a target template.
const DomainPlaceholder = "{domain}"

var (
	domainPath = regexp.MustCompile(`^/v1/(?:ch|query)/([^/]+)/`)
	// Framework domain grammar; anything else never reaches a DNS name.
	validDomain = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,63}$`)
)

// DomainFromPath returns the `{domain}` segment of an aggregate-scoped REST
// path.
func DomainFromPath(path string) (string, bool) {
	m := domainPath.FindStringSubmatch(path)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// DomainAnnotator is a runtime.WithMetadata annotator that forwards the
// path's domain to the router.
func DomainAnnotator(_ context.Context, r *http.Request) metadata.MD {
	if domain, ok := DomainFromPath(r.URL.Path); ok {
		return metadata.Pairs(DomainMetadataKey, domain)
	}
	return nil
}

// Dialer opens a client connection to a target.
type Dialer func(target string) (*grpc.ClientConn, error)

// Router dispatches each call to the aggregate serving the call's domain.
type Router struct {
	template string
	fallback string
	dial     Dialer

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewRouter routes by `template` (containing DomainPlaceholder) and/or a
// single `fallback` target; at least one must be set.
func NewRouter(template, fallback string, dial Dialer) (*Router, error) {
	if template == "" && fallback == "" {
		return nil, fmt.Errorf("routing: no aggregate target template or fallback target")
	}
	if template != "" && !strings.Contains(template, DomainPlaceholder) {
		return nil, fmt.Errorf("routing: target template %q lacks %s", template, DomainPlaceholder)
	}
	return &Router{
		template: template,
		fallback: fallback,
		dial:     dial,
		conns:    make(map[string]*grpc.ClientConn),
	}, nil
}

// TargetFor returns the gRPC target serving `domain` ("" = no domain).
func (r *Router) TargetFor(domain string) (string, error) {
	if domain != "" && !validDomain.MatchString(domain) {
		return "", status.Errorf(codes.InvalidArgument, "invalid domain %q", domain)
	}
	if domain != "" && r.template != "" {
		return strings.ReplaceAll(r.template, DomainPlaceholder, domain), nil
	}
	if r.fallback != "" {
		return r.fallback, nil
	}
	return "", status.Error(codes.InvalidArgument,
		"request names no domain and the gateway has no single aggregate target")
}

func (r *Router) connFor(ctx context.Context) (*grpc.ClientConn, error) {
	domain := ""
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if v := md.Get(DomainMetadataKey); len(v) > 0 {
			domain = v[0]
		}
	}
	target, err := r.TargetFor(domain)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if cc, ok := r.conns[target]; ok {
		return cc, nil
	}
	cc, err := r.dial(target)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial %s: %v", target, err)
	}
	r.conns[target] = cc
	return cc, nil
}

// Invoke implements grpc.ClientConnInterface.
func (r *Router) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	cc, err := r.connFor(ctx)
	if err != nil {
		return err
	}
	return cc.Invoke(ctx, method, args, reply, opts...)
}

// NewStream implements grpc.ClientConnInterface.
func (r *Router) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	cc, err := r.connFor(ctx)
	if err != nil {
		return nil, err
	}
	return cc.NewStream(ctx, desc, method, opts...)
}

// Close closes every connection the router opened.
func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for target, cc := range r.conns {
		_ = cc.Close()
		delete(r.conns, target)
	}
}

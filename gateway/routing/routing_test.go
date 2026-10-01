package routing

import (
	"context"
	"net"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	angzarrv1 "github.com/angzarr-io/angzarr/gateway/gen/io/angzarr/v1"
)

func TestDomainFromPath(t *testing.T) {
	tests := []struct {
		path   string
		domain string
		ok     bool
	}{
		{"/v1/ch/order/commands", "order", true},
		{"/v1/ch/order/commands/speculative", "order", true},
		{"/v1/query/payment/events", "payment", true},
		{"/v1/query/payment/events/stream", "payment", true},
		{"/v1/query/aggregates", "", false},
		{"/api/dlq", "", false},
		{"/v1/ch/order", "", false},
	}
	for _, tt := range tests {
		domain, ok := DomainFromPath(tt.path)
		if domain != tt.domain || ok != tt.ok {
			t.Errorf("DomainFromPath(%q) = (%q, %v), want (%q, %v)", tt.path, domain, ok, tt.domain, tt.ok)
		}
	}
}

func TestDomainAnnotatorForwardsPathDomain(t *testing.T) {
	md := DomainAnnotator(context.Background(), httptest.NewRequest("POST", "/v1/ch/order/commands", nil))
	if got := md.Get(DomainMetadataKey); len(got) != 1 || got[0] != "order" {
		t.Fatalf("metadata = %v, want order", md)
	}
	if md := DomainAnnotator(context.Background(), httptest.NewRequest("GET", "/health", nil)); md != nil {
		t.Fatalf("non-domain path produced metadata %v", md)
	}
}

func TestNewRouterRequiresATarget(t *testing.T) {
	if _, err := NewRouter("", "", nil); err == nil {
		t.Fatal("router without template or fallback must be rejected")
	}
	if _, err := NewRouter("aggregate:1310", "", nil); err == nil {
		t.Fatal("template without {domain} must be rejected")
	}
}

func TestTargetForSubstitutesDomain(t *testing.T) {
	r, err := NewRouter("{domain}-aggregate.ns.svc.cluster.local:1310", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.TargetFor("order")
	if err != nil || got != "order-aggregate.ns.svc.cluster.local:1310" {
		t.Fatalf("TargetFor(order) = %q, %v", got, err)
	}
}

func TestTargetForRejectsDomainOutsideGrammar(t *testing.T) {
	r, _ := NewRouter("{domain}-aggregate:1310", "", nil)
	for _, bad := range []string{"evil.example.com:443/x", "Order", "a b", "x/y"} {
		if _, err := r.TargetFor(bad); status.Code(err) != codes.InvalidArgument {
			t.Errorf("TargetFor(%q) error = %v, want InvalidArgument", bad, err)
		}
	}
}

func TestTargetForFallback(t *testing.T) {
	single, _ := NewRouter("", "aggregate:1310", nil)
	if got, _ := single.TargetFor("order"); got != "aggregate:1310" {
		t.Fatalf("single-target router sent order to %q", got)
	}
	templated, _ := NewRouter("{domain}-aggregate:1310", "", nil)
	if _, err := templated.TargetFor(""); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("domain-less call without fallback: %v", err)
	}
}

// eventQuery answers GetEventBook with a book naming the server's domain.
type eventQuery struct {
	angzarrv1.UnimplementedEventQueryServiceServer
	domain string
}

func (s eventQuery) GetEventBook(context.Context, *angzarrv1.Query) (*angzarrv1.EventBook, error) {
	return &angzarrv1.EventBook{Cover: &angzarrv1.Cover{Domain: s.domain}}, nil
}

// fakeAggregates serves one in-memory EventQueryService per domain and
// returns a Dialer that reaches them by `{domain}-aggregate` target.
func fakeAggregates(t *testing.T, domains ...string) (Dialer, *int) {
	t.Helper()
	listeners := map[string]*bufconn.Listener{}
	for _, d := range domains {
		lis := bufconn.Listen(1 << 20)
		srv := grpc.NewServer()
		angzarrv1.RegisterEventQueryServiceServer(srv, eventQuery{domain: d})
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)
		listeners[d+"-aggregate"] = lis
	}
	dials := 0
	return func(target string) (*grpc.ClientConn, error) {
		dials++
		lis, ok := listeners[target]
		if !ok {
			t.Fatalf("dialed unknown target %q", target)
		}
		return grpc.NewClient("passthrough:///"+target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	}, &dials
}

func TestRouterSendsEachCallToItsDomainsAggregate(t *testing.T) {
	dial, dials := fakeAggregates(t, "order", "payment")
	r, err := NewRouter("{domain}-aggregate", "", dial)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	client := angzarrv1.NewEventQueryServiceClient(r)

	for _, domain := range []string{"order", "payment", "order"} {
		ctx := metadata.AppendToOutgoingContext(context.Background(), DomainMetadataKey, domain)
		book, err := client.GetEventBook(ctx, &angzarrv1.Query{})
		if err != nil {
			t.Fatalf("%s: %v", domain, err)
		}
		if book.GetCover().GetDomain() != domain {
			t.Fatalf("call for %s answered by %s", domain, book.GetCover().GetDomain())
		}
	}
	if *dials != 2 {
		t.Fatalf("dialed %d times, want one connection per domain", *dials)
	}
}

// A call carrying metadata but no domain uses the single fallback target.
func TestRouterWithoutDomainUsesFallback(t *testing.T) {
	dial, _ := fakeAggregates(t, "order")
	r, err := NewRouter("", "order-aggregate", dial)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-correlation-id", "c-1")

	book, err := angzarrv1.NewEventQueryServiceClient(r).GetEventBook(ctx, &angzarrv1.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if book.GetCover().GetDomain() != "order" {
		t.Fatalf("answered by %q", book.GetCover().GetDomain())
	}
}

// Streaming calls are routed the same way; an invalid domain fails before
// any connection is opened.
func TestRouterRejectsInvalidDomainOnStream(t *testing.T) {
	dial, dials := fakeAggregates(t, "order")
	r, _ := NewRouter("{domain}-aggregate", "", dial)
	defer r.Close()
	ctx := metadata.AppendToOutgoingContext(context.Background(), DomainMetadataKey, "../etc")

	stream, err := angzarrv1.NewEventQueryServiceClient(r).GetEvents(ctx, &angzarrv1.Query{})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("stream error = %v, want InvalidArgument", err)
	}
	if *dials != 0 {
		t.Fatalf("dialed %d times for an invalid domain", *dials)
	}
}

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	angzarrv1 "github.com/angzarr-io/angzarr/gateway/gen/io/angzarr/v1"
	"github.com/angzarr-io/angzarr/gateway/routing"
)

// eventQuery answers GetEventBook with a book naming the serving domain.
type eventQuery struct {
	angzarrv1.UnimplementedEventQueryServiceServer
	domain string
}

func (s eventQuery) GetEventBook(context.Context, *angzarrv1.Query) (*angzarrv1.EventBook, error) {
	return &angzarrv1.EventBook{Cover: &angzarrv1.Cover{Domain: s.domain}}, nil
}

func aggregateRouter(t *testing.T, domains ...string) *routing.Router {
	t.Helper()
	listeners := map[string]*bufconn.Listener{}
	for _, d := range domains {
		lis := bufconn.Listen(1 << 20)
		srv := grpc.NewServer()
		angzarrv1.RegisterEventQueryServiceServer(srv, eventQuery{domain: d})
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)
		listeners[d+"-aggregate:1310"] = lis
	}
	r, err := routing.NewRouter("{domain}-aggregate:1310", "", func(target string) (*grpc.ClientConn, error) {
		lis := listeners[target]
		return grpc.NewClient("passthrough:///"+target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

// A REST call for /v1/query/{domain} reaches that domain's aggregate, not
// whichever single aggregate the gateway was pointed at.
func TestQueryRoutesToPathDomainAggregate(t *testing.T) {
	mux, err := buildGatewayMux(context.Background(), aggregateRouter(t, "order", "payment"), nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, domain := range []string{"order", "payment"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/query/"+domain+"/events", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d body %s", domain, rec.Code, rec.Body)
		}
		var book struct {
			Cover struct {
				Domain string `json:"domain"`
			} `json:"cover"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &book); err != nil {
			t.Fatal(err)
		}
		if book.Cover.Domain != domain {
			t.Fatalf("GET /v1/query/%s answered by %q", domain, book.Cover.Domain)
		}
	}
}

// Without a status target the DLQ admin routes are not registered rather
// than proxied to an aggregate that does not serve them.
func TestDlqRoutesAbsentWithoutStatusTarget(t *testing.T) {
	mux, err := buildGatewayMux(context.Background(), aggregateRouter(t, "order"), nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/dlq", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /api/dlq = %d, want 404", rec.Code)
	}
}

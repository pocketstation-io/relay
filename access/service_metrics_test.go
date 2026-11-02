package access

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/storage/memory"
)

func TestGivenSharedStorageWhenRestartDeleteAndExpiryThenMetricsCountCommittedLiveSessions(t *testing.T) {
	now := time.Now()
	backend := memory.NewWithClock(func() time.Time { return now })
	defer backend.Close()
	first, err := NewService([]byte("0123456789abcdef0123456789abcdef"), backend, Config{SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	created, err := first.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewService([]byte("0123456789abcdef0123456789abcdef"), backend, Config{SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	check := func(service *Service, expected string) {
		t.Helper()
		var output bytes.Buffer
		if err := service.WriteMetricsContext(context.Background(), &output); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "pocketstation_control_plane_sessions_active "+expected+"\n") {
			t.Fatal(output.String())
		}
	}
	check(first, "1")
	check(second, "1")
	if _, err := second.DeleteContext(context.Background(), created.ID()); err != nil {
		t.Fatal(err)
	}
	check(first, "0")
	if _, err := second.Create("application"); err != nil {
		t.Fatal(err)
	}
	check(first, "1")
	now = now.Add(2 * time.Hour)
	check(first, "0")
	check(second, "0")
}

func TestGivenUnavailableStorageWhenScrapingMetricsThenNoPartialSuccessIsWritten(t *testing.T) {
	backend := memory.New()
	service, err := NewService([]byte("0123456789abcdef0123456789abcdef"), backend, Config{})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.WriteMetricsContext(ctx, &output); !errors.Is(err, context.Canceled) || output.Len() != 0 {
		t.Fatalf("cancellation: %v, output=%q", err, output.String())
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := service.WriteMetrics(&output); !errors.Is(err, storage.ErrClosed) || output.Len() != 0 {
		t.Fatalf("closed: %v, output=%q", err, output.String())
	}
}

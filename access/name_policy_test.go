package access

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pocketstation-io/relay/access/names"
	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/storage/memory"
	"github.com/pocketstation-io/relay/access/storage/sqlite"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestGivenNamePolicyConfigurationWhenParsedThenOnlySupportedLengthsAreAccepted(t *testing.T) {
	for raw, want := range map[string]NameAllocationPolicy{"": AutoTwoThenThree, "auto": AutoTwoThenThree, "2": FixedTwo, "3": FixedThree} {
		got, e := ParseNameAllocationPolicy(func(string) string { return raw })
		if e != nil || got != want {
			t.Fatalf("policy %q: %s %v", raw, got, e)
		}
	}
	for _, raw := range []string{"1", "16", "two", "AUTO", " 2 ", "02", "2.0", "true"} {
		if _, e := ParseNameAllocationPolicy(func(string) string { return raw }); e == nil {
			t.Fatalf("invalid policy accepted: %q", raw)
		}
	}
	if _, e := NewService(sessionTestSecret, memory.New(), Config{NamePolicy: "16"}); e == nil {
		t.Fatal("invalid library policy accepted")
	}
	for _, raw := range []string{"9223372036854775807", "9223372037", "-1", "0"} {
		if _, e := ParseSessionExpiryDuration(func(string) string { return raw }); e == nil {
			t.Fatalf("overflow or invalid duration accepted: %s", raw)
		}
	}
	if _, e := ParseSessionExpiryDuration(func(string) string { return "9223372036" }); e != nil {
		t.Fatal(e)
	}
}

type pressureStore struct {
	storage.Store
	probes, puts, collisions int
	counts                   []int
	err                      error
	idConflict               bool
}
type pressureTx struct {
	storage.Tx
	parent *pressureStore
}

func (s *pressureStore) Update(ctx context.Context, fn func(storage.Tx) error) error {
	return s.Store.Update(ctx, func(tx storage.Tx) error { return fn(&pressureTx{tx, s}) })
}
func (tx *pressureTx) Grant(locator string) (storage.GrantRecord, bool, error) {
	// Model another reservation already owning each newly proposed name. The
	// adapter owns collision simulation, not a product bypass/random override.
	if _, e := names.ParseStored(names.GrammarName, locator); e == nil {
		tx.parent.probes++
		tx.parent.counts = append(tx.parent.counts, len(strings.Split(locator, "-")))
		if tx.parent.err != nil {
			return storage.GrantRecord{}, false, tx.parent.err
		}
		if tx.parent.probes <= tx.parent.collisions {
			return storage.GrantRecord{Alias: locator}, true, nil
		}
	}
	return tx.Tx.Grant(locator)
}
func (tx *pressureTx) PutGrant(r storage.GrantRecord) error {
	tx.parent.puts++
	if tx.parent.idConflict {
		return storage.ErrConflict
	}
	return tx.Tx.PutGrant(r)
}
func TestGivenVerifiedShortNamePressureWhenAllocatingThenAutoFallsBackWithinOneBudget(t *testing.T) {
	for _, policy := range []NameAllocationPolicy{AutoTwoThenThree, FixedTwo, FixedThree} {
		backend := &pressureStore{Store: memory.New(), collisions: 8}
		service, e := NewService(sessionTestSecret, backend, Config{NamePolicy: policy})
		if e != nil {
			t.Fatal(e)
		}
		session := readySession(t, service)
		grant, e := service.CreateInvitation(session.ID(), "application")
		if e != nil {
			t.Fatal(e)
		}
		expected := 2
		if policy != FixedTwo {
			expected = 3
		}
		if grant.WordCount != expected || backend.probes != 9 || backend.puts != 1 {
			t.Fatalf("policy=%s count=%d probes=%d puts=%d", policy, grant.WordCount, backend.probes, backend.puts)
		}
		for i, count := range backend.counts {
			want := 2
			if policy == FixedThree || (policy == AutoTwoThenThree && i >= 8) {
				want = 3
			}
			if count != want {
				t.Fatal("fallback occurred before eight verified collisions")
			}
		}
	}
}
func TestGivenUnrelatedStoreFailureWhenAllocatingThenItDoesNotTriggerLongerNames(t *testing.T) {
	backend := &pressureStore{Store: memory.New()}
	service, e := NewService(sessionTestSecret, backend, Config{})
	if e != nil {
		t.Fatal(e)
	}
	session := readySession(t, service)
	backend.err = errors.New("store unavailable")
	if _, e := service.CreateInvitation(session.ID(), "application"); !errors.Is(e, backend.err) || backend.probes != 1 || backend.puts != 0 {
		t.Fatal("backend failure retried or hidden")
	}
	backend.err = nil
	backend.probes = 0
	backend.counts = nil
	backend.idConflict = true
	if _, e := service.CreateInvitation(session.ID(), "application"); !errors.Is(e, ErrNameAllocationUnavailable) || backend.puts != 32 {
		t.Fatal("ID collision exceeded budget")
	}
	for _, count := range backend.counts {
		if count != 2 {
			t.Fatal("unrelated ID collision triggered long names")
		}
	}
}
func TestGivenPersistentNamePressureWhenBudgetEndsThenNoGrantIsInserted(t *testing.T) {
	backend := &pressureStore{Store: memory.New(), collisions: 100}
	s, e := NewService(sessionTestSecret, backend, Config{})
	if e != nil {
		t.Fatal(e)
	}
	session := readySession(t, s)
	if _, e = s.CreateInvitation(session.ID(), "application"); !errors.Is(e, ErrNameAllocationUnavailable) || backend.probes != 32 || backend.puts != 0 {
		t.Fatalf("unbounded pressure handling: probes=%d err=%v", backend.probes, e)
	}
	for i, count := range backend.counts {
		want := 2
		if i >= 8 {
			want = 3
		}
		if count != want {
			t.Fatal("wrong bounded allocation policy")
		}
	}
}
func TestGivenHTTPWordCountWhenCreatingThenStrictSelectorsAndLegacyFormatsAgree(t *testing.T) {
	s, session, mux := readyInvitationTestServer(t)
	for _, test := range []struct {
		body          string
		status, count int
	}{
		{`{"bus_id":"application"}`, 201, 2}, {`{"bus_id":"application","word_count":2}`, 201, 2}, {`{"bus_id":"application","word_count":3}`, 201, 3},
		{`{"bus_id":"application","visibility":"public"}`, 201, 2}, {`{"bus_id":"application","visibility":"private"}`, 201, 3},
		{`{"bus_id":"application","visibility":"private","word_count":3}`, 400, 0}, {`{"bus_id":"application","visibility":"private","word_count":2}`, 400, 0},
		{`{"bus_id":"application","word_count":null}`, 400, 0}, {`{"bus_id":"application","word_count":true}`, 400, 0}, {`{"bus_id":"application","word_count":"2"}`, 400, 0}, {`{"bus_id":"application","word_count":2.0}`, 400, 0}, {`{"bus_id":"application","word_count":1}`, 400, 0}, {`{"bus_id":"application","word_count":16}`, 400, 0},
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+session.ID()+"/invitations", bytes.NewBufferString(test.body))
		req.Header.Set("Authorization", "Bearer "+session.SourceToken())
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, req)
		if res.Code != test.status {
			t.Fatalf("request %s status=%d body=%s", test.body, res.Code, res.Body.String())
		}
		if test.status == 201 {
			var body createInvitationResponse
			if e := json.Unmarshal(res.Body.Bytes(), &body); e != nil {
				t.Fatal(e)
			}
			if body.WordCount != test.count || len(strings.Split(body.ShareAlias, "-")) != test.count {
				t.Fatal("response length differs from allocated format")
			}
			if _, e := s.ResolveInvitationContext(context.Background(), body.ShareAlias, ""); e == nil {
				t.Fatal("word count changed authority")
			}
		}
	}
}

func TestGivenStoredPreviousGrammarWhenLoadedThenExistingLinksRetainTheirRecordedMeaning(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var backend storage.Store = memory.New()
			if kind == "sqlite" {
				var err error
				backend, err = sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "stored.db"))
				if err != nil {
					t.Fatal(err)
				}
			}
			defer backend.Close()
			s, err := NewService(sessionTestSecret, backend, Config{})
			if err != nil {
				t.Fatal(err)
			}
			session := readySession(t, s)
			for _, old := range []struct{ grammar, text string }{{"natural-words-1", "mountain-silly-lemon"}, {"en-scene-v1", "amberaura-amberbadger"}} {
				grant, err := s.CreateInvitation(session.ID(), "application")
				if err != nil {
					t.Fatal(err)
				}
				err = backend.Update(context.Background(), func(tx storage.Tx) error {
					record, found, err := tx.Grant(grant.Code)
					if err != nil {
						return err
					}
					if !found {
						return errors.New("missing prepared grant")
					}
					if err := tx.DeleteGrant(record.ID); err != nil {
						return err
					}
					record.Alias = old.text
					record.GrammarName = old.grammar
					record.WordCount = len(strings.Split(old.text, "-"))
					return tx.PutGrant(record)
				})
				if err != nil {
					t.Fatal(err)
				}
				metadata, err := s.InspectInvitationContext(context.Background(), old.text)
				if err != nil || metadata.Alias != old.text {
					t.Fatalf("old link inspection: %v", err)
				}
				if _, err = s.ResolveInvitationContext(context.Background(), old.text, ""); err == nil {
					t.Fatal("legacy name became authority")
				}
				if _, err = s.ResolveInvitationContext(context.Background(), old.text, grant.Code); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestGivenEverySupportedFixedLengthWhenAllocatingThenFormatIsExplicitAndAuthorityUnchanged(t *testing.T) {
	for count := 2; count <= 15; count++ {
		policy, e := FixedNamePolicy(count)
		if e != nil {
			t.Fatal(e)
		}
		parsed, e := ParseNameAllocationPolicy(func(string) string { return string(policy) })
		if e != nil || parsed != policy {
			t.Fatal("fixed configuration mismatch")
		}
		store, session, mux := readyInvitationTestServer(t)
		store.config.NamePolicy = policy
		// Omission uses the configured fixed length; every explicit requested length
		// is independently accepted, not coerced to a legacy visibility length.
		for _, body := range []string{`{"bus_id":"application"}`, fmt.Sprintf(`{"bus_id":"application","word_count":%d}`, count)} {
			req := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+session.ID()+"/invitations", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+session.SourceToken())
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != 201 {
				t.Fatalf("count%d HTTP%d", count, rec.Code)
			}
			var created createInvitationResponse
			if e = json.Unmarshal(rec.Body.Bytes(), &created); e != nil {
				t.Fatal(e)
			}
			if created.WordCount != count || len(strings.Split(created.ShareAlias, "-")) != count {
				t.Fatal("fixed count changed")
			}
			if count >= 3 && created.Visibility != InvitationVisibilityPrivate {
				t.Fatal("compatibility label changed")
			}
			preview := httptest.NewRecorder()
			mux.ServeHTTP(preview, httptest.NewRequest(http.MethodGet, "/v1/invitations/"+created.ShareAlias, nil))
			var metadata inspectInvitationResponse
			if e = json.Unmarshal(preview.Body.Bytes(), &metadata); e != nil || metadata.WordCount != count {
				t.Fatal("metadata lost explicit count")
			}
			if _, e = store.ResolveInvitationContext(context.Background(), created.ShareAlias, ""); e == nil {
				t.Fatal("longer words became authority")
			}
			redemption := httptest.NewRecorder()
			mux.ServeHTTP(redemption, httptest.NewRequest(http.MethodPost, "/v1/join/"+created.ShareAlias, strings.NewReader(fmt.Sprintf(`{"join_code":%q}`, created.JoinCode))))
			if redemption.Code != 200 {
				t.Fatalf("long join rejected count%d status%d", count, redemption.Code)
			}
		}
	}
	for _, count := range []int{0, 1, 16} {
		if _, e := FixedNamePolicy(count); e == nil {
			t.Fatal("invalid fixed constructor accepted")
		}
	}
}

func TestGivenFixedFifteenWordsWhenCollisionsPersistThenTheCountNeverExpands(t *testing.T) {
	backend := &allNamePressureStore{Store: memory.New()}
	s, e := NewService(sessionTestSecret, backend, Config{NamePolicy: "15"})
	if e != nil {
		t.Fatal(e)
	}
	session := readySession(t, s)
	if _, e = s.CreateInvitation(session.ID(), "application"); !errors.Is(e, ErrNameAllocationUnavailable) || len(backend.counts) != 32 {
		t.Fatalf("fixed budget: %v attempts%d", e, len(backend.counts))
	}
	for _, count := range backend.counts {
		if count != 15 {
			t.Fatal("fixed length expanded or shortened")
		}
	}
}

type allNamePressureStore struct {
	storage.Store
	counts []int
}
type allNamePressureTx struct {
	storage.Tx
	owner *allNamePressureStore
}

func (s *allNamePressureStore) Update(ctx context.Context, fn func(storage.Tx) error) error {
	return s.Store.Update(ctx, func(tx storage.Tx) error { return fn(&allNamePressureTx{tx, s}) })
}
func (tx *allNamePressureTx) Grant(locator string) (storage.GrantRecord, bool, error) {
	if name, e := names.Parse(locator); e == nil {
		tx.owner.counts = append(tx.owner.counts, name.WordCount)
		return storage.GrantRecord{Alias: locator}, true, nil
	}
	return tx.Tx.Grant(locator)
}

func TestGivenFifteenWordSQLiteGrantWhenRestartedThenJoinAndBoundedRetrySurvive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fifteen.db")
	backend, e := sqlite.Open(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	service, e := NewService(sessionTestSecret, backend, Config{NamePolicy: "15"})
	if e != nil {
		t.Fatal(e)
	}
	session := readySession(t, service)
	grant, e := service.CreateInvitation(session.ID(), "application")
	if e != nil {
		t.Fatal(e)
	}
	if e = backend.Close(); e != nil {
		t.Fatal(e)
	}
	backend, e = sqlite.Open(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	defer backend.Close()
	service, e = NewService(sessionTestSecret, backend, Config{NamePolicy: "15"})
	if e != nil {
		t.Fatal(e)
	}
	metadata, e := service.InspectInvitationContext(context.Background(), grant.Alias)
	if e != nil || metadata.WordCount != 15 {
		t.Fatal("restart changed long name")
	}
	first, e := service.redeem(context.Background(), grant.Alias, grant.Code, "fifteen-word-retry-key", encodeResolved)
	if e != nil {
		t.Fatal(e)
	}
	second, e := service.redeem(context.Background(), grant.Alias, grant.Code, "fifteen-word-retry-key", encodeResolved)
	if e != nil || !bytes.Equal(first, second) {
		t.Fatal("long name receipt changed")
	}
	if _, e = service.redeem(context.Background(), grant.Alias, "", "fifteen-word-retry-key", encodeResolved); e == nil {
		t.Fatal("retry key or words became authority")
	}
}

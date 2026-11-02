package access

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pocketstation-io/relay/access/names"
	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/token"
)

const invitationTTL = 15 * time.Minute

var (
	ErrInvalidWordCount            = errors.New("word_count must be an integer from 2 through 15 and cannot accompany legacy visibility")
	ErrInvalidInvitationVisibility = errors.New("invalid invitation name format")
	ErrNameAllocationUnavailable   = errors.New("name allocation retry budget exhausted")
	ErrRetryConflict               = errors.New("retry does not match the original authorized request")
	ErrReceiptCapacity             = errors.New("retry receipt capacity reached")
)

// InvitationVisibility is retained as a deprecated wire-format selector.
// Both formats require the same opaque join credential.
// Deprecated: visibility chooses two or three readable words, never permission.
type InvitationVisibility string

const (
	InvitationVisibilityPublic  InvitationVisibility = "public"
	InvitationVisibilityPrivate InvitationVisibility = "private"
)

type InvitationOptions struct {
	// Visibility is a deprecated format selector; omitted uses NamePolicy.
	Visibility InvitationVisibility
	// WordCount is 2 through 15; zero uses the configured policy.
	WordCount int
}

type Invitation struct {
	WordCount  int
	Code       string
	Alias      string
	Visibility InvitationVisibility
	ExpiresAt  time.Time
}
type InvitationMetadata struct {
	WordCount  int
	Alias      string
	Visibility InvitationVisibility
	ExpiresAt  time.Time
}
type ResolvedInvitation struct {
	SessionID       string
	BusID           string
	SubscriberToken string
}

func (s *Service) CreateInvitation(id, bus string) (Invitation, error) {
	return s.CreateInvitationContext(context.Background(), id, bus, "")
}

// CreateInvitationContext retains explicit legacy format selection. An omitted
// selection uses the configured naming policy; both formats require a join code.
func (s *Service) CreateInvitationContext(ctx context.Context, id, bus string, visibility InvitationVisibility) (Invitation, error) {
	return s.CreateInvitationWithOptionsContext(ctx, id, bus, InvitationOptions{Visibility: visibility})
}
func (s *Service) CreateInvitationWithOptionsContext(ctx context.Context, id, bus string, options InvitationOptions) (Invitation, error) {
	policy := s.config.NamePolicy
	words := 0
	switch options.Visibility {
	case "":
	case InvitationVisibilityPublic:
		words = 2
	case InvitationVisibilityPrivate:
		words = 3
	default:
		return Invitation{}, ErrInvalidInvitationVisibility
	}
	if options.WordCount != 0 {
		if (options.WordCount < names.MinWordCount || options.WordCount > names.MaxWordCount) || words != 0 {
			return Invitation{}, ErrInvalidWordCount
		}
		words = options.WordCount
	}
	if words != 0 {
		policy, _ = FixedNamePolicy(words)
	}
	var result Invitation
	err := s.update(ctx, func(tx storage.Tx) error {
		r, err := s.active(tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return os.ErrNotExist
		}
		if err != nil {
			return err
		}
		if !s.busReady(r, bus, tx.Now()) {
			return ErrSessionNotReady
		}
		counts, err := tx.Counts()
		if err != nil {
			return err
		}
		if counts.GrantCount >= s.config.MaxInvitations {
			return ErrInvitationCapacity
		}
		shortCollisions := 0
		for attempt := 0; attempt < 32; attempt++ {
			if fixed, ok := policy.fixedCount(); ok {
				words = fixed
			} else {
				words = 2
				if shortCollisions >= 8 {
					words = 3
				}
			}
			visibility := InvitationVisibilityPublic
			if words >= 3 {
				visibility = InvitationVisibilityPrivate
			}
			name, err := names.Generate(rand.Reader, words)
			if err != nil {
				return err
			}
			if _, occupied, lookupErr := tx.Grant(name.Text); lookupErr != nil {
				return lookupErr
			} else if occupied {
				if words == 2 {
					shortCollisions++
				}
				continue
			}
			code, err := randomSessionID()
			if err != nil {
				return err
			}
			grantID, err := randomSessionID()
			if err != nil {
				return err
			}
			expires := tx.Now().Add(invitationTTL)
			if r.ExpiresAt.Before(expires) {
				expires = r.ExpiresAt
			}
			grant := storage.GrantRecord{SchemaVersion: storage.SchemaVersion, ID: grantID, JoinCode: code, Alias: name.Text, GrammarName: name.GrammarName, WordCount: words, Visibility: string(visibility), SessionID: id, BusID: bus, CreatedAt: tx.Now(), ExpiresAt: expires}
			if err := tx.PutGrant(grant); errors.Is(err, storage.ErrConflict) {
				// A code/ID conflict consumes budget but cannot trigger longer names.
				if _, occupied, lookupErr := tx.Grant(name.Text); lookupErr != nil {
					return lookupErr
				} else if occupied && words == 2 {
					shortCollisions++
				}
				continue
			} else if err != nil {
				return err
			}
			result = Invitation{WordCount: words, Code: code, Alias: name.Text, Visibility: visibility, ExpiresAt: expires}
			return nil
		}
		return ErrNameAllocationUnavailable
	})
	if err != nil {
		return Invitation{}, err
	}
	return result, nil
}
func (s *Service) busReady(r storage.SessionRecord, bus string, now time.Time) bool {
	if r.WriterTerm != s.term || now.Sub(r.Relay.ObservedAt) > s.config.ObservationTTL {
		return false
	}
	if bus == "mix" {
		return Session{record: r}.State().Ready
	}
	if !contains(r.RequiredBuses, bus) {
		return false
	}
	for _, b := range r.Relay.Buses {
		if b.BusID == bus {
			return b.SourceActive
		}
	}
	return false
}
func (s *Service) InspectInvitationContext(ctx context.Context, locator string) (InvitationMetadata, error) {
	if locator == "" || len(locator) > names.MaxNameBytes {
		return InvitationMetadata{}, os.ErrNotExist
	}
	var result InvitationMetadata
	err := s.view(ctx, func(tx storage.Tx) error {
		if err := s.checkWriter(tx); err != nil {
			return err
		}
		r, found, err := tx.Grant(locator)
		if err != nil {
			return err
		}
		if !found || !r.ExpiresAt.After(tx.Now()) {
			return os.ErrNotExist
		}
		if _, err := s.active(tx, r.SessionID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return os.ErrNotExist
			}
			return err
		}
		if _, err := names.ParseStored(r.GrammarName, r.Alias); err != nil {
			return errors.New("stored readable name grammar is invalid")
		}
		result = InvitationMetadata{WordCount: r.WordCount, Alias: r.Alias, Visibility: InvitationVisibility(r.Visibility), ExpiresAt: r.ExpiresAt}
		return nil
	})
	return result, err
}

func (s *Service) ResolveInvitationContext(ctx context.Context, locator, code string) (ResolvedInvitation, error) {
	var result ResolvedInvitation
	_, err := s.redeem(ctx, locator, code, "", func(r ResolvedInvitation) ([]byte, error) { result = r; return json.Marshal(r) })
	if err != nil {
		return ResolvedInvitation{}, err
	}
	return result, nil
}

func (s *Service) redeem(ctx context.Context, locator, code, key string, encode func(ResolvedInvitation) ([]byte, error)) ([]byte, error) {
	if code == "" && validOpaqueInvitationCode(locator) {
		code = locator
	}
	if !validOpaqueInvitationCode(code) || len(locator) > names.MaxNameBytes || locator == "" {
		return nil, os.ErrNotExist
	}
	if key != "" && (len(key) < 16 || len(key) > 128 || !validIdentifier(key, 128)) {
		return nil, ErrRetryConflict
	}
	requestDigest := sha256.Sum256([]byte("redeem\x00" + locator))
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte("join\x00" + code))
	var credentialDigest [32]byte
	copy(credentialDigest[:], mac.Sum(nil))
	keyDigest := sha256.Sum256([]byte("redeem\x00" + key))
	receiptKey := hex.EncodeToString(keyDigest[:])
	var result []byte
	err := s.update(ctx, func(tx storage.Tx) error {
		if key != "" {
			receipt, found, err := tx.Receipt(receiptKey)
			if err != nil {
				return err
			}
			if found {
				if receipt.Operation != "redeem" || receipt.RequestDigest != requestDigest || subtle.ConstantTimeCompare(receipt.CredentialDigest[:], credentialDigest[:]) != 1 {
					return ErrRetryConflict
				}
				if !receipt.ExpiresAt.After(tx.Now()) {
					return os.ErrNotExist
				}
				if _, err := s.active(tx, receipt.SessionID); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						return os.ErrNotExist
					}
					return err
				}
				result = append([]byte(nil), receipt.Response...)
				return nil
			}
		}
		grant, found, err := tx.Grant(locator)
		if err != nil {
			return err
		}
		if !found || !grant.ExpiresAt.After(tx.Now()) || subtle.ConstantTimeCompare([]byte(grant.JoinCode), []byte(code)) != 1 {
			return os.ErrNotExist
		}
		if _, err := names.ParseStored(grant.GrammarName, grant.Alias); err != nil {
			return errors.New("stored readable name grammar is invalid")
		}
		session, err := s.active(tx, grant.SessionID)
		if errors.Is(err, sql.ErrNoRows) {
			return os.ErrNotExist
		}
		if err != nil {
			return err
		}
		if key != "" {
			counts, err := tx.Counts()
			if err != nil {
				return err
			}
			if counts.ReceiptCount >= s.config.MaxReceipts {
				return ErrReceiptCapacity
			}
		}
		credential, err := s.sign(session, token.Claims{SessionID: grant.SessionID, Role: token.RoleSubscriber, BusID: grant.BusID}, s.config.SubscriberTokenTTL, tx.Now())
		if err != nil {
			return err
		}
		result, err = encode(ResolvedInvitation{SessionID: grant.SessionID, BusID: grant.BusID, SubscriberToken: credential})
		if err != nil {
			return err
		}
		if len(result) > 16384 {
			return storage.ErrCapacity
		}
		if key != "" {
			expires := tx.Now().Add(s.config.ReceiptTTL)
			for _, bound := range []time.Time{grant.ExpiresAt, session.ExpiresAt, tx.Now().Add(s.config.SubscriberTokenTTL).Truncate(time.Second)} {
				if bound.Before(expires) {
					expires = bound
				}
			}
			if err := tx.PutReceipt(storage.Receipt{SchemaVersion: storage.SchemaVersion, Key: receiptKey, Operation: "redeem", SessionID: grant.SessionID, RequestDigest: requestDigest, CredentialDigest: credentialDigest, Response: result, StatusCode: 200, CreatedAt: tx.Now(), ExpiresAt: expires}); err != nil {
				return err
			}
		}
		return tx.DeleteGrant(grant.ID)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func validOpaqueInvitationCode(value string) bool {
	if len(value) != 36 {
		return false
	}
	_, err := uuid.Parse(value)
	return err == nil && strings.Count(value, "-") == 4
}

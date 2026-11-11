package ledger

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"example.com/llmgateway/internal/core"
)

type auditEnvelope struct {
	Event core.Event `json:"event"`
	Time  time.Time  `json:"time"`
}

func (s *Store) Audit(ctx context.Context, event core.Event) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.appendAudit(ctx, tx, event); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) appendAudit(ctx context.Context, tx *sql.Tx, event core.Event) error {
	if event.Kind == "" {
		return errors.New("audit event kind is required")
	}
	payload, err := json.Marshal(auditEnvelope{Event: event, Time: time.Now().UTC()})
	if err != nil {
		return err
	}
	var seq int64
	var prev, public []byte
	err = tx.QueryRowContext(ctx, `
		SELECT seq, digest, key_public FROM gateway_audit_head WHERE singleton FOR UPDATE`).Scan(&seq, &prev, &public)
	if err != nil {
		return err
	}
	if !s.key.Public().(ed25519.PublicKey).Equal(ed25519.PublicKey(public)) {
		return errors.New("audit signing key does not match chain")
	}
	seq++
	digest := auditDigest(seq, prev, payload)
	sig := ed25519.Sign(s.key, digest)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO gateway_audit (seq, kind, attempt_id, payload, prev_digest, digest, signature)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		seq, event.Kind, event.AttemptID, payload, prev, digest, sig); err != nil {
		return err
	}
	return requireOne(tx.ExecContext(ctx, `UPDATE gateway_audit_head SET seq = $1, digest = $2 WHERE singleton`, seq, digest))
}

func auditDigest(seq int64, prev, payload []byte) []byte {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(seq))
	h := sha256.New()
	_, _ = h.Write([]byte("llmgateway.audit.v1\x00"))
	_, _ = h.Write(n[:])
	_, _ = h.Write(prev)
	_, _ = h.Write(payload)
	return h.Sum(nil)
}

// VerifyAudit uses an independently configured public key and a consistent snapshot.
// A valid prefix plus a correspondingly rolled-back database head requires an
// independent head checkpoint to detect. This function does not provide one.
func (s *Store) VerifyAudit(ctx context.Context, trusted ed25519.PublicKey) error {
	if len(trusted) != ed25519.PublicKeySize {
		return errors.New("trusted audit public key required")
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var headSeq int64
	var headDigest, public []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT seq, digest, key_public FROM gateway_audit_head WHERE singleton`).Scan(&headSeq, &headDigest, &public); err != nil {
		return err
	}
	if !trusted.Equal(ed25519.PublicKey(public)) {
		return errors.New("audit key differs from trusted key")
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT seq, kind, attempt_id, payload, prev_digest, digest, signature FROM gateway_audit ORDER BY seq`)
	if err != nil {
		return err
	}
	defer rows.Close()
	wantSeq := int64(1)
	prev := make([]byte, sha256.Size)
	for rows.Next() {
		var seq int64
		var kind, id string
		var payload, link, digest, sig []byte
		if err := rows.Scan(&seq, &kind, &id, &payload, &link, &digest, &sig); err != nil {
			return err
		}
		if seq != wantSeq || !bytes.Equal(link, prev) ||
			!bytes.Equal(auditDigest(seq, link, payload), digest) ||
			!ed25519.Verify(trusted, digest, sig) {
			return fmt.Errorf("invalid audit record at sequence %d", wantSeq)
		}
		var envelope auditEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil ||
			envelope.Event.Kind != kind || envelope.Event.AttemptID != id {
			return fmt.Errorf("invalid indexed audit fields at sequence %d", seq)
		}
		wantSeq++
		prev = digest
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if headSeq != wantSeq-1 || !bytes.Equal(headDigest, prev) {
		return errors.New("audit chain disagrees with persisted head")
	}
	return tx.Commit()
}

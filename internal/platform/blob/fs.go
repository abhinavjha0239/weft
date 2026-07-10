package blob

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// FS stores blobs on a local filesystem — the bare-metal driver, and the
// default for self-hosters (any mounted volume works). Writes go to a temp
// file in the same directory and rename into place: atomic on POSIX, so a
// concurrent Open never sees a partial blob.
type FS struct {
	root string
}

func NewFS(root string) (*FS, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &FS{root: root}, nil
}

// path fans keys out into subdirectories (keys are content hashes, so the
// first bytes distribute uniformly); the key is validated against escapes.
func (s *FS) path(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || strings.ContainsAny(key, `\:`) {
		return "", errors.New("blob: invalid key")
	}
	return filepath.Join(s.root, key), nil
}

func (s *FS) Put(ctx context.Context, key string, r io.Reader) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if _, err := os.Stat(p); err == nil {
		return nil // content-addressed: already present
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".upload-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), p)
}

func (s *FS) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

func (s *FS) Delete(ctx context.Context, key string) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

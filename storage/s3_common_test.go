package storage

import (
	"bytes"
	"context"
	"errors"
	urlpkg "net/url"
	"strings"
	"testing"
)

func TestParseEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		wantHost string
		wantErr  bool
	}{
		{"empty returns sentinel", "", "", false},
		{"https url", "https://s3.example.com", "s3.example.com", false},
		{"with port", "http://minio.local:9000", "minio.local:9000", false},
		{"gcs", "https://storage.googleapis.com", "storage.googleapis.com", false},
		{"transfer accel", "https://s3-accelerate.amazonaws.com", "s3-accelerate.amazonaws.com", false},
		// "://bad" yields a parse error from net/url.
		{"malformed scheme", "://bad", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := parseEndpoint(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseEndpoint(%q) err=%v wantErr=%v", tc.in, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got := u.Host; got != tc.wantHost {
				t.Errorf("parseEndpoint(%q).Host = %q, want %q", tc.in, got, tc.wantHost)
			}
		})
	}
}

func TestSupportsTransferAcceleration(t *testing.T) {
	cases := map[string]bool{
		"https://s3-accelerate.amazonaws.com":      true,
		"https://s3-accelerate.amazonaws.com/path": true,
		"https://s3.amazonaws.com":                 false,
		"https://storage.googleapis.com":           false,
		"":                                         false,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			u, _ := parseEndpoint(in)
			if got := supportsTransferAcceleration(u); got != want {
				t.Errorf("supportsTransferAcceleration(%q) = %v, want %v", in, got, want)
			}
		})
	}
}

func TestIsGoogleEndpoint(t *testing.T) {
	cases := map[string]bool{
		"https://storage.googleapis.com":      true,
		"https://storage.googleapis.com/path": true,
		"https://s3.amazonaws.com":            false,
		"":                                    false,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			u, _ := parseEndpoint(in)
			if got := IsGoogleEndpoint(u); got != want {
				t.Errorf("IsGoogleEndpoint(%q) = %v, want %v", in, got, want)
			}
		})
	}
}

func TestIsVirtualHostStyle(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"sentinel (no endpoint)", "", true},
		{"transfer accel", "https://s3-accelerate.amazonaws.com", true},
		{"gcs", "https://storage.googleapis.com", true},
		{"minio (path-style)", "http://minio.local:9000", false},
		{"r2-style custom", "https://r2.cloudflarestorage.com", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, _ := parseEndpoint(tc.in)
			if got := isVirtualHostStyle(u); got != tc.want {
				t.Errorf("isVirtualHostStyle(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestGenerateRetryID(t *testing.T) {
	seen := make(map[string]struct{}, 1024)
	for i := 0; i < 1024; i++ {
		id := generateRetryID()
		if id == "" {
			t.Fatal("generateRetryID returned empty string")
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("generateRetryID returned duplicate token %q on attempt %d", id, i)
		}
		seen[id] = struct{}{}
	}
}

func TestIsCancelationError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", errors.New("boom"), false},
		{"context.Canceled", context.Canceled, true},
		{"context.DeadlineExceeded", context.DeadlineExceeded, true},
		{"wrapped Canceled", &wrappedErr{ctx: context.Canceled}, true},
		{"wrapped DeadlineExceeded", &wrappedErr{ctx: context.DeadlineExceeded}, true},
		{"unrelated wrapped", &wrappedErr{ctx: errors.New("other")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCancelationError(tc.err); got != tc.want {
				t.Errorf("IsCancelationError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// wrappedErr exercises errors.Is traversal for IsCancelationError.
type wrappedErr struct{ ctx error }

func (w *wrappedErr) Error() string { return "wrapped: " + w.ctx.Error() }
func (w *wrappedErr) Unwrap() error { return w.ctx }

func TestJSONDecoder(t *testing.T) {
	r := strings.NewReader(`{"a":1}{"b":2}`)
	d := NewJSONDecoder(r)

	v1, err := d.Decode()
	if err != nil {
		t.Fatalf("first Decode: %v", err)
	}
	if string(v1) != `{"a":1}` {
		t.Errorf("v1 = %q, want {\"a\":1}", v1)
	}

	v2, err := d.Decode()
	if err != nil {
		t.Fatalf("second Decode: %v", err)
	}
	if string(v2) != `{"b":2}` {
		t.Errorf("v2 = %q, want {\"b\":2}", v2)
	}

	if _, err := d.Decode(); err == nil {
		t.Fatal("third Decode: want EOF error, got nil")
	}
}

func TestCsvDecoder(t *testing.T) {
	r := bytes.NewBufferString("a,b,c\nd,e,f\n")
	d := NewCsvDecoder(r)

	row1, err := d.Decode()
	if err != nil {
		t.Fatalf("first Decode: %v", err)
	}
	if got := string(row1); got != "a,b,c" {
		t.Errorf("row1 = %q, want a,b,c", got)
	}

	row2, err := d.Decode()
	if err != nil {
		t.Fatalf("second Decode: %v", err)
	}
	if got := string(row2); got != "d,e,f" {
		t.Errorf("row2 = %q, want d,e,f", got)
	}
}

func TestSentinelURL(t *testing.T) {
	if (sentinelURL != urlpkg.URL{}) {
		t.Errorf("sentinelURL is not the zero URL: %+v", sentinelURL)
	}
}

package doclite

import (
	"encoding/json"
	"testing"
)

// The fast path of topMatcher must give the same result as a full
// decode.
func FuzzMatchRaw(f *testing.F) {
	for _, s := range []string{
		`{"name":"Ada","age":36,"tags":["a","b"],"manager":{"name":"x"}}`,
		`{ "age" : 28.0 , "city":"La\"gos" }`,
		`{"ab":1,"age":2}`,
		`{"city":null,"age":-1e3,"x":[{"name":"Ada"}]}`,
		`{}`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		raw, err := encodeDoc(json.RawMessage(s))
		if err != nil {
			return
		}
		for _, tc := range filterCases {
			m, _, err := compileFilter(tc.filter)
			if err != nil {
				t.Fatal(err)
			}
			tm, ok := m.(*topMatcher)
			if !ok {
				continue
			}
			doc, err := decodeJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			for _, in := range [][]byte{raw, []byte(s)} {
				got, err := tm.matchRaw(in)
				if err != nil {
					t.Fatal(err)
				}
				if want := tm.m.match(doc); got != want {
					t.Fatalf("doc %s filter %v: fast=%v full=%v", in, tc.filter, got, want)
				}
			}
		}
	})
}

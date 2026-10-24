package doclite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// A filter selects documents. It is a map (or a value that encodes to a
// JSON object) from field paths to conditions:
//
//	M{"name": "Ada"}                        // equal
//	M{"age": M{"$gte": 18, "$lt": 65}}      // range
//	M{"address.city": "Lagos"}             // field of a sub-document
//	M{"tags": "go"}                         // array has an element
//	M{"$or": []M{{"a": 1}, {"b": 2}}}
//
// Operators: $eq $ne $gt $gte $lt $lte $in $nin $exists $regex $not, and
// at the top level $and $or $nor. Numbers compare by value, so 1 and 1.0
// are equal. A nil filter matches all documents.
//
// A struct filter is encoded with encoding/json, so zero fields are part
// of the filter unless they have the omitempty tag.

type matcher interface {
	match(doc any) bool
}

type matchAll struct{}

func (matchAll) match(any) bool { return true }

type andMatcher []matcher

func (m andMatcher) match(doc any) bool {
	for _, x := range m {
		if !x.match(doc) {
			return false
		}
	}
	return true
}

type orMatcher []matcher

func (m orMatcher) match(doc any) bool {
	for _, x := range m {
		if x.match(doc) {
			return true
		}
	}
	return false
}

type notMatcher struct{ m matcher }

func (m notMatcher) match(doc any) bool { return !m.m.match(doc) }

// fieldMatcher tests the values at one field path.
type fieldMatcher struct {
	path []string
	cond cond
}

func (m *fieldMatcher) match(doc any) bool {
	vals, found := lookup(doc, m.path)
	return m.cond.test(vals, found)
}

// cond is a test on the values found at a path.
type cond interface {
	test(vals []any, found bool) bool
}

type eqCond struct{ v any }

func (c eqCond) test(vals []any, found bool) bool {
	if !found {
		return c.v == nil
	}
	for _, x := range vals {
		if jsonEqual(x, c.v) {
			return true
		}
	}
	return false
}

type cmpCond struct {
	op string
	v  any
}

func (c cmpCond) test(vals []any, found bool) bool {
	for _, x := range vals {
		r, ok := jsonCompare(x, c.v)
		if !ok {
			continue
		}
		switch c.op {
		case "$gt":
			if r > 0 {
				return true
			}
		case "$gte":
			if r >= 0 {
				return true
			}
		case "$lt":
			if r < 0 {
				return true
			}
		case "$lte":
			if r <= 0 {
				return true
			}
		}
	}
	return false
}

type inCond struct{ list []any }

func (c inCond) test(vals []any, found bool) bool {
	for _, v := range c.list {
		if (eqCond{v}).test(vals, found) {
			return true
		}
	}
	return false
}

type existsCond struct{ want bool }

func (c existsCond) test(_ []any, found bool) bool { return found == c.want }

type regexCond struct{ re *regexp.Regexp }

func (c regexCond) test(vals []any, _ bool) bool {
	for _, x := range vals {
		if s, ok := x.(string); ok && c.re.MatchString(s) {
			return true
		}
	}
	return false
}

type notCond struct{ c cond }

func (c notCond) test(vals []any, found bool) bool { return !c.c.test(vals, found) }

type allCond []cond

func (c allCond) test(vals []any, found bool) bool {
	for _, x := range c {
		if !x.test(vals, found) {
			return false
		}
	}
	return true
}

// normalize encodes v as JSON and decodes it into maps, slices, strings,
// bools, nil and json.Number values.
func normalize(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return decodeJSON(b)
}

func decodeJSON(b []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var out any
	if err := d.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func invalidFilter(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidFilter, fmt.Sprintf(format, args...))
}

// compileFilter turns a filter into a matcher. It also returns the
// normalized filter, which the query planner reads.
func compileFilter(filter any) (matcher, map[string]any, error) {
	if filter == nil {
		return matchAll{}, nil, nil
	}
	n, err := normalize(filter)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}
	if n == nil {
		return matchAll{}, nil, nil
	}
	m, ok := n.(map[string]any)
	if !ok {
		return nil, nil, invalidFilter("filter must be an object")
	}
	if len(m) == 0 {
		return matchAll{}, m, nil
	}
	mt, err := compileDoc(m)
	if err != nil {
		return nil, nil, err
	}
	return &topMatcher{m: mt, fields: topFields(m, nil)}, m, nil
}

// topMatcher knows the top-level fields that its filter reads, so that
// a document can be decoded in part.
type topMatcher struct {
	m      matcher
	fields []string
}

func (t *topMatcher) match(doc any) bool { return t.m.match(doc) }

// matchRaw tests the JSON text of a document. Only the fields that the
// filter reads are decoded.
func (t *topMatcher) matchRaw(raw []byte) (bool, error) {
	doc := make(map[string]any, len(t.fields))
	ok := scanTop(raw, func(key, val []byte) bool {
		for _, f := range t.fields {
			if f == string(key) {
				x, err := decodeValue(val)
				if err != nil {
					return false
				}
				doc[f] = x
			}
		}
		return true
	})
	if !ok {
		// Keys with escapes and bad JSON take the slow path.
		v, err := decodeJSON(raw)
		if err != nil {
			return false, err
		}
		return t.m.match(v), nil
	}
	return t.m.match(doc), nil
}

// scanTop calls fn for each key and raw value of the top-level JSON
// object in b. It returns false if b is not a simple object (for
// example, a key has an escape); the caller then decodes it in full.
func scanTop(b []byte, fn func(key, val []byte) bool) bool {
	i := skipSpace(b, 0)
	if i >= len(b) || b[i] != '{' {
		return false
	}
	i = skipSpace(b, i+1)
	if i < len(b) && b[i] == '}' {
		return true
	}
	for i < len(b) {
		if b[i] != '"' {
			return false
		}
		ks := i + 1
		ke := ks
		for ke < len(b) && b[ke] != '"' {
			if b[ke] == '\\' {
				return false
			}
			ke++
		}
		if ke >= len(b) {
			return false
		}
		i = skipSpace(b, ke+1)
		if i >= len(b) || b[i] != ':' {
			return false
		}
		vs := skipSpace(b, i+1)
		ve := skipValue(b, vs)
		if ve < 0 || !fn(b[ks:ke], b[vs:ve]) {
			return false
		}
		i = skipSpace(b, ve)
		if i >= len(b) {
			return false
		}
		if b[i] == '}' {
			return true
		}
		if b[i] != ',' {
			return false
		}
		i = skipSpace(b, i+1)
	}
	return false
}

func skipSpace(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// skipValue returns the end of the JSON value that starts at i, or -1.
func skipValue(b []byte, i int) int {
	if i >= len(b) {
		return -1
	}
	switch b[i] {
	case '"':
		return skipString(b, i)
	case '{', '[':
		depth := 0
		for i < len(b) {
			switch b[i] {
			case '"':
				if i = skipString(b, i); i < 0 {
					return -1
				}
				continue
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
			i++
		}
		return -1
	default:
		for i < len(b) && b[i] != ',' && b[i] != '}' && b[i] != ']' && b[i] != ' ' && b[i] != '\n' && b[i] != '\t' && b[i] != '\r' {
			i++
		}
		return i
	}
}

func skipString(b []byte, i int) int {
	for i++; i < len(b); i++ {
		switch b[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return -1
}

// decodeValue decodes one JSON value. Simple scalars do not use the
// json package.
func decodeValue(v []byte) (any, error) {
	if len(v) > 0 {
		switch c := v[0]; {
		case c == '"':
			if bytes.IndexByte(v[1:len(v)-1], '\\') < 0 {
				return string(v[1 : len(v)-1]), nil
			}
		case c == '-' || (c >= '0' && c <= '9'):
			return json.Number(v), nil
		case string(v) == "true":
			return true, nil
		case string(v) == "false":
			return false, nil
		case string(v) == "null":
			return nil, nil
		}
	}
	return decodeJSON(v)
}

func topFields(m map[string]any, out []string) []string {
	for key, v := range m {
		if strings.HasPrefix(key, "$") {
			list, _ := v.([]any)
			for _, x := range list {
				if sm, ok := x.(map[string]any); ok {
					out = topFields(sm, out)
				}
			}
			continue
		}
		out = append(out, splitPath(key)[0])
	}
	return out
}

func compileDoc(m map[string]any) (matcher, error) {
	var out andMatcher
	for key, v := range m {
		switch key {
		case "$and", "$or", "$nor":
			list, ok := v.([]any)
			if !ok || len(list) == 0 {
				return nil, invalidFilter("%s needs a non-empty array", key)
			}
			var subs []matcher
			for _, x := range list {
				sm, ok := x.(map[string]any)
				if !ok {
					return nil, invalidFilter("%s items must be objects", key)
				}
				c, err := compileDoc(sm)
				if err != nil {
					return nil, err
				}
				subs = append(subs, c)
			}
			switch key {
			case "$and":
				out = append(out, andMatcher(subs))
			case "$or":
				out = append(out, orMatcher(subs))
			default:
				out = append(out, notMatcher{orMatcher(subs)})
			}
		default:
			if strings.HasPrefix(key, "$") {
				return nil, invalidFilter("unknown operator %s", key)
			}
			c, err := compileCond(v)
			if err != nil {
				return nil, err
			}
			out = append(out, &fieldMatcher{path: splitPath(key), cond: c})
		}
	}
	if len(out) == 1 {
		return out[0], nil
	}
	return out, nil
}

// isOperatorDoc reports whether v is an object of $operators.
func isOperatorDoc(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return nil, false
	}
	for k := range m {
		if !strings.HasPrefix(k, "$") {
			return nil, false
		}
	}
	return m, true
}

func compileCond(v any) (cond, error) {
	ops, ok := isOperatorDoc(v)
	if !ok {
		return eqCond{v}, nil
	}
	var out allCond
	for op, arg := range ops {
		switch op {
		case "$eq":
			out = append(out, eqCond{arg})
		case "$ne":
			out = append(out, notCond{eqCond{arg}})
		case "$gt", "$gte", "$lt", "$lte":
			out = append(out, cmpCond{op, arg})
		case "$in", "$nin":
			list, ok := arg.([]any)
			if !ok {
				return nil, invalidFilter("%s needs an array", op)
			}
			var c cond = inCond{list}
			if op == "$nin" {
				c = notCond{c}
			}
			out = append(out, c)
		case "$exists":
			b, ok := arg.(bool)
			if !ok {
				return nil, invalidFilter("$exists needs true or false")
			}
			out = append(out, existsCond{b})
		case "$regex":
			s, ok := arg.(string)
			if !ok {
				return nil, invalidFilter("$regex needs a string")
			}
			re, err := regexp.Compile(s)
			if err != nil {
				return nil, invalidFilter("$regex: %v", err)
			}
			out = append(out, regexCond{re})
		case "$not":
			c, err := compileCond(arg)
			if err != nil {
				return nil, err
			}
			out = append(out, notCond{c})
		default:
			return nil, invalidFilter("unknown operator %s", op)
		}
	}
	if len(out) == 1 {
		return out[0], nil
	}
	return out, nil
}

func splitPath(field string) []string { return strings.Split(field, ".") }

// lookup returns the values at path in doc. If a value on the way is an
// array, lookup looks into each element. An array at the end of the path
// gives the array and each of its elements.
func lookup(doc any, path []string) ([]any, bool) {
	var out []any
	found := false
	var walk func(v any, p []string)
	walk = func(v any, p []string) {
		if len(p) == 0 {
			found = true
			out = append(out, v)
			if arr, ok := v.([]any); ok {
				out = append(out, arr...)
			}
			return
		}
		switch x := v.(type) {
		case map[string]any:
			if next, ok := x[p[0]]; ok {
				walk(next, p[1:])
			}
		case []any:
			if i, err := strconv.Atoi(p[0]); err == nil && i >= 0 && i < len(x) {
				walk(x[i], p[1:])
				return
			}
			for _, e := range x {
				if _, ok := e.(map[string]any); ok {
					walk(e, p)
				}
			}
		}
	}
	walk(doc, path)
	return out, found
}

// jsonEqual compares two normalized JSON values. Numbers compare by
// value.
func jsonEqual(a, b any) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case string:
		y, ok := b.(string)
		return ok && x == y
	case json.Number:
		y, ok := b.(json.Number)
		return ok && compareNumbers(x, y) == 0
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !jsonEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !jsonEqual(v, w) {
				return false
			}
		}
		return true
	}
	return false
}

// jsonCompare orders two values of the same kind: numbers, strings or
// bools. ok is false for other kinds.
func jsonCompare(a, b any) (int, bool) {
	switch x := a.(type) {
	case json.Number:
		if y, ok := b.(json.Number); ok {
			return compareNumbers(x, y), true
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y), true
		}
	case bool:
		if y, ok := b.(bool); ok {
			switch {
			case x == y:
				return 0, true
			case !x:
				return -1, true
			default:
				return 1, true
			}
		}
	}
	return 0, false
}

func compareNumbers(a, b json.Number) int {
	if x, err := a.Int64(); err == nil {
		if y, err := b.Int64(); err == nil {
			switch {
			case x < y:
				return -1
			case x > y:
				return 1
			}
			return 0
		}
	}
	x, ok1 := new(big.Float).SetString(string(a))
	y, ok2 := new(big.Float).SetString(string(b))
	if !ok1 || !ok2 {
		return strings.Compare(string(a), string(b))
	}
	return x.Cmp(y)
}

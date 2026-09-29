package main

import (
	"encoding/json"
	"testing"
)

func TestPolicyThresholds(t *testing.T) {
	for _, test := range []struct {
		answer string
		want   string
	}{
		{`{"answers":{"route":{"confidence":0.95}}}`, "auto"},
		{`{"answers":{"route":{"confidence":0.70}}}`, "review"},
		{`{"answers":{"route":{"confidence":0.69}}}`, "conservative"},
		{`{"answers":{"noul":{"noul":0.99}}}`, "conservative"},
	} {
		if got := policy(json.RawMessage(test.answer)); got != test.want {
			t.Errorf("policy(%s) = %q, want %q", test.answer, got, test.want)
		}
	}
}

func TestLRUEvictsOldestEntry(t *testing.T) {
	cache := newLRU(2)
	cache.put("first", json.RawMessage(`1`))
	cache.put("second", json.RawMessage(`2`))
	if _, ok := cache.get("first"); !ok {
		t.Fatal("expected first cache hit")
	}
	cache.put("third", json.RawMessage(`3`))
	if _, ok := cache.get("second"); ok {
		t.Fatal("expected second entry to be evicted")
	}
}

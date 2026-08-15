package simulator

import (
	"reflect"
	"testing"
	"time"
)

func TestGeneratorIsDeterministic(t *testing.T) {
	g1, _ := New(42, 10, 0.2)
	g2, _ := New(42, 10, 0.2)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for range 100 {
		tx1, label1 := g1.Next(now, time.Second)
		tx2, label2 := g2.Next(now, time.Second)
		if !reflect.DeepEqual(tx1, tx2) || !reflect.DeepEqual(label1, label2) {
			t.Fatal("same seed produced different events")
		}
	}
}

func TestTransactionDoesNotContainLabel(t *testing.T) {
	g, _ := New(1, 1, 1)
	tx, label := g.Next(time.Now(), time.Second)
	if !label.IsFraud {
		t.Fatal("expected a fraud label")
	}
	if err := tx.Validate(); err != nil {
		t.Fatalf("generated transaction is invalid: %v", err)
	}
}

//go:build integration
// +build integration

package dynamo

import (
	"testing"

	"github.com/micromdm/nanomdm/storage/internal/test"

	_ "github.com/go-sql-driver/mysql"
)

func TestQueue(t *testing.T) {

	storage, err := New(WithDSN(*flDSN), WithDeleteCommands())
	if err != nil {
		t.Fatal(err)
	}

	d, err := enrollTestDevice(storage)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("WithDeleteCommands()", func(t *testing.T) {
		test.TestQueue(t, d.UDID, storage)
	})

	storage, err = New(WithDSN(*flDSN))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("normal", func(t *testing.T) {
		test.TestQueue(t, d.UDID, storage)
	})
}

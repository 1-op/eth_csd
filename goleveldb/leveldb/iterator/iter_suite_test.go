package iterator_test

import (
	"testing"

	"awesomeProject1/goleveldb/leveldb/testutil"
)

func TestIterator(t *testing.T) {
	testutil.RunSuite(t, "Iterator Suite")
}

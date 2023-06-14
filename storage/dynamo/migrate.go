package dynamo

import (
	"context"
)

func (s DSDynamoTable) RetrieveMigrationCheckins(_ context.Context, c chan<- interface{}) error {
	return nil
}

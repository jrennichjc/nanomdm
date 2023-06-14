package dynamo

import (
	"context"
	"errors"

	"github.com/micromdm/nanomdm/mdm"
)

type dsTokenUpdate struct {
	Device string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	Token string `dynamodbav:"token,omitempty"`
}

// RetrievePushInfo retrieves APNs-related data for push notifications
func (s *DSDynamoTable) RetrievePushInfo(_ context.Context, ids []string) (map[string]*mdm.Push, error) {
	pushInfos := make(map[string]*mdm.Push)

	for _, id := range ids {
		var tokenUpdate dsTokenUpdate

		if s.GetSingleItemPKSK("device#" + id, TokenUpdateFilename, &tokenUpdate) {

			if tokenUpdate.Token == "" {
				continue
			}
			
			msg, err := mdm.DecodeCheckin([]byte(tokenUpdate.Token))
			if err != nil {
				return nil, err
			}
			message, ok := msg.(*mdm.TokenUpdate)
			if !ok {
				return nil, errors.New("saved TokenUpdate is not a TokenUpdate")
			}
			pushInfos[id] = &message.Push
		}
	}

	return pushInfos, nil
}

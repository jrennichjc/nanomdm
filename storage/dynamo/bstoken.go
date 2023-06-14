package dynamo

import (
	"encoding/base64"
	"errors"

	"github.com/micromdm/nanomdm/mdm"
)

type dynamoBSToken struct {
	Device string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	BSToken string `dynamodbav:"bstoken,omitempty"`
}

func (s *DSDynamoTable) StoreBootstrapToken(r *mdm.Request, msg *mdm.SetBootstrapToken) error {

	bsToken := dynamoBSToken{
		Device: "device#" + r.ID,
		Category: BootstrapTokenFile,
		BSToken: msg.BootstrapToken.BootstrapToken.String(),
	}

	return s.AddItem(bsToken)
}

func (s *DSDynamoTable) RetrieveBootstrapToken(r *mdm.Request, _ *mdm.GetBootstrapToken) (*mdm.BootstrapToken, error) {

	var bsToken dynamoBSToken

	if s.GetSingleItemPKSK("device#" + r.ID, BootstrapTokenFile,&bsToken) {

		if bsToken.BSToken != "" {
			return nil, errors.New("unable to find record in dynamo")
		}

		bsTokenRaw, _ := base64.StdEncoding.DecodeString(bsToken.BSToken)

		bsToken := &mdm.BootstrapToken{
			BootstrapToken: bsTokenRaw,
		}
		return bsToken, nil
	}

	return nil, errors.New("unable to find record in dynamo")
}

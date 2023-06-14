package dynamo


import (
	"context"
	"crypto/tls"

	"github.com/micromdm/nanomdm/cryptoutil"
)

type dsPushCert struct {
	Category string `dynamodbav:"pk"`
	Topic string `dynamodbav:"sk"`
	Key string `dynamodbav:"key"`
	PEM string `dynamodbav:"pem"`
	Stale bool `dynamodbav:"staletoken"`
}

// RetrievePushCert is passed through to a new PushCertFileStorage
func (s *DSDynamoTable) RetrievePushCert(ctx context.Context, topic string) (*tls.Certificate, string, error) {

	var pushCert dsPushCert

	if s.GetSingleItemPKSK(PushCertFile, topic, &pushCert) {

		if pushCert.Key == "" {
			return nil, "", nil
		}

		cert, err := tls.X509KeyPair([]byte(pushCert.PEM), []byte(pushCert.Key))
		if err != nil {
			return nil, "", err
		}
		return &cert, "", err
	}

	return nil, "", nil
}

// IsPushCertStale is passed through to a new PushCertFileStorage
func (s *DSDynamoTable) IsPushCertStale(ctx context.Context, topic, providedStaleToken string) (bool, error) {
	//TODO: Make this real
	return false, nil
}

// StorePushCert is passed through to a new PushCertFileStorage
func (s *DSDynamoTable) StorePushCert(ctx context.Context, pemCert, pemKey []byte) error {
	topic, err := cryptoutil.TopicFromPEMCert(pemCert)
	if err != nil {
		return err
	}

	pushCert := dsPushCert{
		Category: PushCertFile,
		Topic: topic,
		PEM: string(pemCert),
		Key: string(pemKey),
		Stale: false,
	}
	
	return s.AddItem(pushCert)
}
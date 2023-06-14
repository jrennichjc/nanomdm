package dynamo

import (
	"errors"

	"github.com/micromdm/nanomdm/mdm"
)

type dynamoDeviceToCertHash struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	Hash     string `dynamodbav:"hash"`
}
type dynamoCertHashtoDevice struct {
	Hash   string `dynamodbav:"pk"`
	Device string `dynamodbav:"sk"`
}

func (s *DSDynamoTable) EnrollmentHasCertHash(r *mdm.Request, _ string) (bool, error) {

	var deviceToCert dynamoDeviceToCertHash

	if s.GetSingleItemPKSK("device#"+r.ID, CertHashFile, &deviceToCert) {
		if deviceToCert.Hash != "" {
			return true, nil
		}
	}

	return false, errors.New("no device hash")
}

func (s *DSDynamoTable) HasCertHash(r *mdm.Request, hash string) (bool, error) {

	var hashToDevice dynamoCertHashtoDevice

	if s.GetSingleItemPKSK("hash#"+hash, CertHashFile, &hashToDevice) {
		if hashToDevice.Hash != "" {
			return true, nil
		}
	}

	return false, nil
}

func (s *DSDynamoTable) IsCertHashAssociated(r *mdm.Request, hash string) (bool, error) {

	var deviceToCert dynamoDeviceToCertHash

	if s.GetSingleItemPKSK("device#"+r.ID, CertHashFile, &deviceToCert) {
		if deviceToCert.Hash == hash {
			return true, nil
		}
	}
	return false, nil
}

func (s *DSDynamoTable) AssociateCertHash(r *mdm.Request, hash string) error {

	deviceHash := dynamoDeviceToCertHash{
		Device:   "device#" + r.ID,
		Category: CertHashFile,
		Hash:     hash,
	}

	err := s.AddItem(deviceHash)

	if err != nil {
		return err
	}

	hashToDevice := dynamoCertHashtoDevice{
		Hash:   "hash#" + hash,
		Device: r.ID,
	}

	err = s.AddItem(hashToDevice)

	return err

}

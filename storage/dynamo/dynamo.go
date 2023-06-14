package dynamo

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/micromdm/nanomdm/cryptoutil"
	"github.com/micromdm/nanomdm/log"
	"github.com/micromdm/nanomdm/mdm"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DSDynamoTable struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
	Logger         log.Logger
}

type dsGenericItem struct {
	PK   string `dynamodbav:"pk"`
	SK   string `dynamodbav:"sk"`
	Body string `dynamodbav:"body,omitempty"`
}

type dsUserAuthentication struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	UserAuth string `dynamodbav:"bstoken,omitempty"`
}

type dsTokenUpdateTally struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	Tally    int    `dynamodbav:"tally,omitempty"`
}

type dsTokenUnlock struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	Token    string `dynamodbav:"token,omitempty"`
}

type dsIdentityCert struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	Cert     string `dynamodbav:"cert,omitempty"`
}

const (
	AuthenticateFilename = "Authenticate.plist"
	TokenUpdateFilename  = "TokenUpdate.plist"
	UnlockTokenFilename  = "UnlockToken.dat"
	SerialNumberFilename = "SerialNumber"
	IdentityCertFilename = "Identity.pem"
	DisabledFilename     = "Disabled"
	BootstrapTokenFile   = "BootstrapToken"
	CertHashFile         = "DeviceToCertHash"
	PushCertFile         = "pushcert#"

	TokenUpdateTallyFilename = "TokenUpdate.tally"

	UserAuthFilename       = "UserAuthenticate.plist"
	UserAuthDigestFilename = "UserAuthenticate.Digest.plist"

	CertAuthFilename             = "CertAuth.sha256"
	CertAuthAssociationsFilename = "CertAuth"

	// The associations for "sub"-enrollments (that is: user-channel
	// enrollments to device-channel enrollments) are stored in this
	// directory under the device's directory.
	SubEnrollmentPathname = "SubEnrollments"
)

type config struct {
	driver string
	dsn    string
	db     DSDynamoTable
	logger log.Logger
	rm     bool
}

type Option func(*config)

func WithLogger(logger log.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

func WithDSN(dsn string) Option {
	return func(c *config) {
		c.dsn = dsn
	}
}

func WithDriver(driver string) Option {
	return func(c *config) {
		c.driver = driver
	}
}

func WithDeleteCommands() Option {
	return func(c *config) {
		c.rm = true
	}
}

func CreateTable(svc *dynamodb.Client, tn string, attributes []types.AttributeDefinition, schema []types.KeySchemaElement) {
	out, err := svc.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: attributes,
		KeySchema:            schema,
		TableName:            aws.String(tn),
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	fmt.Println(out)
}

func New(opts ...Option) (*DSDynamoTable, error) {
	dynamocfg := &config{logger: log.NopLogger, driver: "dynamo"}
	for _, opt := range opts {
		opt(dynamocfg)
	}

	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		dynamocfg.logger.Info(err)
		return nil, err
	}

	svc := dynamodb.NewFromConfig(cfg)
	tablename := "nanomdm"
	if dynamocfg.dsn != "" {
		tablename = dynamocfg.dsn
	}
	nanoTable := DSDynamoTable{DynamoDbClient: svc, TableName: tablename, Logger: dynamocfg.logger}

	exists, _ := nanoTable.TableExists(dynamocfg.logger)

	if !exists {

		attrs := []types.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("sk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		}

		schema := []types.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("sk"),
				KeyType:       types.KeyTypeRange,
			},
		}

		CreateTable(svc, nanoTable.TableName, attrs, schema)
		nanoTable.Wait()
	}

	return &nanoTable, nil
}

// TableExists determines whether a DynamoDB table exists.
func (dsynamotable DSDynamoTable) TableExists(logger log.Logger) (bool, error) {
	exists := true
	_, err := dsynamotable.DynamoDbClient.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: aws.String(dsynamotable.TableName)},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			logger.Info("Table %v does not exist.\n", dsynamotable.TableName)
			err = nil
		} else {
			logger.Info("Couldn't determine existence of table %v. Here's why: %v\n", dsynamotable.TableName, err)
		}
		exists = false
	}
	return exists, err
}

// ListTables lists the DynamoDB table names for the current account.
func (dsynamotable DSDynamoTable) ListTables(logger log.Logger) ([]string, error) {
	var tableNames []string
	tables, err := dsynamotable.DynamoDbClient.ListTables(
		context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		logger.Info("Couldn't list tables. Here's why: %v\n", err)
	} else {
		tableNames = tables.TableNames
	}
	return tableNames, err
}

func (dsdynamoTable DSDynamoTable) Wait() error {
	w := dynamodb.NewTableExistsWaiter(dsdynamoTable.DynamoDbClient)
	err := w.Wait(context.TODO(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(dsdynamoTable.TableName),
		},
		2*time.Minute,
		func(o *dynamodb.TableExistsWaiterOptions) {
			o.MaxDelay = 5 * time.Second
			o.MinDelay = 5 * time.Second
		})

	return err
}

func (dsdynamoTable DSDynamoTable) AddItem(item interface{}) error {
	newitem, err := attributevalue.MarshalMap(item)
	if err != nil {
		return err
	}
	_, err = dsdynamoTable.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(dsdynamoTable.TableName), Item: newitem,
	})
	if err != nil {
		dsdynamoTable.Logger.Info("Couldn't add item to table. Here's why: %v\n", err)
		dsdynamoTable.Logger.Info(item)
	}
	return err
}

func (s DSDynamoTable) StoreAuthenticate(r *mdm.Request, msg *mdm.Authenticate) error {

	idCert := dsIdentityCert{
		Device:   "device#" + r.ID,
		Category: IdentityCertFilename,
		Cert:     string(cryptoutil.PEMCertificate(r.Certificate.Raw)),
	}

	serial := dsGenericItem{
		PK:   "device#" + r.ID,
		SK:   "serial",
		Body: msg.SerialNumber,
	}

	s.AddItem(serial)
	return s.AddItem(idCert)
}

func (s DSDynamoTable) StoreTokenUpdate(r *mdm.Request, msg *mdm.TokenUpdate) error {

	tokenUnlock := dsTokenUnlock{
		Device:   "device#" + r.ID,
		Category: UnlockTokenFilename,
		Token:    string(msg.UnlockToken),
	}

	if err := s.AddItem(tokenUnlock); err != nil {
		return err
	}

	tokenUpdate := dsTokenUpdate{
		Device:   "device#" + r.ID,
		Category: TokenUpdateFilename,
		Token:    string(msg.Raw),
	}

	if err := s.AddItem(tokenUpdate); err != nil {
		return err
	}

	var tokenTally dsTokenUpdateTally
	if s.GetSingleItemPKSK("device#"+r.ID, TokenUpdateTallyFilename, &tokenTally) {
		tokenTally.Tally += 1
		tokenTally.Device = "device#" + r.ID
		tokenTally.Category = TokenUpdateTallyFilename
		if err := s.AddItem(tokenTally); err != nil {
			return err
		}
	}

	// delete the disabled flag to let signify this enrollment is enabled
	//if err := os.Remove(e.dirPrefix(DisabledFilename)); err != nil && !errors.Is(err, os.ErrNotExist) {
	//	return err
	//}
	return nil
}

func (s DSDynamoTable) RetrieveTokenUpdateTally(_ context.Context, id string) (int, error) {
	var tokenTally dsTokenUpdateTally
	if s.GetSingleItemPKSK("device#"+id, TokenUpdateTallyFilename, &tokenTally) {
		if tokenTally.Tally != 0 {
			return tokenTally.Tally, nil
		}
	}
	return 0, errors.New("no token tally")
}

func (s DSDynamoTable) StoreUserAuthenticate(r *mdm.Request, msg *mdm.UserAuthenticate) error {
	userAuth := dsUserAuthentication{
		Device:   "device#" + r.ID,
		Category: UserAuthFilename,
		UserAuth: string(msg.Raw),
	}

	if msg.DigestResponse != "" {
		userAuth.Category = UserAuthDigestFilename
	}

	return s.AddItem(userAuth)
}

func (dsdynamoTable DSDynamoTable) GetSingleItemPKSK(pk, sk string, result interface{}) bool {
	out, err := dsdynamoTable.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(dsdynamoTable.TableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: sk},
		},
	})

	if err != nil {
		dsdynamoTable.Logger.Info("Unable to get item")
		dsdynamoTable.Logger.Info(err.Error())
		return false
	} else {

		err = attributevalue.UnmarshalMap(out.Item, result)
		if err != nil {
			dsdynamoTable.Logger.Info("Couldn't unmarshal response. Here's why: %v\n", err)
			dsdynamoTable.Logger.Info(err.Error())
		} else {
			return true
		}
	}

	return false
}

func (s DSDynamoTable) deleteSingleItem(item interface{}) bool {
	deleteItem, err := attributevalue.MarshalMap(item)

	if err != nil {
		return false
	}

	_, err = s.DynamoDbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: aws.String(s.TableName),
		Key:       deleteItem,
	})

	if err != nil {
		return false
	}
	return true
}

func hashCert(cert *x509.Certificate) string {
	hashed := sha256.Sum256(cert.Raw)
	b := make([]byte, len(hashed))
	copy(b, hashed[:])
	return hex.EncodeToString(b)
}

func (s DSDynamoTable) Disable(r *mdm.Request) error {

	hash := hashCert(r.Certificate)

	deviceHash := dynamoDeviceToCertHash{
		Device:   "device#" + r.ID,
		Category: CertHashFile,
		Hash:     hash,
	}

	s.deleteSingleItem(deviceHash)

	hashToDevice := dynamoCertHashtoDevice{
		Hash:   "hash#" + hash,
		Device: r.ID,
	}

	s.deleteSingleItem(hashToDevice)

	return nil
}

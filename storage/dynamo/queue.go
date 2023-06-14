package dynamo

import (
	"context"
	"errors"
	"time"

	"github.com/micromdm/nanomdm/mdm"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	subNotNow   = "QueueNotNow"
	subQueue    = "Queue"
	subDone     = "QueueDone"
	subInactive = "QueueInactive"
)

type dsCommand struct {
	Queue       string `dynamodbav:"pk"`
	Command     string `dynamodbav:"sk"`
	Payload     string `dynamodbav:"bstoken"`
	Status      string `dynamodbav:"status"`
	LastUpdated string `dynamodbav:"last_updated"`
	Result      string `dynamodbav:"result"`
}

// EnqueueCommand writes the command to disk in the queue directory
func (s *DSDynamoTable) EnqueueCommand(_ context.Context, ids []string, command *mdm.Command) (map[string]error, error) {
	idErrs := make(map[string]error)
	for _, id := range ids {

		command := dsCommand{
			Queue:       "queue#" + id,
			Command:     command.CommandUUID,
			Payload:     string(command.Raw),
			LastUpdated: time.Now().String(),
			Status:      subQueue,
		}

		if err := s.AddItem(command); err != nil {
			idErrs[id] = err
		}
	}
	return idErrs, nil
}

// StoreCommandReport moves commands to different queues (like NotNow)
func (s *DSDynamoTable) StoreCommandReport(r *mdm.Request, report *mdm.CommandResults) error {

	if report.Status == "Idle" {
		return nil
	}

	var currentCommand dsCommand
	if s.GetSingleItemPKSK("queue#"+r.ID, report.CommandUUID, &currentCommand) {

		if currentCommand.Payload == "" {
			return nil
		}

		// basic sanity check the command hasn't already been de-queued
		if currentCommand.Queue == "QueueDone" {
			return nil
		}

		currentCommand.LastUpdated = time.Now().String()
		currentCommand.Status = report.Status
		currentCommand.Result = string(report.Raw)

		return s.AddItem(currentCommand)
	}
	return errors.New("unable to find command in database")
}

// RetrieveNextCommand gets the next command from the queue while minding NotNow status
func (s *DSDynamoTable) RetrieveNextCommand(r *mdm.Request, skipNotNow bool) (*mdm.Command, error) {

	var commands []dsCommand

	if s.getAllCommands(r.ID, &commands, skipNotNow) {
		for _, command := range commands {
			return mdm.DecodeCommand([]byte(command.Payload))
		}
	}

	return nil, nil
}

func (s *DSDynamoTable) ClearQueue(r *mdm.Request) error {
	var commands []dsCommand

	if s.getAllCommands(r.ID, &commands, false) {
		for _, command := range commands {
			command.LastUpdated = time.Now().String()
			command.Status = subInactive
			s.AddItem(command)
		}
	}

	return nil
}

func (s *DSDynamoTable) getAllCommands(id string, commands *[]dsCommand, skipNotNow bool) bool {

	if !skipNotNow {
		response, err := s.DynamoDbClient.Query(context.TODO(), &dynamodb.QueryInput{
			TableName:              aws.String(s.TableName),
			KeyConditionExpression: aws.String("#DDB_pk = :pkey"),
			FilterExpression:       aws.String("#DDB_status = :queue Or #DDB_status = :queueNotNow"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pkey":        &types.AttributeValueMemberS{Value: "queue#" + id},
				":queue":       &types.AttributeValueMemberS{Value: "Queue"},
				":queueNotNow": &types.AttributeValueMemberS{Value: "QueueNotNow"},
			},
			ExpressionAttributeNames: map[string]string{
				"#DDB_pk":     "pk",
				"#DDB_status": "status",
			},
		})
		if err != nil {
			return false
		} else {
			err = attributevalue.UnmarshalListOfMaps(response.Items, commands)
			if err != nil {
				return false
			} else {
				return true
			}
		}
	} else {
		response, err := s.DynamoDbClient.Query(context.TODO(), &dynamodb.QueryInput{
			TableName:              aws.String(s.TableName),
			KeyConditionExpression: aws.String("#DDB_pk = :pkey"),
			FilterExpression:       aws.String("#DDB_status = :status"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pkey":   &types.AttributeValueMemberS{Value: "queue#" + id},
				":status": &types.AttributeValueMemberS{Value: "Queue"},
			},
			ExpressionAttributeNames: map[string]string{
				"#DDB_pk":     "pk",
				"#DDB_status": "status",
			},
		})

		if err != nil {
			return false
		} else {
			err = attributevalue.UnmarshalListOfMaps(response.Items, commands)
			if err != nil {
				return false
			} else {
				return true
			}
		}
	}
}

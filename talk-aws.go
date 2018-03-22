package balancercontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/temoto/senderbender/junk"
	"log"
	"time"
)

type apiAws struct {
	configured bool
	config     *aws.Config
	session    *session.Session
	s3         *s3.S3
	sqs        *sqs.SQS

	queueUrl string
}

type eventDataAws struct {
	receiptHandle string
}

func (self *BalancerControl) ConfigureAws(ctx context.Context, config *aws.Config) error {
	var err error
	self.aws.session, err = session.NewSession()
	if err != nil {
		return err
	}
	self.aws.s3 = s3.New(self.aws.session, config)
	self.aws.sqs = sqs.New(self.aws.session, config)
	self.aws.queueUrl = junk.MustContextGetString(ctx, "aws-queue-url")
	self.aws.configured = true
	return nil
}

func (self *BalancerControl) awsRunReceive(ctx context.Context) {
	if !self.aws.configured {
		log.Fatal("must call .ConfigureAws() before using it")
	}
	stopch := self.alive.StopChan()

	recvInput := sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(self.aws.queueUrl),
		WaitTimeSeconds:     aws.Int64(7),
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(33),
		// ReceiveRequestAttemptId: aws.String(),
	}
	changeVisInput := sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(self.aws.queueUrl),
		VisibilityTimeout: aws.Int64(0),
	}

runLoop:
	for {
		received, err := self.aws.sqs.ReceiveMessage(&recvInput)
		if err != nil {
			log.Printf("talk-aws sqs.ReceiveMessage error=%s", err.Error())
		}
		if err != nil || len(received.Messages) == 0 {
			select {
			case <-time.After(5 * time.Second):
			case <-stopch:
				break runLoop
			}
			continue
		}

		self.alive.Add(len(received.Messages)) // corresponding alive.Done() is in self.controlEvent
		for _, m := range received.Messages {
			// TODO: check m.MD5OfBody
			var e Event
			e.aws.receiptHandle = *m.ReceiptHandle
			if err := json.Unmarshal([]byte(*m.Body), &e); err != nil {
				log.Printf("talk-aws received body='%s' unmarshal error=%s", *m.Body, err.Error())
				cmv := changeVisInput
				cmv.ReceiptHandle = m.ReceiptHandle
				self.aws.sqs.ChangeMessageVisibility(&cmv)
				self.alive.Done()
			}
			self.inch <- &e
		}
	}
}

func (self *BalancerControl) awsSend(ctx context.Context, e *Event) error {
	if !self.aws.configured {
		return fmt.Errorf("must call .ConfigureAws() before using it")
	}

	jsonBytes, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = self.aws.sqs.SendMessage(&sqs.SendMessageInput{
		QueueUrl:       aws.String(self.aws.queueUrl),
		MessageGroupId: aws.String(e.Host),
		MessageBody:    aws.String(string(jsonBytes)),
	})
	return err
}

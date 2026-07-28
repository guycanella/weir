package fake

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/guycanella/weir/internal/awsclient"
)

// Method name constants for SQS's InjectError, so callers get typo-safety
// and IDE completion instead of a bare string.
const (
	SQSMethodCreateQueue        = "CreateQueue"
	SQSMethodGetQueueUrl        = "GetQueueUrl"
	SQSMethodGetQueueAttributes = "GetQueueAttributes"
	SQSMethodSetQueueAttributes = "SetQueueAttributes"
	SQSMethodSendMessage        = "SendMessage"
	SQSMethodReceiveMessage     = "ReceiveMessage"
	SQSMethodDeleteMessage      = "DeleteMessage"
)

// ErrQueueNotFound is returned by every SQS fake method that operates on a
// queue URL or name that was never created, mirroring the
// QueueDoesNotExist error real SQS returns.
var ErrQueueNotFound = errors.New("fake sqs: queue not found")

// ErrReceiptHandleNotFound is returned by DeleteMessage when the given
// receipt handle does not correspond to a message currently in flight
// (already deleted, or never received), mirroring the
// ReceiptHandleIsInvalid error real SQS returns.
var ErrReceiptHandleNotFound = errors.New("fake sqs: receipt handle not found")

type sqsMessage struct {
	id                      string
	body                    string
	approximateReceiveCount int
}

type inFlightMessage struct {
	queueURL string
	msg      sqsMessage
}

// SQS is an in-memory fake of awsclient.SQSClient, safe for concurrent use.
//
// It does not model automatic wall-clock visibility-timeout expiry: once a
// message is handed out by ReceiveMessage it stays "in flight" (unavailable
// to further ReceiveMessage calls) until DeleteMessage removes it, or until
// a test manually calls ExpireInFlight to simulate the timeout elapsing.
// Tests that need to exercise WR-024's delete-on-success/redelivery-on-
// failure behavior call ExpireInFlight explicitly instead of sleeping,
// keeping the fake deterministic (ADR-003: no time/rand leaking into the
// pure test double).
//
// Sent, Received and Deleted record everything sent, handed out and
// deleted, keyed by queue URL, so a test can assert on what happened.
type SQS struct {
	mu sync.Mutex

	Queues          map[string]string            // name -> URL
	QueueAttributes map[string]map[string]string // URL -> attributes

	Sent     map[string][]awsclient.Message
	Received map[string][]awsclient.Message
	Deleted  map[string][]string

	pending  map[string][]sqsMessage    // URL -> messages waiting to be received (FIFO)
	inFlight map[string]inFlightMessage // receipt handle -> message

	nextMsgID     int
	nextReceiptID int
	errs          *errorQueue
}

// NewSQS returns an empty, ready-to-use SQS fake.
func NewSQS() *SQS {
	return &SQS{
		Queues:          make(map[string]string),
		QueueAttributes: make(map[string]map[string]string),
		Sent:            make(map[string][]awsclient.Message),
		Received:        make(map[string][]awsclient.Message),
		Deleted:         make(map[string][]string),
		pending:         make(map[string][]sqsMessage),
		inFlight:        make(map[string]inFlightMessage),
		errs:            newErrorQueue(),
	}
}

// InjectError arranges for the next n calls (n < 1 is treated as 1) to the
// named method (one of the SQSMethod* constants) to return err instead of
// performing their normal work.
func (f *SQS) InjectError(method string, err error, n int) {
	f.errs.push(method, err, n)
}

var _ awsclient.SQSClient = (*SQS)(nil)

// CreateQueue implements awsclient.SQSClient. Like real SQS, creating a
// queue with a name that already exists is a no-op that returns the
// existing queue's URL.
func (f *SQS) CreateQueue(_ context.Context, in awsclient.CreateQueueInput) (awsclient.CreateQueueOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SQSMethodCreateQueue); err != nil {
		return awsclient.CreateQueueOutput{}, err
	}

	if url, ok := f.Queues[in.Name]; ok {
		return awsclient.CreateQueueOutput{QueueUrl: url}, nil
	}

	url := fmt.Sprintf("https://queue.local/000000000000/%s", in.Name)
	f.Queues[in.Name] = url

	attrs := make(map[string]string, len(in.Attributes))
	for k, v := range in.Attributes {
		attrs[k] = v
	}
	attrs["QueueArn"] = fmt.Sprintf("arn:aws:sqs:local:000000000000:%s", in.Name)
	f.QueueAttributes[url] = attrs

	return awsclient.CreateQueueOutput{QueueUrl: url}, nil
}

// GetQueueUrl implements awsclient.SQSClient.
func (f *SQS) GetQueueUrl(_ context.Context, in awsclient.GetQueueUrlInput) (awsclient.GetQueueUrlOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SQSMethodGetQueueUrl); err != nil {
		return awsclient.GetQueueUrlOutput{}, err
	}

	url, ok := f.Queues[in.Name]
	if !ok {
		return awsclient.GetQueueUrlOutput{}, ErrQueueNotFound
	}

	return awsclient.GetQueueUrlOutput{QueueUrl: url}, nil
}

// GetQueueAttributes implements awsclient.SQSClient. ApproximateNumberOfMessages
// is always computed live from the queue's current pending message count,
// so tests exercising backlog-driven scaling (WR-031) see it reflect
// SendMessage/ReceiveMessage calls made against this fake.
func (f *SQS) GetQueueAttributes(_ context.Context, in awsclient.GetQueueAttributesInput) (awsclient.GetQueueAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SQSMethodGetQueueAttributes); err != nil {
		return awsclient.GetQueueAttributesOutput{}, err
	}

	stored, ok := f.QueueAttributes[in.QueueUrl]
	if !ok {
		return awsclient.GetQueueAttributesOutput{}, ErrQueueNotFound
	}

	all := make(map[string]string, len(stored)+1)
	for k, v := range stored {
		all[k] = v
	}
	all["ApproximateNumberOfMessages"] = strconv.Itoa(len(f.pending[in.QueueUrl]))

	if len(in.AttributeNames) == 0 {
		return awsclient.GetQueueAttributesOutput{Attributes: all}, nil
	}

	filtered := make(map[string]string, len(in.AttributeNames))
	for _, name := range in.AttributeNames {
		if name == "All" {
			return awsclient.GetQueueAttributesOutput{Attributes: all}, nil
		}
		if v, ok := all[name]; ok {
			filtered[name] = v
		}
	}

	return awsclient.GetQueueAttributesOutput{Attributes: filtered}, nil
}

// SetQueueAttributes implements awsclient.SQSClient. Attributes not
// mentioned in in.Attributes are left unchanged.
func (f *SQS) SetQueueAttributes(_ context.Context, in awsclient.SetQueueAttributesInput) (awsclient.SetQueueAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SQSMethodSetQueueAttributes); err != nil {
		return awsclient.SetQueueAttributesOutput{}, err
	}

	stored, ok := f.QueueAttributes[in.QueueUrl]
	if !ok {
		return awsclient.SetQueueAttributesOutput{}, ErrQueueNotFound
	}

	for k, v := range in.Attributes {
		stored[k] = v
	}

	return awsclient.SetQueueAttributesOutput{}, nil
}

// SendMessage implements awsclient.SQSClient.
func (f *SQS) SendMessage(_ context.Context, in awsclient.SendMessageInput) (awsclient.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SQSMethodSendMessage); err != nil {
		return awsclient.SendMessageOutput{}, err
	}

	if _, ok := f.QueueAttributes[in.QueueUrl]; !ok {
		return awsclient.SendMessageOutput{}, ErrQueueNotFound
	}

	f.nextMsgID++
	id := fmt.Sprintf("msg-%d", f.nextMsgID)

	f.pending[in.QueueUrl] = append(f.pending[in.QueueUrl], sqsMessage{id: id, body: in.Body})
	f.Sent[in.QueueUrl] = append(f.Sent[in.QueueUrl], awsclient.Message{MessageId: id, Body: in.Body})

	return awsclient.SendMessageOutput{MessageId: id}, nil
}

// ReceiveMessage implements awsclient.SQSClient. Messages are handed out
// FIFO, up to in.MaxNumberOfMessages (a value <= 0 is treated as 1,
// matching real SQS's default).
func (f *SQS) ReceiveMessage(_ context.Context, in awsclient.ReceiveMessageInput) (awsclient.ReceiveMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SQSMethodReceiveMessage); err != nil {
		return awsclient.ReceiveMessageOutput{}, err
	}

	if _, ok := f.QueueAttributes[in.QueueUrl]; !ok {
		return awsclient.ReceiveMessageOutput{}, ErrQueueNotFound
	}

	max := int(in.MaxNumberOfMessages)
	if max <= 0 {
		max = 1
	}

	available := f.pending[in.QueueUrl]
	if len(available) > max {
		available = available[:max]
	}
	f.pending[in.QueueUrl] = f.pending[in.QueueUrl][len(available):]

	out := make([]awsclient.Message, 0, len(available))
	for _, m := range available {
		m.approximateReceiveCount++

		f.nextReceiptID++
		receipt := fmt.Sprintf("receipt-%d", f.nextReceiptID)
		f.inFlight[receipt] = inFlightMessage{queueURL: in.QueueUrl, msg: m}

		msg := awsclient.Message{
			MessageId:     m.id,
			ReceiptHandle: receipt,
			Body:          m.body,
			Attributes: map[string]string{
				"ApproximateReceiveCount": strconv.Itoa(m.approximateReceiveCount),
			},
		}
		out = append(out, msg)
	}

	f.Received[in.QueueUrl] = append(f.Received[in.QueueUrl], out...)

	return awsclient.ReceiveMessageOutput{Messages: out}, nil
}

// DeleteMessage implements awsclient.SQSClient.
func (f *SQS) DeleteMessage(_ context.Context, in awsclient.DeleteMessageInput) (awsclient.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SQSMethodDeleteMessage); err != nil {
		return awsclient.DeleteMessageOutput{}, err
	}

	inFlight, ok := f.inFlight[in.ReceiptHandle]
	if !ok || inFlight.queueURL != in.QueueUrl {
		return awsclient.DeleteMessageOutput{}, ErrReceiptHandleNotFound
	}

	delete(f.inFlight, in.ReceiptHandle)
	f.Deleted[in.QueueUrl] = append(f.Deleted[in.QueueUrl], in.ReceiptHandle)

	return awsclient.DeleteMessageOutput{}, nil
}

// ExpireInFlight simulates every message currently in flight on queueURL
// having its visibility timeout elapse: each moves back to the front of
// pending (as if never received) and becomes available to a subsequent
// ReceiveMessage again, still carrying its prior approximateReceiveCount
// so a redelivered message's count keeps climbing across repeated
// receive/expire cycles. Returns the number of messages requeued.
//
// This is a fake-only capability with no equivalent on
// awsclient.SQSClient — real SQS does this automatically on a wall-clock
// timer. Tests that need to simulate WR-024's redelivery-on-failure
// behavior call this explicitly instead of sleeping, keeping the fake
// deterministic (ADR-003: no time/rand leaking into the pure test double).
// The receipt handle a caller was holding for an expired message becomes
// invalid immediately — a subsequent DeleteMessage with it now fails with
// ErrReceiptHandleNotFound, matching real SQS's behavior on an expired
// receipt handle.
func (f *SQS) ExpireInFlight(queueURL string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	var requeued []sqsMessage
	for receipt, inFlight := range f.inFlight {
		if inFlight.queueURL != queueURL {
			continue
		}
		requeued = append(requeued, inFlight.msg)
		delete(f.inFlight, receipt)
	}

	if len(requeued) == 0 {
		return 0
	}

	// Relative order among messages expired in the same call isn't
	// preserved beyond "they go to the front of pending" — map iteration
	// order is unspecified in Go.
	f.pending[queueURL] = append(requeued, f.pending[queueURL]...)

	return len(requeued)
}

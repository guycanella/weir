package awsclient

import "context"

// SQSClient is the subset of SQS operations Weir needs: provisioning the
// main queue and its DLQ (WR-018), sending load-generated messages
// (WR-048), and the worker's long-poll consume/delete loop (WR-021,
// WR-024).
type SQSClient interface {
	// CreateQueue creates a queue, or reports the existing one if a queue
	// with that name already exists (SQS's own CreateQueue is idempotent
	// on name). Used for both the DLQ and the main queue.
	CreateQueue(ctx context.Context, in CreateQueueInput) (CreateQueueOutput, error)

	// GetQueueUrl looks up a queue's URL by name, so a caller can check
	// whether a queue already exists before creating it (WR-018's
	// idempotent lookup-by-name).
	GetQueueUrl(ctx context.Context, in GetQueueUrlInput) (GetQueueUrlOutput, error)

	// GetQueueAttributes reads a queue's attributes: its ARN (for wiring a
	// redrive policy), and later its approximate backlog depth (WR-031's
	// scaling poll).
	GetQueueAttributes(ctx context.Context, in GetQueueAttributesInput) (GetQueueAttributesOutput, error)

	// SetQueueAttributes sets a queue's attributes: visibility timeout and
	// redrive policy (WR-018).
	SetQueueAttributes(ctx context.Context, in SetQueueAttributesInput) (SetQueueAttributesOutput, error)

	// SendMessage sends a single message to a queue. Used by the load
	// generator (WR-048).
	SendMessage(ctx context.Context, in SendMessageInput) (SendMessageOutput, error)

	// ReceiveMessage long-polls a queue for messages. Used by the worker's
	// consume loop (WR-021).
	ReceiveMessage(ctx context.Context, in ReceiveMessageInput) (ReceiveMessageOutput, error)

	// DeleteMessage removes a message from a queue by its receipt handle.
	// The worker calls this only after successful processing
	// (delete-on-success, WR-024) — a message left undeleted becomes
	// visible again after its visibility timeout and eventually reaches
	// the DLQ if it keeps failing.
	DeleteMessage(ctx context.Context, in DeleteMessageInput) (DeleteMessageOutput, error)
}

// CreateQueueInput names the queue to create and its initial attributes
// (e.g. VisibilityTimeout, RedrivePolicy), keyed the same way SQS itself
// keys queue attributes.
type CreateQueueInput struct {
	Name       string
	Attributes map[string]string
}

// CreateQueueOutput reports the created (or already-existing) queue's URL.
type CreateQueueOutput struct {
	QueueUrl string
}

// GetQueueUrlInput names the queue to look up.
type GetQueueUrlInput struct {
	Name string
}

// GetQueueUrlOutput reports the looked-up queue's URL.
type GetQueueUrlOutput struct {
	QueueUrl string
}

// GetQueueAttributesInput identifies the queue and which attributes to
// read. An empty AttributeNames requests every attribute.
type GetQueueAttributesInput struct {
	QueueUrl       string
	AttributeNames []string
}

// GetQueueAttributesOutput carries the requested attribute values.
type GetQueueAttributesOutput struct {
	Attributes map[string]string
}

// SetQueueAttributesInput identifies the queue and the attributes to set.
// Attributes not mentioned are left unchanged.
type SetQueueAttributesInput struct {
	QueueUrl   string
	Attributes map[string]string
}

// SetQueueAttributesOutput carries no data on success.
type SetQueueAttributesOutput struct{}

// SendMessageInput describes a message to send.
type SendMessageInput struct {
	QueueUrl string
	Body     string
}

// SendMessageOutput reports the sent message's ID.
type SendMessageOutput struct {
	MessageId string
}

// ReceiveMessageInput configures a receive call: how many messages to
// return at most, how long to wait for one to become available (long
// polling), and how long a received message stays invisible to other
// receivers before it is eligible for redelivery.
type ReceiveMessageInput struct {
	QueueUrl            string
	MaxNumberOfMessages int32
	WaitTimeSeconds     int32
	VisibilityTimeout   int32
}

// ReceiveMessageOutput carries the messages received, if any.
type ReceiveMessageOutput struct {
	Messages []Message
}

// Message is a single message received from a queue.
type Message struct {
	MessageId     string
	ReceiptHandle string
	Body          string

	// Attributes carries SQS system attributes relevant to Weir, such as
	// ApproximateReceiveCount.
	Attributes map[string]string
}

// DeleteMessageInput identifies the message to delete by its queue and
// receipt handle.
type DeleteMessageInput struct {
	QueueUrl      string
	ReceiptHandle string
}

// DeleteMessageOutput carries no data on success.
type DeleteMessageOutput struct{}

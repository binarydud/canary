package canary

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/mailgun/oxy/utils"
	"github.com/mailgun/vulcand/Godeps/_workspace/src/github.com/codegangsta/cli"
	"github.com/mailgun/vulcand/plugin"
	"github.com/satori/go.uuid"
	"io"
	"log"
    "time"
    "encoding/json"
	"net/http"
)

const Type = "canary"

func GetSpec() *plugin.MiddlewareSpec {
	return &plugin.MiddlewareSpec{
		Type:      Type,       // A short name for the middleware
		FromOther: FromOther,  // Tells vulcand how to create middleware from another one
		FromCli:   FromCli,    // Tells vulcand how to create middleware from CLI
		CliFlags:  CliFlags(), // Vulcand will add this flags CLI command
	}
}

type CanaryMiddleware struct {
	BrokerList []string
}
type Canary struct {
	cfg  CanaryMiddleware
	next http.Handler
}

func (c *Canary) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	pw := &utils.ProxyWriter{W: w}
	c.next.ServeHTTP(pw, req)
	l := c.newRecord(req, pw, time.Since(start))
	if err := json.NewEncoder(c.writer).Encode(l); err != nil {
		c.log.Errorf("Failed to marshal request: %v", err)
	}
}
func New(brokers []string) (*CanaryMiddleware, error) {
	if len(brokers) < 1 {
		return nil, fmt.Errorf("Cannot have an empty broker list")
	}
	return &CanaryMiddleware{BrokerList: brokers}, nil
}
func (c *CanaryMiddleware) NewHandler(next http.Handler) (http.Handler, error) {
	return &CanaryHandler{next: next, cfg: *c}, nil
}
func (c *CanaryMiddleware) String() string {
	return fmt.Sprintf("brokers=%v", c.BrokerList)
}
func FromOther(c CanaryMiddleware) (plugin.Middleware, error) {
	return New(c.BrokerList)
}
func FromCli(c *cli.Context) (plugin.Middleware, error) {
	return New(c.StringSlice("brokers"))
}

func CliFlags() []cli.Flag {
	empty := make(cli.StringSlice, 0)
	return []cli.Flag{
		cli.StringSliceFlag{"brokers", &empty, "Kafka Broker List", ""},
	}
}

func newWriter(brokers []string) (io.Writer, error) {
	dataCollector := newDataCollector(brokers)
	return &kafkaWriter{s: dataCollector}, nil
}

type kafkaWriter struct {
	s sarama.SyncProducer
}

func newDataCollector(brokerList []string) sarama.SyncProducer {

	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

func (k *kafkaWriter) Write(val []byte) (int, error) {
	u1 := uuid.NewV4()
	_, _, err := k.s.SendMessage(&sarama.ProducerMessage{
		Topic: "important",
		Key:   sarama.StringEncoder(u1.String()),
		Value: sarama.ByteEncoder(val),
	})
	return 1, err
}
func (c *Canary) newRecord(req *http.Request, pw *utils.ProxyWriter, diff time.Duration) *Record {
    return &Record{
		Request: Request{
			Method:    req.Method,
			URL:       req.URL.String(),
			TLS:       newTLS(req),
			BodyBytes: bodyBytes(req.Header),
			Headers:   captureHeaders(req.Header, t.reqHeaders),
            Body: captureBody(req.Body),
		},
		Response: Response{
			Code:      pw.StatusCode(),
			BodyBytes: bodyBytes(pw.Header()),
			Roundtrip: float64(diff) / float64(time.Millisecond),
			Headers:   captureHeaders(pw.Header(), t.respHeaders),
            Body:      captureBody()
		},
	}
}
func captureBody(body io.ReadCloser) ([]byte, error) {
    b, err := ioutil.ReadAll(body)
    return b, err
}
func captureHeaders(in http.Header) http.Header {
	if in == nil {
		return nil
	}
	out := make(http.Header, len(in))
	for h, vals := range in {
		for i := range vals {
			out.Add(h, vals[i])
		}
	}
	return out
}

type Record struct {
	Request  Request  `json:"request"`
	Response Response `json:"response"`
}

type Request struct {
	Method    string      `json:"method"`            // Method - request method
	BodyBytes int64       `json:"body_bytes"`        // BodyBytes - size of request body in bytes
	URL       string      `json:"url"`               // URL - Request URL
	Headers   http.Header `json:"headers,omitempty"` // Headers - optional request headers, will be recorded if configured
	TLS       *TLS        `json:"tls,omitempty"`     // TLS - optional TLS record, will be recorded if it's a TLS connection
	Body      string      `json:"body"`              // Body of the Request
}

// Resp contains information about HTTP response
type Response struct {
	Code      int         `json:"code"`              // Code - response status code
	Roundtrip float64     `json:"roundtrip"`         // Roundtrip - round trip time in milliseconds
	Headers   http.Header `json:"headers,omitempty"` // Headers - optional headers, will be recorded if configured
	BodyBytes int64       `json:"body_bytes"`        // BodyBytes - size of response body in bytes
	Body      string      `json:"body"`              // Body sent back to the requesting client
}

// TLS contains information about this TLS connection
type TLS struct {
	Version     string `json:"version"`      // Version - TLS version
	Resume      bool   `json:"resume"`       // Resume tells if the session has been re-used (session tickets)
	CipherSuite string `json:"cipher_suite"` // CipherSuite contains cipher suite used for this connection
	Server      string `json:"server"`       // Server contains server name used in SNI
}

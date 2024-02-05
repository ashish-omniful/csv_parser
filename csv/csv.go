package csv

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"os"
)

const defaultBatchSize = 100

// Headers represents a slice of strings for CSV headers
type Headers []string

// Records represents a 2D slice of strings for CSV records
type Records [][]string

// ToMaps converts records to a slice of maps using headers as keys
func (records Records) ToMaps(headers Headers) []map[string]string {
	var keyValueMaps []map[string]string

	for _, record := range records {
		keyValueMap := make(map[string]string)

		// Iterate through the header and record values to create key-value pairs
		for i, headerValue := range headers {
			keyValueMap[headerValue] = record[i]
		}

		// Append the key-value map to the slice
		keyValueMaps = append(keyValueMaps, keyValueMap)
	}

	return keyValueMaps
}

func (records Records) Unmarshal(headers Headers, data interface{}) (err error) {
	if records == nil {
		err = fmt.Errorf("nil pointer reference")
		return
	}

	recordMap := records.ToMaps(headers)
	// Convert the JSON objects to a JSON-encoded string
	jsonData, err := json.Marshal(recordMap)
	if err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}

	err = json.Unmarshal(jsonData, data)
	if err != nil {
		fmt.Println(err)
		return
	}

	return
}

func (h Headers) ToStringSlice() []string {
	return []string(h)
}

type FileInfo struct {
	ObjectKey string
	Bucket    string
}

type CommonCSV struct {
	Reader    *csv.Reader
	Headers   Headers
	batchSize int
	eof       bool
	rawData   []byte
	fileInfo  FileInfo
}

// Options is a struct to hold optional parameters for CommonCSV initialization
type Options struct {
	Headers   Headers
	BatchSize int
	CSVReader *csv.Reader
	fileInfo  FileInfo
}

// OptionFunc is a function type for setting options
type OptionFunc func(*Options)

// WithHeaders sets the headers option
func WithHeaders(headers Headers) OptionFunc {
	return func(o *Options) {
		o.Headers = headers
	}
}

// WithBatchSize sets the batch size option
func WithBatchSize(batchSize int) OptionFunc {
	return func(o *Options) {
		o.BatchSize = batchSize
	}
}

// WithCSVReader sets the csv.Reader option
func WithCSVReader(reader *csv.Reader) OptionFunc {
	return func(o *Options) {
		o.CSVReader = reader
	}
}

func WithFileInfo(objectKey, bucket string) OptionFunc {
	return func(o *Options) {
		o.fileInfo = FileInfo{
			ObjectKey: objectKey,
			Bucket:    bucket,
		}
	}
}

// NewCommonCSV initializes a CommonCSV instance with options
func NewCommonCSV(options ...OptionFunc) (*CommonCSV, error) {
	opts := &Options{
		BatchSize: defaultBatchSize, // Default value
	}

	for _, option := range options {
		option(opts)
	}

	// Use the provided csv.Reader if available, otherwise create a new one
	csvReader := opts.CSVReader
	if opts.Headers != nil {
		csvReader.FieldsPerRecord = len(opts.Headers)
	}

	return &CommonCSV{
		Reader:    opts.CSVReader,
		Headers:   opts.Headers,
		batchSize: opts.BatchSize,
		fileInfo:  opts.fileInfo,
	}, nil
}

// ReadNextBatch reads the next batch of records from the CSV data
func (commonCSV *CommonCSV) ReadNextBatch() (records Records, err error) {
	if commonCSV == nil {
		return
	}

	if commonCSV.Headers == nil {
		commonCSV.ParseHeaders()
	}

	for i := 0; i < commonCSV.batchSize; i++ {
		record, err := commonCSV.Reader.Read()
		if err == io.EOF {
			commonCSV.SetEOF()
			break
		} else if err != nil {
			return nil, fmt.Errorf("error reading CSV record: %w", err)
		}

		records = append(records, record)
	}

	return records, nil
}

func (commonCSV *CommonCSV) SetEOF() {
	if commonCSV == nil {
		return
	}

	commonCSV.eof = true
}

func (commonCSV *CommonCSV) IsEOF() bool {
	if commonCSV == nil {
		return true
	}

	return commonCSV.eof
}

// ParseHeaders parses and retrieves CSV headers
func (commonCSV *CommonCSV) ParseHeaders() (headers Headers, err error) {
	if commonCSV == nil {
		return
	}

	if commonCSV.Headers != nil {
		return commonCSV.Headers, nil
	}

	headers, err = commonCSV.Reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV headers: %w", err)
	}

	commonCSV.Headers = headers
	return headers, nil
}

// GetHeaders gets headers without re-parsing if already parsed
func (commonCSV *CommonCSV) GetHeaders() (Headers, error) {
	if commonCSV.Headers == nil {
		return commonCSV.ParseHeaders()
	}
	return commonCSV.Headers, nil
}

func (commonCSV *CommonCSV) ParseNextBatch(data interface{}) (err error) {
	if commonCSV == nil {
		err = fmt.Errorf("nil pointer reference")
		return
	}

	headers, err := commonCSV.ParseHeaders()
	if err != nil {
		return
	}

	records, err := commonCSV.ReadNextBatch()
	if err != nil {
		return
	}

	err = records.Unmarshal(headers, data)
	if err != nil {
		return
	}

	return
}

func (commonCSV *CommonCSV) S3Download(ctx context.Context, objectKey string, bucket string) ([]byte, error) {
	if objectKey == "" || bucket == "" {
		return nil, fmt.Errorf("objectkey or bucket is missing")
	}
	// Create an AWS session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	})
	if err != nil {
		return nil, fmt.Errorf("error creating AWS session: %v", err)
	}

	// Create an S3 service client
	svc := s3.New(sess)

	// Create an S3.GetObjectInput to specify the bucket and object to download
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	}

	// Use the S3 service client to download the object
	result, err := svc.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error downloading object from S3: %w", err)
	}

	defer result.Body.Close()

	objectData, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading object data: %w", err)
	}

	commonCSV.rawData = objectData
	return objectData, nil
}

func (commonCSV *CommonCSV) NewCSVReaderClient(ctx context.Context) (csvReader *csv.Reader) {
	if commonCSV == nil {
		return
	}

	// if file already downloaded use that to initialize reader
	if commonCSV.rawData != nil {
		reader := bytes.NewReader(commonCSV.rawData)
		csvReader = csv.NewReader(reader)
	}

	return
}

// function to read csv from local
func (commonCSV *CommonCSV) FileFromLocal(ctx context.Context) ([]byte, error) {

	//file, err := os.Open("orders.csv")
	objectData, err := os.ReadFile("orders_update.csv")
	if err != nil {
		return nil, fmt.Errorf("error reading object data: %w", err)
	}

	commonCSV.rawData = objectData
	return objectData, nil
}

func (commonCSV *CommonCSV) InitializeS3CSVReader(ctx context.Context) (err error) {
	if commonCSV == nil {
		err = fmt.Errorf("nil pointer reference")
		return
	}

	// if file already downloaded use that to initialize reader
	if commonCSV.rawData != nil {
		reader := bytes.NewReader(commonCSV.rawData)
		commonCSV.Reader = csv.NewReader(reader)
	}

	// Download S3 File and create csvReader client
	//rawData, err := commonCSV.S3Download(ctx, commonCSV.fileInfo.ObjectKey, commonCSV.fileInfo.Bucket)
	//if err != nil {
	//	return
	//}

	// get the file from local
	rawData, err := commonCSV.FileFromLocal(ctx)
	if err != nil {
		return
	}

	reader := bytes.NewReader(rawData)
	commonCSV.Reader = csv.NewReader(reader)

	return
}

//
//func main() {
//	// new common csv object creation with filInfo Option
//	// commonCSV.InitializeS3CSVReader
//	// while (!commonCSV.IsEOF()) {
//	// var req csvOrderModel
//	//  commonCSV.ParseNextBatch(data)
//	//   process batch on application layer
//	//   process(req)
//	// }
//
//	commonCSV, err := NewCommonCSV(WithBatchSize(5),
//		WithFileInfo("14451/sales/orders/NSAF70001822077#814_58620.csv", "production-noon-partners"))
//	if err != nil {
//		return
//	}
//	commonCSV.InitializeS3CSVReader(context.TODO())
//
//	for {
//		if commonCSV.IsEOF() {
//			break
//		}
//		var req interface{}
//		commonCSV.ParseNextBatch(&req)
//		fmt.Println(req)
//		fmt.Println("Request Processed")
//		fmt.Println(".........................")
//
//	}
//
//	fmt.Println("done")
//}

// Processes a file from S3 inbound bucket, transforms its contents and calls API to store that content, moves file to S3 archive

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type ColumnsType struct {
	TransactionID   int `json:"transactionID"`
	TransactionDate int `json:"transactionDate"`
	FirstName       int `json:"firstName"`
	LastName        int `json:"lastName"`
}

type Company struct {
	CompanyID string      `json:"companyID"`
	Columns   ColumnsType `json:"columns"`
}

// API JSON payload struct
type HeaderType struct {
	CompanyID string `json:"companyID"`
}

type RecordType struct {
	TransactionID   string `json:"transactionID"`
	TransactionDate string `json:"transactionDate"`
	FirstName       string `json:"firstName"`
	LastName        string `json:"lastName"`
}

type Payload struct {
	Header HeaderType   `json:"header"`
	Record []RecordType `json:"record"`
}

func main() {
	lambda.Start(ftpImporttHandler)
}

func ftpImporttHandler(ctx context.Context, s3Event events.S3Event) (string, error) {
	// Single handler invoked by Lambda.  Get env variables set by lambda, init session used for S3 functions, and process all event.s
	// Should only be 1 event?

	fileDef := os.Getenv("FILE_DEF")
	if len(fileDef) == 0 {
		exitErrorf("FILE_DEF environment variable is not set")
	}
	fmt.Println("DEBUG: FILE_DEF", fileDef)

	// Get the JSON file definition
	companyConfig := getFileDef(fileDef)

	// Initialize session
	sess, err := session.NewSession()
	if err != nil {
		exitErrorf("Cannot initialize session")
	}

	// Create S3 service client
	svc := s3.New(sess)

	// Get event info.  A lambda is triggered for every file upload, so we should probably only have a single event
	for _, record := range s3Event.Records {
		s3 := record.S3
		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
		bucket := s3.Bucket.Name
		item := s3.Object.Key
		// Download file from S3 to /tmp/ for processing
		fileName := downloadFromS3(bucket, item, sess, companyConfig)
		// Transform the file and display API json
		payload := transformFile(fileName, companyConfig)
		// Call the API with the JSON payload from above
		callAPI(payload)
		// Move the file to the archive bucket
		archiveS3(bucket, item, sess, svc)
	}

	fmt.Println("Processed event")

	return "Processed files", nil
}

func getFileDef(fileDef string) Company {

	// Initialize our company struct
	companyConfig := Company{}

	// Umarshal our byteArray which contains our
	// json env content into 'companyConfig' which we defined above
	err := json.Unmarshal([]byte(fileDef), &companyConfig)
	if err != nil {
		exitErrorf("Unable to Unmarshal fileDef %q, %v", fileDef, err)
	}

	return companyConfig
}

func downloadFromS3(bucket string, item string, sess *session.Session, companyConfig Company) string {
	downloader := s3manager.NewDownloader(sess)
	file, err := os.Create("/tmp/" + item)
	if err != nil {
		exitErrorf("Unable to create file %q, %v", item, err)
	}

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})

	if err != nil {
		exitErrorf("Unable to download item %q, %v", item, err)
	}

	fmt.Println("DEBUG: downloadFromS3:Downloaded", file.Name(), numBytes, "bytes")
	return file.Name()
}

func transformFile(fileName string, companyConfig Company) *Payload {
	// Open the file and transform to json payload

	file, err := os.Open(fileName)
	if err != nil {
		exitErrorf("Unable to open file %q, %w", fileName, err)
	}
	defer file.Close()

	// Build the JSON payload header
	payload := &Payload{
		Header: HeaderType{
			CompanyID: companyConfig.CompanyID,
		},
	}

	// Add each record to JSON paylod
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// fmt.Println(scanner.Text())
		matched, _ := regexp.MatchString(`(?i)^Transaction`, scanner.Text())
		if !matched {
			strs := strings.Split(scanner.Text(), ",")
			payload.Record = append(payload.Record,
				RecordType{
					TransactionID:   strs[companyConfig.Columns.TransactionID],
					TransactionDate: strs[companyConfig.Columns.TransactionDate],
					FirstName:       strs[companyConfig.Columns.FirstName],
					LastName:        strs[companyConfig.Columns.LastName],
				},
			)
		}
	}

	if err := scanner.Err(); err != nil {
		exitErrorf("Error on scan of file %q, %w", fileName, err)
	}

	return payload
}

func callAPI(payload *Payload) {
	// Display the JSON payload
	payloadpDisp, _ := json.Marshal(&payload)
	fmt.Println("DEBUG JSON Payload", string(payloadpDisp))
	// Call API here
}

func archiveS3(bucket string, item string, sess *session.Session, svc *s3.S3) {
	// Copy  the file to archive bucket
	bucketArch := bucket + "-archive"
	source := bucket + "/" + item
	_, err := svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(bucketArch), CopySource: aws.String(source), Key: aws.String(item)})
	if err != nil {
		exitErrorf("Unable to copy item from bucket %q to bucket %q, %v", bucket, bucketArch, err)
	}

	// Wait to see if the item got copied
	err = svc.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: aws.String(bucketArch), Key: aws.String(item)})
	if err != nil {
		exitErrorf("Error occurred while waiting for item %q to be copied to bucket %q, %v", bucket, item, bucketArch, err)
	}

	fmt.Printf("DEBUG: Item %q successfully copied from bucket %q to bucket %q\n", item, bucket, bucketArch)

	// Delete the original file
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(item)})
	if err != nil {
		exitErrorf("Unable to delete object %q from bucket %q, %v", item, bucket, err)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(item),
	})
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

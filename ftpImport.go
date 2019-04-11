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
	// "log"
	"github.com/pkg/errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// CSV file definition
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
		exitError(errors.New("FILE_DEF environment variable is not set"))
	}
	fmt.Println("DEBUG: FILE_DEF", fileDef)

	// Get the JSON file definition
	companyConfig, err := getFileDef(fileDef)
	if err != nil {
		exitError(err)
	}

	// Initialize session
	sess, err := session.NewSession()
	if err != nil {
		exitError(errors.New("Cannot initialize session"))
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
		fileName, err := downloadFromS3(bucket, item, sess, companyConfig)
		if err != nil {
			exitError(err)
		}
		// Transform the file and display API json
		payloadPtr,err := transformFile(fileName, companyConfig)
		if err != nil {
			exitError(err)
		}
		// Call the API with the JSON payload from above
		err = callAPI(payloadPtr)
		if err != nil {
			exitError(err)
		}
		// Move the file to the archive bucket
		err = archiveS3(bucket, item, sess, svc)
		if err != nil {
			exitError(err)
		}
	}

	fmt.Println("Processed event")

	return "Processed files", nil
}

func getFileDef(fileDef string) (Company, error) {

	// Initialize our company struct
	companyConfig := Company{}

	// Umarshal our byteArray which contains our
	// json env content into 'companyConfig' which we defined above
	err := json.Unmarshal([]byte(fileDef), &companyConfig)
	if err != nil {
		return companyConfig, errors.New("Unable to Unmarshal fileDef " + fileDef)
	}

	return companyConfig, nil
}

func downloadFromS3(bucket string, item string, sess *session.Session, companyConfig Company) (string,error) {
	downloader := s3manager.NewDownloader(sess)
	file, err := os.Create("/tmp/" + item)
	if err != nil {
		return file.Name(), errors.New("Unable to create file " + item)
	}

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})

	if err != nil {
		return file.Name(), errors.New("Unable to download file from S3 " + item)
	}

	fmt.Println("DEBUG: downloadFromS3:Downloaded", file.Name(), numBytes, "bytes")
	return file.Name(), nil
}

func transformFile(fileName string, companyConfig Company) (*Payload, error) {
	// Open the file and transform to json payload

	file, err := os.Open(fileName)
	if err != nil {
		return nil, errors.New("Unable to open file " + fileName)
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
		return nil, errors.New("Error on scan of file " + fileName)
	}

	return payload, nil
}

func callAPI(payload *Payload) error {
	// Display the JSON payload
	payloadpDisp, _ := json.Marshal(&payload)
	fmt.Println("DEBUG JSON Payload", string(payloadpDisp))
	// Call API here
	return nil
}

func archiveS3(bucket string, item string, sess *session.Session, svc *s3.S3) error {
	// Copy  the file to archive bucket
	bucketArch := bucket + "-archive"
	source := bucket + "/" + item
	_, err := svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(bucketArch), CopySource: aws.String(source), Key: aws.String(item)})
	if err != nil {
		return errors.New("Unable to copy " + source + " to bucket " + bucketArch)
	}

	// Wait to see if the item got copied
	err = svc.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: aws.String(bucketArch), Key: aws.String(item)})
	if err != nil {
		return errors.New("Error occurred while waiting for copy " + source + " to " + bucketArch)
	}

	fmt.Printf("DEBUG: Item %q successfully copied from bucket %q to bucket %q\n", item, bucket, bucketArch)

	// Delete the original file
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(item)})
	if err != nil {
		return errors.New("Unable to delete " + source)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(item),
	})

	if err != nil {
		return errors.New("Error occurred while waiting to delete " + source)
	}

	return nil
}

func exitError(err error) {
	fmt.Printf("Error: %+v", err)
	os.Exit(1)
}

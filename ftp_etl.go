// List a bucket
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

func main() {
	lambda.Start(listBucketHandler)
}

func listBucketHandler(ctx context.Context, even events.AppSyncResolverTemplate) (string, error) {
	fmt.Println("Entered list_bucket_handler")

	// Get environment variables
	company := os.Getenv("COMPANY_ID")
	if len(company) == 0 {
		exitErrorf("COMPANY_ID environment variable is not set")
	}

	bucket := os.Getenv("BUCKET")
	if len(bucket) == 0 {
		exitErrorf("BUCKET environment variable is not set")
	}

	fileDef := os.Getenv("FILE_DEF")
	if len(bucket) == 0 {
		exitErrorf("FILE_DEF environment variable is not set")
	}
	fmt.Println("COMPANY_ID,", company, "BUCKET", bucket, "FILE_DEF", fileDef)

	// Get the JSON file definition
	companyConfig := getFileDef(fileDef)
	
	// Initialize session
	sess, err := session.NewSession()

	// Create S3 service client
	svc := s3.New(sess)

	// List the bucket contents
	prefix := company
	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}

	// fmt.Println("Contents of Bucket:	", bucket)
	for _, item := range resp.Contents {
		// fmt.Println("Object Name   ", *item.Key)
		// fmt.Println("Last modified:", *item.LastModified)
		// fmt.Println("Size:         ", *item.Size)
		// fmt.Println("Storage class:", *item.StorageClass)
		// fmt.Println("")

		// Download and transform the file
		downloadFromS3(bucket, *item.Key, sess, company, companyConfig)
		// Move the file to the archive bucket
		archiveS3(bucket, *item.Key, sess, svc)
	}

	fmt.Println("Processed", len(resp.Contents), "items in bucket", bucket)

	return "Processed files in bucket " + bucket, nil
}

func getFileDef(fileDef string)(Company) {

	// Initialize our company struct
	companyConfig := Company{}

	// Umarshal our byteArray which contains our
	// json env content into 'companyConfig' which we defined above
	err := json.Unmarshal([]byte(fileDef), &companyConfig)
	if err != nil {
		exitErrorf("Unable to Unmarshal fileDef %q, %v", fileDef, err)
	}

	// fmt.Println("CompanyID: ", companyConfig.CompanyID)
	// fmt.Println("TransactionID: ", companyConfig.Columns.TransactionID)
	// fmt.Println("TransactionDate: ", companyConfig.Columns.TransactionDate)
	// fmt.Println("FirstName: ", companyConfig.Columns.FirstName)
	// fmt.Println("LasttName: ", companyConfig.Columns.LastName)
	// fmt.Println("LasttName:", companyConfig.Columns.LastName)

	return companyConfig
}

func downloadFromS3(bucket string, item string, sess *session.Session, company string, companyConfig Company) {
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

	fmt.Println("downloadFromS3:Downloaded", file.Name(), numBytes, "bytes")

	transformFile(file.Name(), company, companyConfig)
}

func transformFile(fileName string, company string, companyConfig Company) {
	// Open the file and transform to json payload

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

	type response struct {
		Header HeaderType   `json:"header"`
		Record []RecordType `json:"record"`
	}

	file, err := os.Open(fileName)
	if err != nil {
		exitErrorf("Unable to open file %q, %w", fileName, err)
	}
	defer file.Close()

	// Build the JSON payload header
	resp := &response{
		Header: HeaderType{
			CompanyID: company,
		},
	}

	// Add each record to JSON paylod
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// fmt.Println(scanner.Text())
		matched, _ := regexp.MatchString(`(?i)^Transaction`, scanner.Text())
		if !matched {
			strs := strings.Split(scanner.Text(), ",")

			resp.Record = append(resp.Record,
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

	// Display the JSON payload
	respDisp, _ := json.Marshal(resp)
	fmt.Println("JSON Payload", string(respDisp))
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

	fmt.Printf("Item %q successfully copied from bucket %q to bucket %q\n", item, bucket, bucketArch)

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

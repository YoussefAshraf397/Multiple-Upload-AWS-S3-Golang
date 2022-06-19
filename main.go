package main

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	BucketName  = "YOUR_BUCKET_NAME"
	REGION      = "YOUR_REGION"
	FOLDER_NAME = "YOUR_FOLDER"
	FILE        = "video.mp4"
	PartSize    = 50_000_000
	RETRIES     = 2
)

var s3session *s3.S3

type partUploadResult struct {
	completedPart *s3.CompletedPart
	err           error
}

func init() {
	s3session = s3.New(session.Must(session.NewSession(&aws.Config{
		Region:                        aws.String(REGION),
		Credentials:                   credentials.NewStaticCredentials("AccessKeyId", "SecretAccessKey", "TOKEN"),
		MaxRetries:                    aws.Int(RETRIES),
		CredentialsChainVerboseErrors: aws.Bool(true),
	})))
}

var wg = sync.WaitGroup{}
var ch = make(chan partUploadResult)

func main() {
	file, _ := os.Open(FILE)
	defer file.Close()

	stat, _ := file.Stat()
	fileSize := stat.Size()

	buffer := make([]byte, fileSize)

	_, _ = file.Read(buffer)

	expiryDate := time.Now().AddDate(0, 0, 1)

	createdResp, err := s3session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:  aws.String(BucketName),
		Key:     aws.String("recordings/parallelUploadTest.mp4"),
		Expires: &expiryDate,
	})

	if err != nil {
		fmt.Print(err)
		return
	}

	var start, currentSize int
	var remaining = int(fileSize)
	var partNum = 1
	var completedParts []*s3.CompletedPart
	for start = 0; remaining > 0; start += PartSize {
		wg.Add(1)
		if remaining < PartSize {
			currentSize = remaining
		} else {
			currentSize = PartSize
		}
		go uploadToS3(createdResp, buffer[start:start+currentSize], partNum, &wg)

		remaining -= currentSize
		fmt.Printf("Uplaodind of part %v started and remaning is %v \n", partNum, remaining)
		partNum++

	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for result := range ch {
		if result.err != nil {
			_, err = s3session.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(BucketName),
				Key:      aws.String("recordings/parallelUploadTest.mp4"),
				UploadId: createdResp.UploadId,
			})
			if err != nil {
				fmt.Print(err)
				os.Exit(1)
			}
		}
		fmt.Printf("Uploading of part %v has been finished \n", *result.completedPart.PartNumber)
		completedParts = append(completedParts, result.completedPart)
	}

	// Ordering the array based on the PartNumber as each parts could be uploaded in different order!
	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	// Signalling AWS S3 that the multiPartUpload is finished
	resp, err := s3session.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   createdResp.Bucket,
		Key:      createdResp.Key,
		UploadId: createdResp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	if err != nil {
		fmt.Print(err)
		return
	} else {
		fmt.Println(resp.String())
	}

}

func uploadToS3(resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNum int, wg *sync.WaitGroup) {
	defer wg.Done()
	var try int
	fmt.Printf("Uploading %v \n", len(fileBytes))
	for try <= RETRIES {
		uploadRes, err := s3session.UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(fileBytes),
			Bucket:        resp.Bucket,
			Key:           resp.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      resp.UploadId,
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})
		if err != nil {
			fmt.Println(err)
			if try == RETRIES {
				ch <- partUploadResult{nil, err}
				return
			} else {
				try++
				time.Sleep(time.Duration(time.Second * 15))
			}
		} else {
			ch <- partUploadResult{
				&s3.CompletedPart{
					ETag:       uploadRes.ETag,
					PartNumber: aws.Int64(int64(partNum)),
				}, nil,
			}
			return
		}
	}
	ch <- partUploadResult{}
}

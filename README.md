# In that project you can upload media by downloadable url via AWS-S3

All companies and even individuals prefer to store their files into cloud storage like AWS S3, 
S3 is one of the best Cloud Storages available at our disposal.

### S3, has attractive feature called `Multipart upload` 
This feature lets a service breaks a big file into smaller chunks and then uploads them; once all parts are uploaded, S3 could merge all in one single file. By doing this, you could also benefit from multithreading and start uploading many chunks simultaneously.
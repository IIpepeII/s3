package s3

import (
	"chatbot/core/models/validation"
	"io"
	"strings"
	"time"

	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
)

// Config represents the s3 configuration.
type Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	SSL             bool
	BucketName      string
}

// Validate validates the struct.
func (c Config) Validate() error {
	return validation.ValidateStruct(
		&c,
		validation.Field(&c.Endpoint, validation.Required),
		validation.Field(&c.AccessKeyID, validation.Required),
		validation.Field(&c.SecretAccessKey, validation.Required),
		validation.Field(&c.Region, validation.Required),
		validation.Field(&c.BucketName, validation.Required),
	)
}

// Helper is the helper interface
type Helper interface {
	CreateBucket(name string) error
	CreateDirectory(bucket string, name string) error
	CreateFile(bucket, directory, file string, content io.Reader, length int64, mime string) error
	GetS3Host() string
	BucketExists(bucket string) (bool, error)
	ListOfBucket() ([]string, error)
	ListOfBucketFolder(bucketName string, isRecursive bool) (*Folder, error)
	GetBucketName() string
}

// Folder represents the folder structure in s3.
type Folder struct {
	Name    string
	Folders map[string]*Folder
}

// Add adds a new sub folder to the parent folder.
func (f *Folder) Add(key string, name string) {
	if f.Folders == nil {
		f.Folders = map[string]*Folder{}
	}
	f.Folders[key] = &Folder{Name: name}
}

// Get gets the correct folder for the keys.
func (f *Folder) Get(keys ...string) *Folder {
	for _, key := range keys {
		f = f.Folders[key]
	}
	return f
}

// Set sets the folder name for proper folder.
func (f *Folder) Set(name string, keys ...string) {
	f = f.Get(keys...)
	f.Name = name
}

// helper represents the S3 helper.
type helper struct {
	Enabled bool
	Config  Config
	Client  *minio.Client
}

// New create a new S3 helper instance
func New(config Config) (Helper, error) {
	err := config.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "New Validator")
	}

	s3 := helper{
		Config:  config,
		Enabled: false,
	}

	s3.Client, err = minio.NewWithRegion(config.Endpoint, config.AccessKeyID, config.SecretAccessKey, config.SSL, config.Region)
	if err != nil {
		return nil, errors.Wrap(err, "New minio.NewWithRegion")
	}
	s3.Enabled = true
	return &s3, nil
}

// CreateBucket make new bucket on s3
func (s helper) CreateBucket(name string) error {
	if !s.Enabled {
		return nil
	}

	return s.Client.MakeBucket(name, s.Config.Region)
}

// CreateDirectory make new directory in a bucket
func (s helper) CreateDirectory(bucket, name string) error {
	if !s.Enabled {
		return nil
	}

	opts := minio.PutObjectOptions{
		ContentType: "plain/text",
	}
	reader := strings.NewReader(time.Now().String())

	_, err := s.Client.PutObject(bucket, name+"/.created", reader, int64(reader.Len()), opts)
	if err != nil {
		return err
	}

	return err
}

// CreateFile make new file in specific directory in a specific bucket
func (s helper) CreateFile(bucket, directory, fileName string, content io.Reader, length int64, mime string) error {
	if !s.Enabled {
		return nil
	}

	opts := minio.PutObjectOptions{
		ContentType: mime,
	}

	_, err := s.Client.PutObject(bucket, directory+"/"+fileName, content, length, opts)
	if err != nil {
		return err
	}

	return err
}

// GetS3Host returns S3 host.
func (s helper) GetS3Host() string {
	return s.Config.Endpoint
}

// BucketExists checks the bucket exists or not.
func (s helper) BucketExists(bucket string) (bool, error) {
	if !s.Enabled {
		return false, nil
	}

	exists, err := s.Client.BucketExists(bucket)
	if err, ok := err.(minio.ErrorResponse); ok && (err.Code == "NoSuchBucket") {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "BucketExists failed")
	}
	return exists, nil
}

// ListOfBucket lists the buckets.
func (s helper) ListOfBucket() ([]string, error) {
	if !s.Enabled {
		return nil, nil
	}

	binfos, err := s.Client.ListBuckets()
	if err != nil {
		return nil, errors.Wrap(err, "list failed")
	}

	ret := make([]string, 0)
	for _, binfo := range binfos {
		ret = append(ret, binfo.Name)
	}

	return ret, nil
}

// ListOfBucketFolder lists the buckets folders.
func (s helper) ListOfBucketFolder(bucketName string, isRecursive bool) (*Folder, error) {
	if !s.Enabled {
		return nil, nil
	}

	root := &Folder{Name: bucketName}

	doneCh := make(chan struct{})
	defer close(doneCh)

	objinfo := s.Client.ListObjectsV2(bucketName, "", isRecursive, doneCh)
	for obj := range objinfo {
		if obj.Err != nil {
			return nil, errors.Wrap(obj.Err, "list object error")
		}

		path := strings.Split(obj.Key, "/")
		for i, elem := range path {
			if len(path) == 1 && root.Get(elem) == nil {
				root.Add(elem, elem)
				continue
			}

			parent := root.Get(path[0:i]...)
			parent.Add(elem, elem)
		}
	}

	return root, nil
}

// GetBucketName returns the buckets name.
func (s helper) GetBucketName() string {
	return s.Config.BucketName
}

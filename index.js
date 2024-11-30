const express = require('express');
const { 
  S3Client, 
  ListBucketsCommand, 
  ListObjectsV2Command, 
  PutObjectCommand, 
  GetObjectCommand, 
  DeleteObjectCommand,
  CopyObjectCommand
} = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const multer = require('multer');
const multerS3 = require('multer-s3');
const cors = require('cors');
const dotenv = require('dotenv');
const stream = require('stream');

// Load environment variables from .env file
dotenv.config();

const app = express();
const port = 3000;

// Middleware to handle JSON and CORS
app.use(express.json());
app.use(cors());

// Configure AWS SDK
const s3Client = new S3Client({ region: process.env.AWS_REGION });

// Configure multer for file uploads
const upload = multer({
  storage: multerS3({
    s3: s3Client,
    bucket: process.env.S3_BUCKET_NAME,
    key: function (req, file, cb) {
      const prefix = req.query.prefix || '';
      cb(null, `${prefix}${Date.now()}_${file.originalname}`);
    },
  }),
});

// Helper function to generate pre-signed URL
async function generatePresignedUrl(bucketName, fileKey) {
  const command = new GetObjectCommand({ Bucket: bucketName, Key: fileKey });
  return getSignedUrl(s3Client, command, { expiresIn: 3600 }); // URL valid for 1 hour
}

// Endpoint to generate pre-signed URL
app.get('/generate-presigned-url/:bucketName/:fileKey(*)', async (req, res) => {
  const { bucketName, fileKey } = req.params;

  try {
    const url = await generatePresignedUrl(bucketName, fileKey);
    res.send({ url });
  } catch (error) {
    console.error("Error generating pre-signed URL:", error);
    res.status(500).send(error);
  }
});

// Endpoint to list all buckets
app.get('/list-buckets', async (req, res) => {
  try {
    const command = new ListBucketsCommand({});
    const data = await s3Client.send(command);
    const buckets = data.Buckets.map(bucket => bucket.Name);
    res.send(buckets);
  } catch (error) {
    console.error("List buckets error:", error);
    res.status(500).send(error);
  }
});

// Upload endpoint for multiple files
app.post('/upload/:bucketName', upload.array('files'), (req, res) => {
  if (!req.files || req.files.length === 0) {
    console.log("No files uploaded.");
    return res.status(400).send({ error: 'No files uploaded.' });
  }

  const fileUrls = req.files.map(file => file.location);
  console.log("Files uploaded successfully:", fileUrls);
  res.send({ fileUrls });
});

// Upload endpoint for a single file
app.post('/upload-single/:bucketName', upload.single('file'), (req, res) => {
  if (!req.file) {
    console.log("No file uploaded.");
    return res.status(400).send({ error: 'No file uploaded.' });
  }

  const fileUrl = req.file.location;
  console.log("File uploaded successfully:", fileUrl);
  res.send({ fileUrl });
});

// Endpoint to list files in a bucket
app.get('/list-files/:bucketName', async (req, res) => {
  const params = {
    Bucket: req.params.bucketName,
    Prefix: req.query.prefix === '/' ? '' : req.query.prefix || '',
    MaxKeys: 100, // Limit the number of keys returned per call
    ContinuationToken: req.query.token || undefined, // Handle pagination token
    Delimiter: '/' // Ensure folders are returned
  };

  try {
    const command = new ListObjectsV2Command(params);
    const data = await s3Client.send(command);
    
    // Filter files based on lastFetchTime
    const lastFetchTime = req.query.lastFetchTime ? new Date(req.query.lastFetchTime) : null;
    const filteredFiles = data.Contents ? data.Contents.filter(file => {
      return lastFetchTime ? new Date(file.LastModified) > lastFetchTime : true;
    }).map(file => ({ key: file.Key, lastModified: file.LastModified, size: file.Size })) : [];

    // Include common prefixes (folders) in the response
    const folders = data.CommonPrefixes ? data.CommonPrefixes.map(prefix => ({
      key: prefix.Prefix,
      isFolder: true
    })) : [];

    res.send({
      files: [...filteredFiles, ...folders],
      nextToken: data.NextContinuationToken || null // Return the continuation token if there are more files, or null if not
    });
  } catch (error) {
    console.error("List files error:", error);
    res.status(500).send(error);
  }
});

// Search files endpoint
app.get('/search-files/:bucketName', async (req, res) => {
  const params = {
    Bucket: req.params.bucketName,
    Prefix: req.query.prefix || '', // Optional: filter files by prefix
  };

  try {
    const command = new ListObjectsV2Command(params);
    const data = await s3Client.send(command);
    const files = data.Contents.filter(file => file.Key.includes(req.query.fileName || '')).map(file => ({
      key: file.Key,
      lastModified: file.LastModified,
      size: file.Size,
    }));
    res.send(files);
  } catch (error) {
    console.error("Search files error:", error);
    res.status(500).send(error);
  }
});

// Endpoint to create a folder in S3 with dynamic bucket name
app.post('/create-folder/:bucketName', async (req, res) => {
  const { folderName } = req.body;

  if (!folderName) {
    return res.status(400).send({ error: 'folderName is required' });
  }

  try {
    const command = new PutObjectCommand({
      Bucket: req.params.bucketName,
      Key: `${folderName}/`, // Folder key ends with a forward slash
    });
    await s3Client.send(command);
    res.send({ message: 'Folder created successfully' });
  } catch (error) {
    console.error('Create folder error:', error);
    res.status(500).send(error);
  }
});

// Download file endpoint with dynamic bucket and file key
app.get('/download/:bucketName/:fileKey(*)', async (req, res) => {
  const params = {
    Bucket: req.params.bucketName,
    Key: req.params.fileKey
  };

  try {
    const command = new GetObjectCommand(params);
    const data = await s3Client.send(command);

    res.attachment(req.params.fileKey);
    const passThrough = new stream.PassThrough();
    stream.pipeline(data.Body, passThrough, (err) => {
      if (err) {
        console.error("Pipeline error:", err);
        res.status(500).send(err);
      }
    });
    passThrough.pipe(res);
  } catch (error) {
    console.error("Download file error:", error);
    res.status(500).send(error);
  }
});

// Delete file endpoint with dynamic bucket and file key
app.delete('/delete-file/:bucketName/:fileKey(*)', async (req, res) => {
  const params = {
    Bucket: req.params.bucketName,
    Key: req.params.fileKey,
  };

  try {
    const command = new DeleteObjectCommand(params);
    await s3Client.send(command);
    res.send({ message: 'File deleted successfully' });
  } catch (error) {
    console.error("Delete file error:", error);
    res.status(500).send(error);
  }
});



// Endpoint to copy a file within the same S3 bucket
app.post('/copy-file/:bucketName', async (req, res) => {
  const { bucketName } = req.params;
  const { sourceKey, targetKey } = req.body;

  if (!sourceKey || !targetKey) {
    return res.status(400).send({ error: 'sourceKey and targetKey are required' });
  }

  const params = {
    Bucket: bucketName,
    CopySource: `${bucketName}/${sourceKey}`,
    Key: targetKey,
  };

  console.log("Copying file with params:", params); // Debugging log

  try {
    const command = new CopyObjectCommand(params);
    await s3Client.send(command);
    res.send({ message: 'File copied successfully' });
  } catch (error) {
    console.error('Copy file error:', error);
    res.status(500).send(error);
  }
});


// Endpoint to move multiple files from one S3 bucket to another
app.post('/move-files', async (req, res) => {
  const { sourceBucket, files, targetBucket } = req.body;

  if (!sourceBucket || !files || !targetBucket || files.length === 0) {
    return res.status(400).send({ error: 'sourceBucket, files, and targetBucket are required' });
  }

  try {
    for (const file of files) {
      const { sourceKey, targetKey } = file;
      
      const copyParams = {
        Bucket: targetBucket,
        CopySource: `${sourceBucket}/${sourceKey}`,
        Key: targetKey,
      };

      const deleteParams = {
        Bucket: sourceBucket,
        Key: sourceKey,
      };

      console.log("Moving file with params:", { copyParams, deleteParams }); // Debugging log

      // Copy the file to the target bucket
      const copyCommand = new CopyObjectCommand(copyParams);
      await s3Client.send(copyCommand);

      // Delete the file from the source bucket
      const deleteCommand = new DeleteObjectCommand(deleteParams);
      await s3Client.send(deleteCommand);
    }

    res.send({ message: 'Files moved successfully' });
  } catch (error) {
    console.error('Move files error:', error);
    res.status(500).send(error);
  }
});


app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

const express = require('express');
const { S3Client, ListObjectsV2Command, CopyObjectCommand, DeleteObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3'); // Added PutObjectCommand here
const { Upload } = require('@aws-sdk/lib-storage');
const multer = require('multer');
const multerS3 = require('multer-s3');
const cors = require('cors');
const dotenv = require('dotenv');

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
      cb(null, `uploads/${Date.now()}_${file.originalname}`);
    },
  }),
});

// Upload endpoint
app.post('/upload', upload.array('files'), (req, res) => {
  if (!req.files || req.files.length === 0) {
    console.log("No files uploaded.");
    return res.status(400).send({ error: 'No files uploaded.' });
  }
  
  const fileUrls = req.files.map(file => file.location);
  console.log("Files uploaded successfully:", fileUrls);
  res.send({ fileUrls });
});

// Search files endpoint
app.get('/search', async (req, res) => {
  const params = {
    Bucket: process.env.S3_BUCKET_NAME,
    Prefix: req.query.prefix, // This is the search keyword
  };

  try {
    const command = new ListObjectsV2Command(params);
    const data = await s3Client.send(command);
    const files = data.Contents.map(file => file.Key);
    res.send(files);
  } catch (error) {
    console.error("Search error:", error);
    res.status(500).send(error);
  }
});

// Archive endpoint
app.post('/archive', async (req, res) => {
  const { sourceKey, archiveKey } = req.body;

  if (!sourceKey || !archiveKey) {
    console.log("sourceKey and archiveKey are required.");
    return res.status(400).send({ error: 'sourceKey and archiveKey are required.' });
  }

  try {
    // Copy the file to the archive location
    const copyParams = {
      Bucket: process.env.S3_BUCKET_NAME,
      CopySource: `${process.env.S3_BUCKET_NAME}/${sourceKey}`,
      Key: `archive/${archiveKey}`,
    };
    await s3Client.send(new CopyObjectCommand(copyParams));

    // Delete the original file
    const deleteParams = {
      Bucket: process.env.S3_BUCKET_NAME,
      Key: sourceKey,
    };
    await s3Client.send(new DeleteObjectCommand(deleteParams));

    console.log("File archived successfully.");
    res.send({ message: 'File archived successfully' });
  } catch (error) {
    console.error("Archive error:", error);
    res.status(500).send(error);
  }
});

// List files endpoint
app.get('/list-files', async (req, res) => {
  const params = {
    Bucket: process.env.S3_BUCKET_NAME,
    Prefix: req.query.prefix || '', // Optional: filter files by prefix
  };

  try {
    const command = new ListObjectsV2Command(params);
    const data = await s3Client.send(command);
    const files = data.Contents ? data.Contents.map(file => ({ key: file.Key, lastModified: file.LastModified, size: file.Size })) : [];
    res.send(files);
  } catch (error) {
    console.error("List files error:", error);
    res.status(500).send(error);
  }
});

// Search files by name endpoint
app.get('/search-files', async (req, res) => {
  const params = {
    Bucket: process.env.S3_BUCKET_NAME,
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

// Endpoint to create a folder in S3
app.post('/create-folder', async (req, res) => {
  const { folderName } = req.body;

  if (!folderName) {
    return res.status(400).send({ error: 'folderName is required' });
  }

  try {
    const command = new PutObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: `${folderName}/`, // Folder key ends with a forward slash
    });
    await s3Client.send(command);
    res.send({ message: 'Folder created successfully' });
  } catch (error) {
    console.error('Create folder error:', error);
    res.status(500).send(error);
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

const express = require('express');
const csvtojson = require('csvtojson');
const multer = require('multer');
const path = require('path');

const app = express();
const port = 8080;

// Set up multer for file uploads
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, 'uploads/');
    },
    filename: (req, file, cb) => {
        cb(null, file.originalname);
    },
});
const upload = multer({ storage: storage });

app.use(express.static('public'));

// Route to upload CSV and convert to JSON
app.post('/upload', upload.single('csvFile'), (req, res) => {
    const csvFilePath = path.join(__dirname, 'uploads', req.file.filename);
    csvtojson()
        .fromFile(csvFilePath)
        .then((jsonObj) => {
            res.setHeader('Content-Disposition', 'attachment; filename=data.json');
            res.setHeader('Content-Type', 'application/json');
            res.send(JSON.stringify(jsonObj, null, 2));
        })
        .catch((err) => res.status(500).send(err.message));
});

app.listen(port, () => {
    console.log(`Server running at http://127.0.0.1:${port}/`);
});

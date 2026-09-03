const express = require('express');
const axios = require('axios');
const app = express();

const PORT = process.env.PORT || 3000;

// 1. Middleware to parse incoming JSON payloads
app.use(express.json());

// 2. Allow cross-origin requests globally (must be placed BEFORE your routes)
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    next();
});

// In-memory storage for the active stream URL
let currentStreamConfig = {
    streamUrl: "https://example.com/placeholder-stream"
};

// Frontend talks to this to find out what URL to play
app.get('/config', (req, res) => {
    res.json(currentStreamConfig);
});

// Your automated background script talks to this to push new stream URLs
app.post('/update-stream', (req, res) => {
    const { newUrl, secretKey } = req.body;

    if (secretKey !== process.env.UPDATE_SECRET) {
        return res.status(403).send('Unauthorized');
    }

    if (!newUrl) {
        return res.status(400).send('Missing newUrl');
    }

    currentStreamConfig.streamUrl = newUrl;
    console.log(`Stream automatically updated to: ${newUrl}`);

    res.status(200).json({ success: true, updatedUrl: newUrl });
});

// Proxy route that feeds clean audio to the Tactical Vibe Machine
app.get('/proxy-stream', async (req, res) => {
    try {
        const targetUrl = currentStreamConfig.streamUrl;
        if (!targetUrl || targetUrl.includes('placeholder')) {
            return res.status(400).send('No active stream configured yet.');
        }

        const response = await axios({
            method: 'get',
            url: targetUrl,
            responseType: 'stream',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
            }
        });

        res.setHeader('Content-Type', response.headers['content-type'] || 'audio/mpeg');
        response.data.pipe(res);
    } catch (error) {
        console.error('Proxy error:', error.message);
        res.status(500).send('Failed to fetch stream');
    }
});

app.listen(PORT, () => {
    console.log(`Stream proxy running on port ${PORT}`);
});

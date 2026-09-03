const express = require('axios');
const expressApp = require('express');
const app = expressApp();

const PORT = process.env.PORT || 3000;

// Middleware to parse incoming JSON payloads from your automated checker script
app.use(expressApp.json());

// In-memory storage for the active stream URL (updates dynamically without manual code pushes)
let currentStreamConfig = {
    streamUrl: "https://example.com/placeholder-stream"
};

// 1. Frontend talks to this to find out what URL to play
app.get('/config', (req, res) => {
    res.json(currentStreamConfig);
});

// 2. Your automated background script talks to this to push new stream URLs
app.post('/update-stream', (req, res) => {
    const { newUrl, secretKey } = req.body;

    // Optional safety check: simple password token so only your script can change it
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

// Allow cross-origin requests from your frontend
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*'); // Or replace '*' with your exact frontend URL
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    next();
});

// 3. Your proxy route that feeds clean audio to the Tactical Vibe Machine
app.get('/proxy-stream', async (expressReq, expressRes) => {
    try {
        const targetUrl = currentStreamConfig.streamUrl;
        if (!targetUrl || targetUrl.includes('placeholder')) {
            return expressRes.status(400).send('No active stream configured yet.');
        }

        const response = await axios({
            method: 'get',
            url: targetUrl,
            responseType: 'stream',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
            }
        });

        expressRes.setHeader('Content-Type', response.headers['content-type'] || 'audio/mpeg');
        response.data.pipe(expressRes);
    } catch (error) {
        console.error('Proxy error:', error.message);
        expressRes.status(500).send('Failed to fetch stream');
    }
});

app.listen(PORT, () => {
    console.log(`Stream proxy running on port ${PORT}`);
});

const express = require('express');
const axios = require('axios');
const app = express();

// Render automatically assigns a port via environment variables, otherwise default to 3000 locally
const PORT = process.env.PORT || 3000;

// Route that your Tactical Vibe Machine will talk to
app.get('/proxy-stream', async (req, res) => {
    try {
        const targetUrl = req.query.url;
        if (!targetUrl) {
            return res.status(400).send('Missing target URL. Use ?url=YOUR_STREAM_URL');
        }

        // Fetch the audio stream with responseType 'stream'
        const response = await axios({
            method: 'get',
            url: targetUrl,
            responseType: 'stream',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' // Mimic a browser so the source doesn't block us
            }
        });

        // Set the appropriate audio headers for your browser player
        res.setHeader('Content-Type', response.headers['content-type'] || 'audio/mpeg');

        // Pipe the incoming audio data directly to your frontend client
        response.data.pipe(res);
    } catch (error) {
        console.error('Proxy error:', error.message);
        res.status(500).send('Failed to fetch stream');
    }
});

app.listen(PORT, () => {
    console.log(`Stream proxy running on port ${PORT}`);
});

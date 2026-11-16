const express = require('express');
const axios = require('axios');
const { randomUUID } = require('node:crypto');
const { pipeline } = require('node:stream/promises');

const app = express();
const PORT = process.env.PORT || 3000;
const RESOURCE_TTL_MS = 30 * 60 * 1000;
const MAX_PLAYLIST_BYTES = 1024 * 1024;

app.set('trust proxy', 1);
app.use(express.json({ limit: '10kb' }));

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header(
        'Access-Control-Allow-Headers',
        'Origin, X-Requested-With, Content-Type, Accept, Range'
    );
    res.header(
        'Access-Control-Allow-Methods',
        'GET, HEAD, POST, OPTIONS'
    );
    res.header(
        'Access-Control-Expose-Headers',
        'Content-Length, Content-Range, Accept-Ranges'
    );

    if (req.method === 'OPTIONS') {
        return res.sendStatus(204);
    }

    next();
});

const allowedHosts = (process.env.STREAM_ALLOWED_HOSTS || '')
    .split(',')
    .map((host) => host.trim().toLowerCase())
    .filter(Boolean);

let currentStreamUrl = process.env.INITIAL_STREAM_URL || '';

// Resource IDs map to upstream playlist/segment URLs. They expire automatically.
const resources = new Map();

function validateUpstreamUrl(value) {
    let url;

    try {
        url = new URL(value);
    } catch {
        throw new Error('Invalid stream URL');
    }

    const hostname = url.hostname.toLowerCase();
    const hostAllowed = allowedHosts.some(
        (host) => hostname === host || hostname.endsWith(`.${host}`)
    );

    if (
        url.protocol !== 'https:' ||
        url.username ||
        url.password ||
        url.port ||
        !hostAllowed
    ) {
        throw new Error('Stream URL host is not allowed');
    }

    return url;
}

function publicBaseUrl(req) {
    const base = process.env.PUBLIC_BASE_URL ||
        `${req.protocol}://${req.get('host')}`;

    return new URL(base).origin;
}

function createProxyUrl(req, upstreamUrl) {
    const validatedUrl = validateUpstreamUrl(upstreamUrl);
    const now = Date.now();

    for (const [id, resource] of resources) {
        if (resource.expiresAt <= now) {
            resources.delete(id);
        }
    }

    const id = randomUUID();
    resources.set(id, {
        url: validatedUrl.href,
        expiresAt: now + RESOURCE_TTL_MS
    });

    return new URL(`/hls-resource/${id}`, publicBaseUrl(req)).href;
}

async function fetchUpstream(startUrl, requestHeaders = {}) {
    let currentUrl = validateUpstreamUrl(startUrl).href;

    for (let redirects = 0; redirects <= 5; redirects += 1) {
        const response = await axios.get(currentUrl, {
            responseType: 'stream',
            timeout: 30000,
            maxRedirects: 0,
            decompress: false,
            validateStatus: () => true,
            headers: {
                'User-Agent': 'Mozilla/5.0',
                ...requestHeaders
            }
        });

        const location = response.headers.location;
        const isRedirect = [301, 302, 303, 307, 308].includes(response.status);

        if (isRedirect && location) {
            response.data.destroy();

            if (redirects === 5) {
                throw new Error('Too many upstream redirects');
            }

            currentUrl = validateUpstreamUrl(
                new URL(location, currentUrl).href
            ).href;
            continue;
        }

        return { response, finalUrl: currentUrl };
    }

    throw new Error('Unable to fetch upstream resource');
}

async function readPlaylist(stream) {
    const chunks = [];
    let totalBytes = 0;

    for await (const chunk of stream) {
        totalBytes += chunk.length;

        if (totalBytes > MAX_PLAYLIST_BYTES) {
            stream.destroy();
            throw new Error('Playlist is too large');
        }

        chunks.push(chunk);
    }

    return Buffer.concat(chunks).toString('utf8');
}

function isPlaylist(contentType, url) {
    return contentType.toLowerCase().includes('mpegurl') ||
        new URL(url).pathname.toLowerCase().endsWith('.m3u8');
}

function rewritePlaylist(text, baseUrl, req) {
    return text.split(/\r?\n/).map((line) => {
        if (line.trim() === '') {
            return line;
        }

        if (line.startsWith('#')) {
            return line.replace(/URI="([^"]+)"/g, (_match, uri) => {
                const upstreamUrl = new URL(uri, baseUrl).href;
                return `URI="${createProxyUrl(req, upstreamUrl)}"`;
            });
        }

        const upstreamUrl = new URL(line.trim(), baseUrl).href;
        return createProxyUrl(req, upstreamUrl);
    }).join('\n');
}

async function serveUpstream(req, res, upstreamUrl) {
    try {
        const requestHeaders = {};
        if (req.headers.range) {
            requestHeaders.Range = req.headers.range;
        }

        const { response, finalUrl } = await fetchUpstream(
            upstreamUrl,
            requestHeaders
        );
        const contentType = response.headers['content-type'] || '';

        if (
            response.status >= 200 &&
            response.status < 300 &&
            isPlaylist(contentType, finalUrl)
        ) {
            const playlist = await readPlaylist(response.data);
            const rewritten = rewritePlaylist(playlist, finalUrl, req);

            res.status(response.status);
            res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
            res.setHeader('Cache-Control', 'no-store');
            return res.send(rewritten);
        }

        res.status(response.status);

        for (const header of [
            'content-type',
            'content-length',
            'content-range',
            'accept-ranges',
            'cache-control',
            'content-encoding'
        ]) {
            if (response.headers[header]) {
                res.setHeader(header, response.headers[header]);
            }
        }

        await pipeline(response.data, res);
    } catch {
        console.error('Upstream proxy request failed');

        if (res.headersSent) {
            res.destroy();
        } else {
            res.status(502).send('Failed to fetch stream resource');
        }
    }
}

app.get('/config', (req, res) => {
    res.json({
        streamUrl: new URL('/proxy-stream', publicBaseUrl(req)).href
    });
});

app.post('/update-stream', (req, res) => {
    if (!process.env.UPDATE_SECRET) {
        return res.status(503).send('UPDATE_SECRET is not configured');
    }

    if (req.body.secretKey !== process.env.UPDATE_SECRET) {
        return res.status(403).send('Unauthorized');
    }

    if (allowedHosts.length === 0) {
        return res.status(503).send('STREAM_ALLOWED_HOSTS is not configured');
    }

    try {
        currentStreamUrl = validateUpstreamUrl(req.body.newUrl).href;
        return res.status(200).json({ success: true });
    } catch {
        return res.status(400).send('Invalid or unapproved stream URL');
    }
});

app.get('/proxy-stream', (req, res) => {
    if (!currentStreamUrl) {
        return res.status(503).send('No stream URL has been configured');
    }

    return serveUpstream(req, res, currentStreamUrl);
});

app.get('/hls-resource/:id', (req, res) => {
    const resource = resources.get(req.params.id);

    if (!resource || resource.expiresAt <= Date.now()) {
        resources.delete(req.params.id);
        return res.status(404).send('Stream resource expired; reload the playlist');
    }

    return serveUpstream(req, res, resource.url);
});

app.listen(PORT, () => {
    console.log(`Stream proxy running on port ${PORT}`);
});

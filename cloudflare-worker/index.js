const ALLOWED_HOSTNAME = 'cdn.discordapp.com';
const ALLOWED_PROTOCOL = 'https:';

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': '*',
};

const buildCorsHeaders = (baseHeaders = {}) => {
  const headers = new Headers(baseHeaders);
  for (const [key, value] of Object.entries(CORS_HEADERS)) {
    headers.set(key, value);
  }
  return headers;
};

const pickForwardHeaders = (incomingHeaders) => {
  const forward = new Headers();
  const allowed = [
    'range',
    'accept',
    'accept-language',
    'accept-encoding',
    'if-none-match',
    'if-modified-since',
    'user-agent',
  ];

  for (const [key, value] of incomingHeaders) {
    if (allowed.includes(key.toLowerCase())) {
      forward.set(key, value);
    }
  }

  return forward;
};

export default {
  async fetch(request) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: buildCorsHeaders() });
    }

    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405, headers: buildCorsHeaders() });
    }

    const requestUrl = new URL(request.url);
    const targetParam = requestUrl.searchParams.get('url');

    if (!targetParam) {
      return new Response('Missing url query parameter', { status: 400, headers: buildCorsHeaders() });
    }

    let targetUrl;
    try {
      targetUrl = new URL(targetParam);
    } catch (error) {
      return new Response('Invalid url query parameter', { status: 400, headers: buildCorsHeaders() });
    }

    if (targetUrl.protocol !== ALLOWED_PROTOCOL || targetUrl.hostname !== ALLOWED_HOSTNAME) {
      return new Response('Forbidden: only cdn.discordapp.com is allowed', {
        status: 403,
        headers: buildCorsHeaders(),
      });
    }

    try {
      const upstreamResponse = await fetch(targetUrl.toString(), {
        method: 'GET',
        headers: pickForwardHeaders(request.headers),
        redirect: 'follow',
      });

      const headers = buildCorsHeaders(upstreamResponse.headers);
      return new Response(upstreamResponse.body, {
        status: upstreamResponse.status,
        statusText: upstreamResponse.statusText,
        headers,
      });
    } catch (error) {
      return new Response('Failed to reach Discord CDN', { status: 502, headers: buildCorsHeaders() });
    }
  },
};

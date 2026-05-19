/**
 * S3 presigning helper.
 *
 * Used by:
 *   - GET /api/calls/:id/recording-url (B10 from the frontend audit)
 *   - GET /api/batches/:id/download-url (returns presigned URL in body)
 *   - GET /api/batches/:id/results (302 redirect to presigned URL)
 *
 * Lambda runtime (Node.js 22.x) provides @aws-sdk/* clients, so these
 * imports don't need to be in production dependencies — they're in
 * devDependencies for local test runs. Promote to dependencies only
 * if the Lambda runtime ever drops the bundled SDK.
 *
 * Shape contract:
 *   presignS3Url(rawPath, opts) → string | null
 *     - rawPath: stored value from voice_calls.recording_path or
 *       voice_batches.output_file_path. May be:
 *         "s3://bucket/key/path"   — presigned and returned
 *         "https://...amazonaws..." — passed through (already a URL)
 *         "https://..." (any host)  — passed through
 *         null / "" / undefined     — returns null
 *     - opts.expiresInSecs: signed-URL TTL in seconds (default 900 = 15 min)
 *     - opts.client: optional S3 client (caller-supplied for tests)
 *     - opts.defaultBucket: bucket to use when rawPath is just a key
 *       (no s3:// prefix). Defaults to process.env.S3_BUCKET.
 *
 * Returns null when the path can't be turned into a usable URL. Never
 * throws — the caller decides what to do with a null (typically 200
 * with `url: null`).
 */

import { S3Client } from '@aws-sdk/client-s3';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

let _client = null;

function defaultClient() {
  if (!_client) {
    _client = new S3Client({
      region: process.env.AWS_REGION || 'us-east-1',
    });
  }
  return _client;
}

/**
 * Parse "s3://bucket/key/path" into { bucket, key } parts. Returns
 * null for anything that isn't an s3:// URI.
 */
export function parseS3Uri(uri) {
  if (typeof uri !== 'string') return null;
  if (!uri.startsWith('s3://')) return null;
  const rest = uri.slice('s3://'.length);
  const slashIdx = rest.indexOf('/');
  if (slashIdx <= 0 || slashIdx === rest.length - 1) return null;
  return { bucket: rest.slice(0, slashIdx), key: rest.slice(slashIdx + 1) };
}

/**
 * Decide whether a stored path string is already a usable HTTPS URL
 * (in which case we just return it). Covers values like
 * "https://...s3.amazonaws.com/..." that older pipeline writes may
 * have stored directly.
 */
function looksLikeHttpsUrl(value) {
  return typeof value === 'string' &&
    (value.startsWith('https://') || value.startsWith('http://'));
}

export async function presignS3Url(rawPath, opts = {}) {
  if (!rawPath) return null;
  if (looksLikeHttpsUrl(rawPath)) return rawPath;

  const expiresInSecs = Math.min(Math.max(opts.expiresInSecs || 900, 60), 3600);
  const client = opts.client || defaultClient();
  const defaultBucket = opts.defaultBucket ?? process.env.S3_BUCKET;

  const parsed = parseS3Uri(rawPath);
  let bucket;
  let key;
  if (parsed) {
    ({ bucket, key } = parsed);
  } else if (defaultBucket && !rawPath.startsWith('/')) {
    // Treat bare key (no scheme, no leading slash) as living in the
    // default bucket. This matches what the pipeline historically
    // wrote when S3_BUCKET was implicit.
    bucket = defaultBucket;
    key = rawPath;
  } else {
    return null;
  }

  try {
    const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
    const url = await getSignedUrl(client, cmd, { expiresIn: expiresInSecs });
    return url;
  } catch (e) {
    console.error('[s3-presign] failed to sign URL', {
      bucket, key, error: e.message,
    });
    return null;
  }
}

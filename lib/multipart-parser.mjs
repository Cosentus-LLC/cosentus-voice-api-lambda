/**
 * Multipart form-data parser for Lambda + API Gateway events.
 *
 * Used by:
 *   - POST /api/batches/upload (B7 from the frontend audit)
 *
 * Why this exists: API Gateway delivers the raw multipart body in
 * `event.body` (base64-encoded when binary content is present). The
 * Lambda handler parses JSON via parseRoute() and falls back to {} on
 * a parse error, which silently swallows the multipart body. This
 * helper gives the upload route a real parse path.
 *
 * Shape contract:
 *   parseMultipart(event) → Promise<{
 *     fields: Record<string, string>,
 *     files: Array<{
 *       fieldname: string,
 *       filename: string,
 *       mimeType: string,
 *       buffer: Buffer,
 *     }>,
 *   }>
 *
 * Rejects when:
 *   - Content-Type is not multipart/form-data
 *   - busboy throws (malformed body, missing boundary, etc.)
 *
 * Returns empty `fields`/`files` arrays if the body is empty.
 *
 * Defensive limits:
 *   - 25 MB total body size (Lambda's hard limit is ~6 MB for sync
 *     invocations; we're below that to leave headroom for response
 *     payload and base64 expansion)
 *   - 100 fields max
 *   - 5 files max
 *
 * Caller is responsible for deciding what to do with the parsed
 * file contents (CSV / Excel parsing, validation, etc.).
 */

import Busboy from 'busboy';

const MAX_BODY_BYTES = 25 * 1024 * 1024;
const MAX_FIELDS = 100;
const MAX_FILES = 5;
const MAX_FILE_BYTES = 25 * 1024 * 1024;

function extractContentType(event) {
  const headers = event.headers || {};
  // API Gateway lowercases header keys for HTTP API but preserves
  // case for REST API. Search both forms.
  return headers['content-type'] ||
    headers['Content-Type'] ||
    headers['CONTENT-TYPE'] ||
    '';
}

function isMultipart(contentType) {
  return typeof contentType === 'string' &&
    contentType.toLowerCase().startsWith('multipart/form-data');
}

export function parseMultipart(event) {
  return new Promise((resolve, reject) => {
    const contentType = extractContentType(event);
    if (!isMultipart(contentType)) {
      reject(new Error(
        `Expected multipart/form-data Content-Type, got: ${contentType || '(empty)'}`,
      ));
      return;
    }

    const rawBody = event.body;
    if (!rawBody) {
      resolve({ fields: {}, files: [] });
      return;
    }

    const body = event.isBase64Encoded
      ? Buffer.from(rawBody, 'base64')
      : Buffer.from(rawBody);

    if (body.length > MAX_BODY_BYTES) {
      reject(new Error(
        `Multipart body exceeds ${MAX_BODY_BYTES} byte limit (got ${body.length})`,
      ));
      return;
    }

    const fields = {};
    const files = [];
    let aborted = false;

    const bb = Busboy({
      headers: { 'content-type': contentType },
      limits: {
        fields: MAX_FIELDS,
        files: MAX_FILES,
        fileSize: MAX_FILE_BYTES,
      },
    });

    bb.on('field', (name, value) => {
      if (aborted) return;
      fields[name] = value;
    });

    bb.on('file', (fieldname, fileStream, info) => {
      if (aborted) return;
      const chunks = [];
      let truncated = false;
      fileStream.on('data', (chunk) => chunks.push(chunk));
      fileStream.on('limit', () => { truncated = true; });
      fileStream.on('end', () => {
        if (truncated) {
          aborted = true;
          reject(new Error(`File "${info.filename}" exceeds ${MAX_FILE_BYTES} byte limit`));
          return;
        }
        files.push({
          fieldname,
          filename: info.filename || '',
          mimeType: info.mimeType || 'application/octet-stream',
          buffer: Buffer.concat(chunks),
        });
      });
      fileStream.on('error', (err) => {
        aborted = true;
        reject(err);
      });
    });

    bb.on('filesLimit', () => {
      if (aborted) return;
      aborted = true;
      reject(new Error(`Multipart body exceeds ${MAX_FILES} file limit`));
    });

    bb.on('fieldsLimit', () => {
      if (aborted) return;
      aborted = true;
      reject(new Error(`Multipart body exceeds ${MAX_FIELDS} field limit`));
    });

    bb.on('error', (err) => {
      if (aborted) return;
      aborted = true;
      reject(err);
    });

    bb.on('close', () => {
      if (aborted) return;
      resolve({ fields, files });
    });

    bb.end(body);
  });
}

/**
 * Parse a single uploaded spreadsheet file (CSV or Excel) into a
 * normalized { columns, rows } shape.
 *
 * Used by the batch upload route after the multipart parse step.
 *
 * Returns:
 *   {
 *     columns: string[]            — header row values (in file order)
 *     rows: Array<Record<string, string>>
 *   }
 *
 * Detection:
 *   - .csv / text/csv → CSV
 *   - .xlsx / .xls / application/vnd.* → Excel via xlsx library
 *   - Anything else → treated as CSV (lenient fallback)
 *
 * For Excel files: first sheet only. Empty rows skipped. All cell
 * values coerced to strings (engine expects string-typed case data).
 *
 * Throws when the file is empty or the xlsx parser fails.
 */
export function parseSpreadsheet(file) {
  if (!file || !file.buffer) {
    throw new Error('parseSpreadsheet: file buffer is required');
  }

  const filename = (file.filename || '').toLowerCase();
  const mimeType = (file.mimeType || '').toLowerCase();
  const isExcel =
    filename.endsWith('.xlsx') ||
    filename.endsWith('.xls') ||
    mimeType.includes('spreadsheetml') ||
    mimeType.includes('ms-excel');

  // Import lazily so the CSV-only path doesn't pay the xlsx load cost
  // and so the xlsx security advisory doesn't fire on every cold
  // start (Excel files are the only thing that touches xlsx code).
  if (isExcel) {
    // eslint-disable-next-line import/no-extraneous-dependencies
    return parseExcelBuffer(file.buffer);
  }
  return parseCsvBuffer(file.buffer);
}

function parseCsvBuffer(buffer) {
  const text = buffer.toString('utf8').replace(/^\uFEFF/, ''); // strip BOM
  const lines = text.split(/\r?\n/).filter(line => line.length > 0);
  if (!lines.length) {
    throw new Error('CSV file is empty');
  }
  const columns = parseCsvLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]);
    const row = {};
    for (let c = 0; c < columns.length; c++) {
      row[columns[c]] = cells[c] !== undefined ? cells[c] : '';
    }
    rows.push(row);
  }
  return { columns, rows };
}

// Minimal RFC 4180 CSV line parser. Handles double-quoted fields
// with embedded commas and escaped quotes ("" → "). Sufficient for
// the Cosentus dialer-list use case; not a full streaming parser.
function parseCsvLine(line) {
  const result = [];
  let i = 0;
  let cur = '';
  let inQuotes = false;
  while (i < line.length) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        cur += '"';
        i += 2;
      } else if (ch === '"') {
        inQuotes = false;
        i++;
      } else {
        cur += ch;
        i++;
      }
    } else {
      if (ch === ',') {
        result.push(cur);
        cur = '';
        i++;
      } else if (ch === '"' && cur === '') {
        inQuotes = true;
        i++;
      } else {
        cur += ch;
        i++;
      }
    }
  }
  result.push(cur);
  return result.map(v => v.trim());
}

async function parseExcelBuffer(buffer) {
  const xlsx = await import('xlsx');
  const wb = xlsx.read(buffer, { type: 'buffer' });
  const sheetName = wb.SheetNames[0];
  if (!sheetName) {
    throw new Error('Excel workbook has no sheets');
  }
  const sheet = wb.Sheets[sheetName];
  // header: 1 → array-of-arrays; first row is the header.
  const aoa = xlsx.utils.sheet_to_json(sheet, {
    header: 1,
    blankrows: false,
    defval: '',
    raw: false, // coerce to formatted strings
  });
  if (!aoa.length) {
    throw new Error('Excel sheet is empty');
  }
  const columns = aoa[0].map(v => (v == null ? '' : String(v).trim()));
  const rows = [];
  for (let i = 1; i < aoa.length; i++) {
    const cells = aoa[i];
    const row = {};
    let hasValue = false;
    for (let c = 0; c < columns.length; c++) {
      const value = cells[c] != null ? String(cells[c]) : '';
      row[columns[c]] = value;
      if (value.length > 0) hasValue = true;
    }
    if (hasValue) rows.push(row);
  }
  return { columns, rows };
}

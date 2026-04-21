const AIRTABLE_TABLE_NAME = "Beta Sign-ups";

function log(scope, message, details) {
  if (typeof details === "undefined") {
    console.log(`[webhook][${scope}] ${message}`);
    return;
  }

  console.log(`[webhook][${scope}] ${message}`, details);
}

function errorLog(scope, message, details) {
  if (typeof details === "undefined") {
    console.error(`[webhook][${scope}] ${message}`);
    return;
  }

  console.error(`[webhook][${scope}] ${message}`, details);
}

function safeJson(value) {
  try {
    return JSON.stringify(value, null, 2);
  } catch (error) {
    return `[unserializable:${error.message}]`;
  }
}

function sanitizeForLog(value) {
  if (value === null || typeof value === "undefined") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((item) => sanitizeForLog(item));
  }

  if (typeof value === "object") {
    const output = {};

    for (const [key, item] of Object.entries(value)) {
      if (/token|authorization|secret|password/i.test(key)) {
        output[key] = "[redacted]";
      } else {
        output[key] = sanitizeForLog(item);
      }
    }

    return output;
  }

  return value;
}

function parseMaybeJson(value) {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();

  if (!trimmed) {
    return null;
  }

  if (
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  ) {
    try {
      return JSON.parse(trimmed);
    } catch (_error) {
      return null;
    }
  }

  return null;
}

function assignNestedValue(target, key, value) {
  if (!key.includes("[")) {
    if (typeof target[key] === "undefined") {
      target[key] = value;
      return;
    }

    if (Array.isArray(target[key])) {
      target[key].push(value);
      return;
    }

    target[key] = [target[key], value];
    return;
  }

  const parts = key.split(/\[|\]/).filter(Boolean);
  let cursor = target;

  for (let index = 0; index < parts.length; index += 1) {
    const part = parts[index];
    const isLast = index === parts.length - 1;

    if (isLast) {
      if (typeof cursor[part] === "undefined") {
        cursor[part] = value;
      } else if (Array.isArray(cursor[part])) {
        cursor[part].push(value);
      } else {
        cursor[part] = [cursor[part], value];
      }
      return;
    }

    if (
      !cursor[part] ||
      typeof cursor[part] !== "object" ||
      Array.isArray(cursor[part])
    ) {
      cursor[part] = {};
    }

    cursor = cursor[part];
  }
}

function parseFormEncoded(input) {
  const parsed = {};
  const params = new URLSearchParams(input);

  for (const [key, value] of params.entries()) {
    const maybeJson = parseMaybeJson(value);
    assignNestedValue(parsed, key, maybeJson ?? value);
  }

  return parsed;
}

async function readRawBody(req) {
  if (typeof req.body === "string") {
    return req.body;
  }

  if (Buffer.isBuffer(req.body)) {
    return req.body.toString("utf8");
  }

  if (req.body && typeof req.body === "object") {
    return safeJson(req.body);
  }

  const chunks = [];

  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks).toString("utf8");
}

function mergeObjects(base, extra) {
  if (!extra || typeof extra !== "object" || Array.isArray(extra)) {
    return base;
  }

  for (const [key, value] of Object.entries(extra)) {
    if (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      base[key] &&
      typeof base[key] === "object" &&
      !Array.isArray(base[key])
    ) {
      mergeObjects(base[key], value);
    } else if (typeof base[key] === "undefined") {
      base[key] = value;
    }
  }

  return base;
}

function normalizeBodyFromRequest(rawBody, req) {
  const contentType = String(req.headers["content-type"] || "").toLowerCase();
  let parsedBody = {};

  if (req.body && typeof req.body === "object" && !Buffer.isBuffer(req.body)) {
    parsedBody = { ...req.body };
  } else if (contentType.includes("application/json")) {
    parsedBody = parseMaybeJson(rawBody) || {};
  } else if (contentType.includes("application/x-www-form-urlencoded")) {
    parsedBody = parseFormEncoded(rawBody);
  } else {
    parsedBody =
      parseMaybeJson(rawBody) ||
      (rawBody.includes("=") ? parseFormEncoded(rawBody) : {});
  }

  const payloadValue =
    parsedBody.payload ||
    req.body?.payload ||
    (typeof parsedBody === "string" ? parsedBody : null);

  const parsedPayload = parseMaybeJson(payloadValue);

  if (parsedPayload && typeof parsedPayload === "object") {
    mergeObjects(parsedBody, parsedPayload);
  }

  if (
    rawBody &&
    contentType.includes("application/x-www-form-urlencoded") &&
    rawBody.includes("=")
  ) {
    mergeObjects(parsedBody, parseFormEncoded(rawBody));
  }

  return parsedBody;
}

function pickFirstNonEmpty(...values) {
  for (const value of values) {
    if (Array.isArray(value)) {
      const nested = pickFirstNonEmpty(...value);
      if (typeof nested !== "undefined") {
        return nested;
      }
      continue;
    }

    if (typeof value === "string") {
      if (value.trim()) {
        return value;
      }
      continue;
    }

    if (value !== null && typeof value !== "undefined") {
      return value;
    }
  }

  return undefined;
}

function normalizeEmail(value) {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  return normalized || null;
}

function normalizeString(value) {
  if (Array.isArray(value)) {
    return normalizeString(value[0]);
  }

  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim();
  return normalized || null;
}

function normalizeFullName(firstName, lastName) {
  const parts = [normalizeString(firstName), normalizeString(lastName)].filter(
    Boolean
  );

  if (!parts.length) {
    return null;
  }

  return parts.join(" ");
}

function normalizeInterest(value) {
  if (typeof value === "undefined" || value === null) {
    return [];
  }

  if (Array.isArray(value)) {
    return [...new Set(value.map((item) => normalizeString(item)).filter(Boolean))];
  }

  if (typeof value === "string") {
    const parsedJson = parseMaybeJson(value);

    if (Array.isArray(parsedJson)) {
      return normalizeInterest(parsedJson);
    }

    const trimmed = value.trim();

    if (!trimmed) {
      return [];
    }

    if (trimmed.includes("|")) {
      return normalizeInterest(trimmed.split("|"));
    }

    if (trimmed.includes(",")) {
      return normalizeInterest(trimmed.split(","));
    }

    return [trimmed];
  }

  return [];
}

function extractCoreFields(body) {
  const email = normalizeEmail(
    pickFirstNonEmpty(body.email, body["contact[email]"], body.contact?.email)
  );

  const contactId = normalizeString(
    pickFirstNonEmpty(
      body.contact_id,
      body["contact[id]"],
      body.contact?.id
    )
  );

  const firstName = normalizeString(
    pickFirstNonEmpty(body["contact[first_name]"], body.contact?.first_name)
  );

  const lastName = normalizeString(
    pickFirstNonEmpty(body["contact[last_name]"], body.contact?.last_name)
  );

  const zone = normalizeString(
    pickFirstNonEmpty(
      body.zone,
      body["contact[zone]"],
      body["contact[fields][zone]"],
      body.contact?.zone,
      body.contact?.fields?.zone
    )
  );

  const interest = normalizeInterest(
    pickFirstNonEmpty(
      body.interest_area,
      body["contact[interest_area]"],
      body["contact[fields][interest_area]"],
      body.contact?.interest_area,
      body.contact?.fields?.interest_area
    )
  );

  return {
    email,
    contactId,
    firstName,
    lastName,
    fullName: normalizeFullName(firstName, lastName),
    zone,
    interest,
  };
}

function getEnv(name) {
  const value = process.env[name];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();
  let data = null;

  try {
    data = text ? JSON.parse(text) : null;
  } catch (_error) {
    data = text;
  }

  return { response, data };
}

function parseActiveCampaignFieldValue(rawValue) {
  if (typeof rawValue === "undefined" || rawValue === null) {
    return [];
  }

  if (Array.isArray(rawValue)) {
    return normalizeInterest(rawValue);
  }

  if (typeof rawValue !== "string") {
    return [];
  }

  const trimmed = rawValue.trim();

  if (!trimmed) {
    return [];
  }

  const parsedJson = parseMaybeJson(trimmed);

  if (Array.isArray(parsedJson)) {
    return normalizeInterest(parsedJson);
  }

  if (trimmed.includes("||")) {
    return normalizeInterest(trimmed.split("||"));
  }

  return normalizeInterest(trimmed);
}

function resolveActiveCampaignCustomFields(fieldDefinitions, fieldValues) {
  const definitionById = new Map();

  for (const field of fieldDefinitions) {
    definitionById.set(String(field.id), field);
  }

  let zone = null;
  let interest = [];

  for (const fieldValue of fieldValues) {
    const definition = definitionById.get(String(fieldValue.field));

    if (!definition) {
      continue;
    }

    const candidates = [
      definition.title,
      definition.perstag,
      definition.personalization,
      definition.type,
    ]
      .filter(Boolean)
      .map((value) => String(value).trim().toLowerCase());

    if (
      !zone &&
      candidates.some(
        (candidate) =>
          candidate === "zone" ||
          candidate === "%zone%" ||
          candidate === "ac_zone"
      )
    ) {
      zone = normalizeString(fieldValue.value);
    }

    if (
      !interest.length &&
      candidates.some(
        (candidate) =>
          candidate === "interest area" ||
          candidate === "interest_area" ||
          candidate === "%interest_area%" ||
          candidate === "ac_interest_area"
      )
    ) {
      interest = parseActiveCampaignFieldValue(fieldValue.value);
    }
  }

  return { zone, interest };
}

async function fetchActiveCampaignFallback(contactId) {
  const apiUrl = getEnv("ACTIVECAMPAIGN_API_URL");
  const apiToken = getEnv("ACTIVECAMPAIGN_API_KEY");

  if (!contactId) {
    return { zone: null, interest: [], skipped: "missing_contact_id" };
  }

  if (!apiUrl || !apiToken) {
    return { zone: null, interest: [], skipped: "missing_activecampaign_env" };
  }

  const normalizedBase = apiUrl.replace(/\/+$/, "");
  const headers = {
    "Api-Token": apiToken,
    Accept: "application/json",
  };

  try {
    const contactUrl = `${normalizedBase}/api/3/contacts/${encodeURIComponent(
      contactId
    )}`;
    const fieldValuesUrl = `${normalizedBase}/api/3/contacts/${encodeURIComponent(
      contactId
    )}/fieldValues`;
    const fieldsUrl = `${normalizedBase}/api/3/fields`;

    log(
      "activecampaign",
      "Fetching fallback data from ActiveCampaign",
      sanitizeForLog({ contactId, contactUrl, fieldValuesUrl, fieldsUrl })
    );

    const [
      { response: contactResponse, data: contactData },
      { response: fieldValuesResponse, data: fieldValuesData },
      { response: fieldsResponse, data: fieldsData },
    ] = await Promise.all([
      fetchJson(contactUrl, { headers }),
      fetchJson(fieldValuesUrl, { headers }),
      fetchJson(fieldsUrl, { headers }),
    ]);

    log(
      "activecampaign",
      "Contact response",
      sanitizeForLog({
        status: contactResponse.status,
        ok: contactResponse.ok,
        data: contactData,
      })
    );
    log(
      "activecampaign",
      "Field values response",
      sanitizeForLog({
        status: fieldValuesResponse.status,
        ok: fieldValuesResponse.ok,
        data: fieldValuesData,
      })
    );
    log(
      "activecampaign",
      "Field definitions response",
      sanitizeForLog({
        status: fieldsResponse.status,
        ok: fieldsResponse.ok,
        data: fieldsData,
      })
    );

    if (!contactResponse.ok || !fieldValuesResponse.ok || !fieldsResponse.ok) {
      return {
        zone: null,
        interest: [],
        firstName: null,
        lastName: null,
        fullName: null,
        error: "activecampaign_lookup_failed",
      };
    }

    const fieldValues = Array.isArray(fieldValuesData?.fieldValues)
      ? fieldValuesData.fieldValues
      : [];
    const fieldDefinitions = Array.isArray(fieldsData?.fields)
      ? fieldsData.fields
      : [];

    const resolved = resolveActiveCampaignCustomFields(
      fieldDefinitions,
      fieldValues
    );
    const contact = contactData?.contact || {};
    const firstName = normalizeString(contact.firstName || contact.first_name);
    const lastName = normalizeString(contact.lastName || contact.last_name);
    const fullName = normalizeFullName(firstName, lastName);

    log(
      "activecampaign",
      "Resolved fallback values",
      sanitizeForLog({
        ...resolved,
        firstName,
        lastName,
        fullName,
      })
    );

    return {
      ...resolved,
      firstName,
      lastName,
      fullName,
    };
  } catch (error) {
    errorLog("activecampaign", "Fallback request failed", {
      message: error.message,
      stack: error.stack,
    });
    return {
      zone: null,
      interest: [],
      firstName: null,
      lastName: null,
      fullName: null,
      error: "activecampaign_request_failed",
    };
  }
}

function escapeAirtableFormulaString(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

async function findAirtableRecordByEmail(email, apiKey, baseId) {
  const formula = `LOWER({Email Address})="${escapeAirtableFormulaString(email)}"`;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
    AIRTABLE_TABLE_NAME
  )}?maxRecords=1&filterByFormula=${encodeURIComponent(formula)}`;

  log(
    "airtable",
    "Looking up record",
    sanitizeForLog({ email, formula, url })
  );

  const { response, data } = await fetchJson(url, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      Accept: "application/json",
    },
  });

  log(
    "airtable",
    "Lookup response",
    sanitizeForLog({
      status: response.status,
      ok: response.ok,
      data,
    })
  );

  return { response, data };
}

function buildAirtableFields({ fullName, zone, interest }) {
  const fields = {};

  if (normalizeString(fullName)) {
    fields["Full Name"] = normalizeString(fullName);
  }

  if (normalizeString(zone)) {
    fields.Zone = normalizeString(zone);
  }

  if (Array.isArray(interest) && interest.length > 0) {
    fields["Interest Area"] = normalizeInterest(interest);
  }

  return fields;
}

async function updateAirtableRecord(recordId, fields, apiKey, baseId) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
    AIRTABLE_TABLE_NAME
  )}/${recordId}`;
  const payload = {
    fields,
    typecast: true,
  };

  log("airtable", "Updating record", sanitizeForLog({ recordId, payload, url }));

  const { response, data } = await fetchJson(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(payload),
  });

  log(
    "airtable",
    "Update response",
    sanitizeForLog({
      status: response.status,
      ok: response.ok,
      data,
    })
  );

  return { response, data };
}

async function updateAirtableWithFallbacks(recordId, fields, apiKey, baseId) {
  const entries = Object.entries(fields);
  const primaryAttempt = await updateAirtableRecord(recordId, fields, apiKey, baseId);

  if (primaryAttempt.response.ok || entries.length <= 1) {
    return {
      ok: primaryAttempt.response.ok,
      response: primaryAttempt.response,
      data: primaryAttempt.data,
      mode: "single",
      appliedFields: primaryAttempt.response.ok ? Object.keys(fields) : [],
      failedFields: primaryAttempt.response.ok ? [] : Object.keys(fields),
    };
  }

  log(
    "airtable",
    "Combined update failed, retrying fields individually",
    sanitizeForLog({
      recordId,
      attemptedFields: Object.keys(fields),
      failure: primaryAttempt.data,
    })
  );

  const appliedFields = [];
  const failedFields = [];
  const results = [];

  for (const [fieldName, fieldValue] of entries) {
    const attempt = await updateAirtableRecord(
      recordId,
      { [fieldName]: fieldValue },
      apiKey,
      baseId
    );

    results.push({
      fieldName,
      status: attempt.response.status,
      ok: attempt.response.ok,
      data: attempt.data,
    });

    if (attempt.response.ok) {
      appliedFields.push(fieldName);
    } else {
      failedFields.push(fieldName);
    }
  }

  log(
    "airtable",
    "Individual retry results",
    sanitizeForLog({ recordId, appliedFields, failedFields, results })
  );

  const lastResult = results[results.length - 1] || null;

  return {
    ok: appliedFields.length > 0,
    response: lastResult
      ? { ok: appliedFields.length > 0, status: lastResult.status }
      : primaryAttempt.response,
    data: {
      primaryAttempt: primaryAttempt.data,
      retries: results,
      appliedFields,
      failedFields,
    },
    mode: "fallback",
    appliedFields,
    failedFields,
  };
}

module.exports = async (req, res) => {
  const requestId =
    req.headers["x-vercel-id"] ||
    req.headers["x-request-id"] ||
    `local-${Date.now()}`;

  log("request", "Webhook invocation started", {
    requestId,
    method: req.method,
    contentType: req.headers["content-type"] || null,
  });

  if (req.method !== "POST") {
    errorLog("request", "Rejected non-POST request", {
      requestId,
      method: req.method,
    });
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const rawBody = await readRawBody(req);
    const body = normalizeBodyFromRequest(rawBody, req);

    log("request", "RAW BODY", rawBody || "[empty]");
    log("request", "BODY KEYS", Object.keys(body || {}));
    log("request", "PARSED BODY", sanitizeForLog(body));

    const parsed = extractCoreFields(body);
    let fullName = parsed.fullName;

    log("parsed", "Normalized values", sanitizeForLog(parsed));

    if (!parsed.email) {
      errorLog("validation", "Missing email in webhook payload", {
        requestId,
        body: sanitizeForLog(body),
      });
      return res.status(400).json({
        error: "Missing email",
        requestId,
      });
    }

    let zone = parsed.zone;
    let interest = parsed.interest;

    if ((!zone || !interest.length) && parsed.contactId) {
      log("activecampaign", "Attempting fallback lookup", {
        requestId,
        contactId: parsed.contactId,
        needsZone: !zone,
        needsInterest: !interest.length,
      });

      const fallback = await fetchActiveCampaignFallback(parsed.contactId);

      if (!zone && fallback.zone) {
        zone = fallback.zone;
      }

      if (!interest.length && fallback.interest.length) {
        interest = fallback.interest;
      }

      if (!fullName && fallback.fullName) {
        fullName = fallback.fullName;
      }

      log(
        "activecampaign",
        "Fallback merge result",
        sanitizeForLog({ fullName, zone, interest, fallback })
      );
    } else if (!parsed.contactId) {
      log(
        "activecampaign",
        "Skipping fallback because contactId is missing",
        { requestId }
      );
    }

    const airtableApiKey = getEnv("AIRTABLE_API_KEY");
    const airtableBaseId = getEnv("AIRTABLE_BASE_ID");

    if (!airtableApiKey || !airtableBaseId) {
      errorLog("config", "Missing Airtable environment variables", {
        hasApiKey: Boolean(airtableApiKey),
        hasBaseId: Boolean(airtableBaseId),
      });
      return res.status(500).json({
        error: "Missing Airtable configuration",
        requestId,
      });
    }

    const lookupResult = await findAirtableRecordByEmail(
      parsed.email,
      airtableApiKey,
      airtableBaseId
    );

    if (!lookupResult.response.ok) {
      errorLog("airtable", "Lookup failed", sanitizeForLog(lookupResult.data));
      return res.status(lookupResult.response.status).json({
        error: "Airtable lookup failed",
        requestId,
        details: lookupResult.data,
      });
    }

    const record = Array.isArray(lookupResult.data?.records)
      ? lookupResult.data.records[0]
      : null;

    if (!record) {
      errorLog("airtable", "No Airtable record matched email", {
        requestId,
        email: parsed.email,
      });
      return res.status(404).json({
        error: "No match found in Airtable",
        requestId,
        email: parsed.email,
      });
    }

    const fields = buildAirtableFields({
      fullName,
      zone,
      interest,
    });

    if (!Object.keys(fields).length) {
      log("airtable", "No valid Airtable fields to update", {
        requestId,
        recordId: record.id,
      });
      return res.status(200).json({
        success: true,
        requestId,
        email: parsed.email,
        recordId: record.id,
        updated: false,
        reason: "No valid fields to update",
      });
    }

    const updateResult = await updateAirtableWithFallbacks(
      record.id,
      fields,
      airtableApiKey,
      airtableBaseId
    );

    if (!updateResult.ok) {
      errorLog("airtable", "Update failed", sanitizeForLog(updateResult.data));
      return res.status(updateResult.response.status || 422).json({
        error: "Airtable update failed",
        requestId,
        details: updateResult.data,
      });
    }

    log("request", "Webhook invocation completed successfully", {
      requestId,
      email: parsed.email,
      recordId: record.id,
      updatedFields: updateResult.appliedFields,
      failedFields: updateResult.failedFields,
    });

    return res.status(200).json({
      success: true,
      requestId,
      email: parsed.email,
      contactId: parsed.contactId,
      recordId: record.id,
      fields,
      appliedFields: updateResult.appliedFields,
      failedFields: updateResult.failedFields,
      updated: updateResult.data,
    });
  } catch (error) {
    errorLog("request", "Unhandled webhook error", {
      requestId,
      message: error.message,
      stack: error.stack,
    });
    return res.status(500).json({
      error: "Server error",
      requestId,
      message: error.message,
    });
  }
};

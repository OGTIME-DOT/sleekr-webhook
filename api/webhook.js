module.exports = async (req, res) => {
  try {
    const body = req.body || {};

    console.log("=== WEBHOOK DEBUG START ===");
    console.log("RAW BODY:", JSON.stringify(body, null, 2));
    console.log("BODY KEYS:", Object.keys(body));

    const email =
      body.email?.toLowerCase?.().trim?.() ||
      body["contact[email]"]?.toLowerCase?.().trim?.() ||
      body.contact?.email?.toLowerCase?.().trim?.() ||
      null;

    const zone =
      body.zone ||
      body["contact[zone]"] ||
      body.contact?.zone ||
      null;

    const interest =
      body.interest_area ||
      body["contact[interest_area]"] ||
      body.contact?.interest_area ||
      null;

    console.log("PARSED VALUES:");
    console.log("email:", email);
    console.log("zone:", zone);
    console.log("interest:", interest);
    console.log("=== WEBHOOK DEBUG END ===");

    if (!email) {
      return res.status(400).json({
        error: "Missing email",
        receivedBody: body,
      });
    }

    const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
    const BASE_ID = process.env.AIRTABLE_BASE_ID;
    const TABLE_NAME = "Beta Sign-ups";

    const formula = `LOWER({Email Address})="${email}"`;
    const url = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(
      TABLE_NAME
    )}?filterByFormula=${encodeURIComponent(formula)}`;

    const findRes = await fetch(url, {
      headers: {
        Authorization: `Bearer ${AIRTABLE_API_KEY}`,
      },
    });

    const findData = await findRes.json();

    console.log("AIRTABLE LOOKUP STATUS:", findRes.status);
    console.log("AIRTABLE LOOKUP RESPONSE:", JSON.stringify(findData, null, 2));

    if (!findRes.ok) {
      return res.status(findRes.status).json({
        error: "Airtable lookup failed",
        details: findData,
      });
    }

    if (!findData.records || !findData.records.length) {
      return res.status(404).json({
        error: "No match found in Airtable",
        email,
      });
    }

    const recordId = findData.records[0].id;

    const fields = {
      last_zone_updated_at: new Date().toISOString(),
    };

    if (zone) fields.Zone = zone;
    if (interest) {
      fields["Interest Area"] = Array.isArray(interest) ? interest : [interest];
    }

    console.log("AIRTABLE UPDATE FIELDS:", JSON.stringify(fields, null, 2));

    const updateRes = await fetch(
      `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(
        TABLE_NAME
      )}/${recordId}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${AIRTABLE_API_KEY}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields }),
      }
    );

    const updateData = await updateRes.json();

    console.log("AIRTABLE UPDATE STATUS:", updateRes.status);
    console.log("AIRTABLE UPDATE RESPONSE:", JSON.stringify(updateData, null, 2));

    if (!updateRes.ok) {
      return res.status(updateRes.status).json({
        error: "Airtable update failed",
        details: updateData,
      });
    }

    return res.status(200).json({
      success: true,
      email,
      zone,
      interest,
      recordId,
      updated: updateData,
    });
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).json({
      error: "Server error",
      message: err.message,
    });
  }
};

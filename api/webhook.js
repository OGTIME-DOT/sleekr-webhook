module.exports = async (req, res) => {
  try {
    const body = req.body || {};

    const email = body.email?.toLowerCase().trim();
    const zone = body.zone || null;
    const interest = body.interest_area || null;

    if (!email) {
      return res.status(400).json({ error: "Missing email" });
    }

    const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
    const BASE_ID = process.env.AIRTABLE_BASE_ID;
    const TABLE_NAME = "Beta Sign-ups";

    if (!AIRTABLE_API_KEY || !BASE_ID) {
      return res.status(500).json({
        error: "Missing Airtable environment variables",
        hasApiKey: !!AIRTABLE_API_KEY,
        hasBaseId: !!BASE_ID,
      });
    }

    const formula = `LOWER({Email Address})="${email}"`;
    const url = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE_NAME)}?filterByFormula=${encodeURIComponent(formula)}`;

    const findRes = await fetch(url, {
      headers: {
        Authorization: `Bearer ${AIRTABLE_API_KEY}`,
      },
    });

    const findData = await findRes.json();

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

    const updateRes = await fetch(
      `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE_NAME)}/${recordId}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${AIRTABLE_API_KEY}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          fields: {
            Zone: zone,
            "Interest Area": interest,
            last_zone_updated_at: new Date().toISOString(),
          },
        }),
      }
    );

    const updateData = await updateRes.json();

    if (!updateRes.ok) {
      return res.status(updateRes.status).json({
        error: "Airtable update failed",
        details: updateData,
      });
    }

    return res.status(200).json({
      success: true,
      email,
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

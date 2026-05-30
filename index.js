import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY;

const BOUNDING_BOXES = [
  [[34.0, 22.8], [41.3, 30.8]]
];

const HISTORY_INTERVAL_MS =
  5 * 60 * 1000;

if (
  !AISSTREAM_API_KEY ||
  !SUPABASE_URL ||
  !SUPABASE_SERVICE_ROLE_KEY
) {
  console.error(
    "Eksik environment variable var."
  );

  process.exit(1);
}

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: {
      persistSession: false
    }
  }
);

let ws;
let reconnectTimer = null;

let messageCount = 0;
let upsertCount = 0;
let historyCount = 0;

const lastHistoryByMmsi =
  new Map();

function getInnerMessage(message) {
  if (
    !message ||
    typeof message !== "object"
  )
    return {};

  const values =
    Object.values(message);

  return values.length
    ? values[0]
    : {};
}

function normalizeAisMessage(raw) {
  const metadata =
    raw.MetaData || {};

  const inner =
    getInnerMessage(raw.Message);

  const mmsi = String(
    metadata.MMSI ||
      metadata.MMSI_String ||
      inner.UserID ||
      ""
  ).trim();

  if (!mmsi) return null;

  const latitude = Number(
    metadata.latitude ??
      inner.Latitude
  );

  const longitude = Number(
    metadata.longitude ??
      inner.Longitude
  );

  if (
    !Number.isFinite(latitude) ||
    !Number.isFinite(longitude)
  ) {
    return null;
  }

  const sog =
    Number.isFinite(
      Number(inner.Sog)
    )
      ? Number(inner.Sog)
      : null;

  const cog =
    Number.isFinite(
      Number(inner.Cog)
    )
      ? Number(inner.Cog)
      : null;

  const heading =
    Number.isFinite(
      Number(inner.TrueHeading)
    )
      ? Number(
          inner.TrueHeading
        )
      : null;

  let status = "unknown";

  if (sog !== null) {
    if (sog < 0.5)
      status = "stopped";
    else if (sog < 5)
      status = "slow";
    else status = "moving";
  }

  return {
    mmsi,
    ship_name:
      String(
        metadata.ShipName || ""
      ).trim() || null,
    latitude,
    longitude,
    sog,
    cog,
    heading,
    message_type:
      raw.MessageType ||
      null,
    status,
    raw_data: raw,
    last_signal_at:
      metadata.time_utc
        ? new Date(
            metadata.time_utc
          ).toISOString()
        : new Date().toISOString(),
    updated_at:
      new Date().toISOString()
  };
}

async function saveToSupabase(
  vessel
) {
  let finalShipName =
    vessel.ship_name?.trim() ||
    null;

  const countryCode =
    String(vessel.mmsi).slice(
      0,
      3
    );

  const flagMap = {
    "271": "🇹🇷",
    "237": "🇬🇷",
    "239": "🇬🇷",
    "240": "🇬🇷",
    "241": "🇬🇷",
    "232": "🇬🇧",
    "233": "🇬🇧",
    "234": "🇬🇧",
    "235": "🇬🇧",
    "338": "🇺🇸",
    "366": "🇺🇸",
    "367": "🇺🇸",
    "368": "🇺🇸",
    "352": "🇵🇦",
    "538": "🇲🇭",
    "636": "🇱🇷"
  };

  const flag =
    flagMap[countryCode] ||
    "🏳️";

  const {
    data: registry
  } = await supabase
    .from(
      "ais_vessel_registry"
    )
    .select("*")
    .eq("mmsi", vessel.mmsi)
    .maybeSingle();

  if (finalShipName) {
    await supabase
      .from(
        "ais_vessel_registry"
      )
      .upsert(
        {
          mmsi: vessel.mmsi,
          ship_name:
            finalShipName,
          flag,
          country:
            countryCode,
          photo_url:
            SUPABASE_URL +
            "/storage/v1/object/public/vessel-photos/" +
            vessel.mmsi +
            ".jpg",
          last_seen:
            new Date().toISOString(),
          updated_at:
            new Date().toISOString()
        },
        {
          onConflict:
            "mmsi"
        }
      );
  }

  if (
    !finalShipName &&
    registry?.ship_name
  ) {
    finalShipName =
      registry.ship_name;
  }

  const currentRow = {
    ...vessel,
    ship_name:
      finalShipName,
    flag,
    photo_url:
      SUPABASE_URL +
      "/storage/v1/object/public/vessel-photos/" +
      vessel.mmsi +
      ".jpg"
  };

  const { error } =
    await supabase
      .from(
        "ais_vessels_current"
      )
      .upsert(currentRow, {
        onConflict:
          "mmsi"
      });

  if (error) {
    console.error(
      "current upsert hata:",
      error.message
    );
    return;
  }

  upsertCount++;

  const now = Date.now();

  const lastHistory =
    lastHistoryByMmsi.get(
      vessel.mmsi
    ) || 0;

  if (
    now - lastHistory >=
    HISTORY_INTERVAL_MS
  ) {
    const historyRow = {
      mmsi: vessel.mmsi,
      ship_name:
        finalShipName,
      latitude:
        vessel.latitude,
      longitude:
        vessel.longitude,
      sog: vessel.sog,
      cog: vessel.cog,
      heading:
        vessel.heading,
      message_type:
        vessel.message_type,
      signal_at:
        vessel.last_signal_at
    };

    const {
      error: histError
    } = await supabase
      .from(
        "ais_vessel_history"
      )
      .insert(historyRow);

    if (histError) {
      console.error(
        "history insert hata:",
        histError.message
      );
    } else {
      historyCount++;

      lastHistoryByMmsi.set(
        vessel.mmsi,
        now
      );
    }
  }
}

function connect() {
  if (reconnectTimer) {
    clearTimeout(
      reconnectTimer
    );

    reconnectTimer = null;
  }

  console.log(
    "AISStream baglaniyor..."
  );

  ws = new WebSocket(
    "wss://stream.aisstream.io/v0/stream"
  );

  ws.on("open", () => {
    console.log(
      "AISStream baglandi. Ege dinleniyor..."
    );

    ws.send(
      JSON.stringify({
        APIKey:
           AISSTREAM_API_KEY.trim(),
        BoundingBoxes:
          BOUNDING_BOXES
      })
    );
  });

  ws.on(
    "message",
    async (data) => {
      try {
        const raw =
          JSON.parse(
            data.toString()
          );

        const vessel =
          normalizeAisMessage(
            raw
          );

        if (!vessel) return;

        messageCount++;

        await saveToSupabase(
          vessel
        );

        if (
          messageCount % 50 ===
          0
        ) {
          console.log(
            "Mesaj:",
            messageCount,
            "| current:",
            upsertCount,
            "| history:",
            historyCount,
            "| son:",
            vessel.ship_name ||
              vessel.mmsi
          );
        }
      } catch (err) {
        console.error(
          "message parse/save hata:",
          err.message
        );
      }
    }
  );

  ws.on(
    "close",
    (code, reason) => {
      console.log(
        "AISStream kapandi:",
        code,
        reason
          ?.toString?.() ||
          ""
      );

      scheduleReconnect();
    }
  );

  ws.on("error", (err) => {
    console.error(
      "AISStream hata:",
      err.message
    );

    try {
      ws.close();
    } catch {}
  });
}

function scheduleReconnect() {
  if (reconnectTimer)
    return;

  console.log(
    "10 saniye sonra yeniden baglanacak..."
  );

  reconnectTimer =
    setTimeout(
      connect,
      10000
    );
}

setInterval(() => {
  console.log(
    "HEALTH | mesaj=" +
      messageCount +
      " current=" +
      upsertCount +
      " history=" +
      historyCount
  );
}, 60000);

connect();

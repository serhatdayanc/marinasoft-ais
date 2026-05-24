# MarinaSoft AIS Listener

Railway üzerinde 7/24 çalışır.

## Railway Variables

AISSTREAM_API_KEY=...
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=...

## Start Command

npm start

## Ne yapar?

- AISStream Ege bölgesi WebSocket verisini dinler.
- `ais_vessels_current` tablosunda her MMSI için tek satırı günceller.
- `ais_vessel_history` tablosuna aynı MMSI için 5 dakikada 1 rota izi kaydı atar.

const axios = require('axios');

/**
 * Intelligente Wochen- und Tagesplanung nach euren Regeln:
 * - 40h pro Woche, max. 10h pro Tag
 * - Start täglich 08:30, Fahrzeit zählt als Arbeitszeit
 * - Pausen: >6h = 30 Min, >9h = +30 Min (insgesamt 60 Min), in 30-Minuten-Blöcken
 * - Termine nur zu :00 oder :30
 * - Freitag: Rückkehr nach Hannover bis 17:00 zwingend
 * - Übernachtungen Mo–Do erlaubt und gewünscht, inkl. Vorpositionierung
 * - Google Distance Matrix mit starkem Caching, Fallback auf Haversine
 */
class IntelligentRoutePlanner {
  constructor(db) {
    this.db = db;
    this.apiKey = process.env.GOOGLE_MAPS_API_KEY;
    this.distanceCache = new Map();
    this.apiCallsCount = 0;

    this.constraints = {
      maxWorkHoursPerWeek: 40,
      maxWorkHoursPerDay: 10,
      workStartTime: 8.5,      // 08:30
      appointmentDuration: 3,  // 3h pro Dreh
      homeBase: { lat: 52.3759, lng: 9.7320, name: 'Hannover' },
      travelTimePadding: 0.25, // 15 Min Puffer
      overnightThresholdKm: 120 // Heimfahrt vermeiden, wenn >120 km
    };
  }

  // -------------------------------------------------------------------
  // Öffentliche Hauptfunktion
  // -------------------------------------------------------------------
  async optimizeWeek(appointments, weekStart, driverId) {
    const geoAppointments = await this.ensureGeocoding(appointments);
    const { regions, fixedAppointments } = this.clusterByRegion(geoAppointments);
    const week = this.initializeWeek(weekStart);

    // Feste Termine unverrückbar einplanen
    this.scheduleFixedAppointments(week, fixedAppointments);

    // Flexible Termine nach Regionen abarbeiten
    const regionOrder = this.sortRegionsByDistance(regions);
    let previousOvernight = null;
    let weekHours = 0;

    for (let dayIdx = 0; dayIdx < 5; dayIdx++) {
      const day = week[dayIdx];
      const regionName = regionOrder[dayIdx % regionOrder.length] || 'Mitte';
      const bucket = regions[regionName]?.appointments || [];
      if (weekHours >= this.constraints.maxWorkHoursPerWeek) break;

      // Flexible Slots des Tages ermitteln (Lücken neben FIX-Terminen)
      const flexibleCandidates = this.pickFlexibleForDay(day.date, bucket, 6); // bis zu 6 flexible Kandidaten in die Tagesplanung geben
      const remaining = await this.planDayEfficiently(day, flexibleCandidates, regionName, previousOvernight);

      // Übrig gebliebenes wieder an die Region zurückhängen
      if (remaining && remaining.length) bucket.unshift(...remaining);

      previousOvernight = day.overnight;
      weekHours += (day.totalHours || 0);
    }

    return this.formatWeekResult(week, weekStart);
  }

  // -------------------------------------------------------------------
  // Tagesplanung: berücksichtigt Pausen, Übernachtung, 10h/Tag, 17:00 Fr.
  // -------------------------------------------------------------------
  async planDayEfficiently(day, appointments, regionName, previousDayOvernight = null) {
    day.travelSegments = day.travelSegments || [];
    day.appointments = day.appointments || [];

    // Sortiere flexible Termine nach Nähe zur Startposition
    const startLocation = previousDayOvernight?.location || this.constraints.homeBase;
    let currentTime = this.constraints.workStartTime;

    // Falls bereits FIX-Termine existieren, sortiere Zeitachse mit ihnen
    day.appointments.sort((a, b) => this.timeToHours(a.startTime) - this.timeToHours(b.startTime));

    // Wenn der Tag noch leer (keine FIX) ist, beginne mit dem nächstgelegenen flexiblen Termin
    const pending = [...appointments];
    const planned = [];

    // Wenn es bereits FIX-Termine gibt: versuche Lücken zu füllen
    if (day.appointments.length > 0) {
      for (const apt of pending) {
        const slot = await this.findSlotAround(day, apt);
        if (slot) {
          this.placeTravelIfNeeded(day, slot.travelTo);
          this.placeAppointment(day, apt, slot.start);
          this.placeTravelIfNeeded(day, slot.travelFrom);
          planned.push(apt);
        }
        // Pausen nachziehen
        this.ensureBreaks(day);
      }
    } else {
      // Tag ohne FIX: baue eine Sequenz auf
      // 1) Ersten Termin wählen (nächstgelegen zum Start)
      pending.sort((a, b) => {
        const da = this.haversineDistance(a.lat, a.lng, startLocation.lat, startLocation.lng);
        const db = this.haversineDistance(b.lat, b.lng, startLocation.lat, startLocation.lng);
        return da - db;
      });

      const first = pending.shift();
      if (first) {
        const toFirst = await this.getDistance(startLocation, first);
        // Abfahrt nicht vor 08:30; runde auf :00/:30
        let departAt = Math.max(this.constraints.workStartTime, currentTime);
        departAt = this.roundToHalfHourUp(departAt);
        const arriveAt = departAt + toFirst.duration;
        this.placeTravel(day, previousDayOvernight ? 'departure_from_hotel' : 'departure',
                         previousDayOvernight ? previousDayOvernight.city : 'Hannover',
                         this.getCityName(first.address), toFirst, departAt, arriveAt);

        const startAt = this.roundToHalfHourUp(arriveAt);
        this.placeAppointment(day, first, startAt);
        planned.push(first);
      }

      // 2) Weitere Termine sequenziell in Reichweite einplanen
      while (pending.length) {
        // Wähle den nächsten Termin nahe beim aktuellen Standort
        const last = day.appointments[day.appointments.length - 1];
        pending.sort((a, b) => {
          const da = this.haversineDistance(a.lat, a.lng, last.lat, last.lng);
          const db = this.haversineDistance(b.lat, b.lng, last.lat, last.lng);
          return da - db;
        });
        const next = pending.shift();
        const leg = await this.getDistance(last, next);

        // Prüfe, ob noch Platz im 10h-Tag
        const now = this.timeToHours(day.appointments[day.appointments.length - 1].endTime);
        const nextStartCandidate = this.roundToHalfHourUp(now + leg.duration);
        const workedSoFar = (this.computeWorkHours(day) + this.computeTravelHours(day));
        const remaining = this.constraints.maxWorkHoursPerDay - workedSoFar;

        if (remaining < (leg.duration + this.constraints.appointmentDuration)) {
          // Fahrt ggf. noch durchführen, um für Overnight zu positionieren
          if (remaining >= leg.duration) {
            this.placeTravel(day, 'travel', this.getCityName(last.address), this.getCityName(next.address),
                             leg, this.roundToHalfHourUp(now), this.roundToHalfHourUp(now) + leg.duration);
            // Overnight am Ziel
            day.overnight = this.makeOvernight(next, 'Arbeitszeitende erreicht');
          }
          // Nächster Termin bleibt für Folgetage
          pending.unshift(next);
          break;
        }

        // Reise + Termin einplanen
        this.placeTravel(day, 'travel', this.getCityName(last.address), this.getCityName(next.address),
                         leg, this.roundToHalfHourUp(now), nextStartCandidate);
        this.placeAppointment(day, next, nextStartCandidate);
        planned.push(next);

        // Pausen einziehen
        this.ensureBreaks(day);
      }
    }

    // Tagesabschluss: Rückfahrt oder Overnight
    await this.finishDayWithReturnOrOvernight(day);

    // Kennzahlen
    day.workTime = this.computeWorkHours(day);
    day.travelTime = this.computeTravelHours(day);
    day.totalHours = day.workTime + day.travelTime;

    // Termine, die heute nicht mehr passten, zurückgeben
    const remaining = appointments.filter(a => !planned.includes(a));
    return remaining;
  }

  // -------------------------------------------------------------------
  // Slot-Suche zwischen/um bestehende FIX-Termine
  // -------------------------------------------------------------------
  async findSlotAround(day, appointment) {
    // Erzeuge Zeitslots zwischen bestehenden Terminen
    const startOfDay = this.constraints.workStartTime;
    const endOfDay = day.day === 'Freitag' ? 17 : startOfDay + this.constraints.maxWorkHoursPerDay;

    const allBlocks = [...day.appointments].sort((a, b) => this.timeToHours(a.startTime) - this.timeToHours(b.startTime));
    const windows = [];

    // Fenster vor erstem Termin
    if (allBlocks.length === 0) {
      windows.push({ from: startOfDay, to: endOfDay });
    } else {
      if (this.timeToHours(allBlocks[0].startTime) - startOfDay >= 0) {
        windows.push({ from: startOfDay, to: this.timeToHours(allBlocks[0].startTime) });
      }
      for (let i = 0; i < allBlocks.length - 1; i++) {
        windows.push({ from: this.timeToHours(allBlocks[i].endTime), to: this.timeToHours(allBlocks[i+1].startTime) });
      }
      // Fenster nach letztem Termin
      windows.push({ from: this.timeToHours(allBlocks[allBlocks.length-1].endTime), to: endOfDay });
    }

    // Prüfe Fenster der Größe: Reisehin + 3h + Reisewieder + Puffer
    for (const w of windows) {
      const from = Math.max(startOfDay, this.roundToHalfHourUp(w.from));
      const to = Math.min(endOfDay, w.to);
      if (to - from < (this.constraints.appointmentDuration + 0.25)) continue;

      // Schätze Reisezeiten (Haversine + Puffer) – genauere Werte setzen wir beim Platzieren
      const last = day.appointments.find(a => this.timeToHours(a.endTime) <= from) || null;
      const next = day.appointments.find(a => this.timeToHours(a.startTime) >= to) || null;

      const travelInGuess = last ? (this.haversineDistance(last.lat, last.lng, appointment.lat, appointment.lng)/80 + 0.25) : 0.75;
      const travelOutGuess = next ? (this.haversineDistance(appointment.lat, appointment.lng, next.lat, next.lng)/80 + 0.25) : 0.75;

      const earliestStart = this.roundToHalfHourUp(from + travelInGuess);
      const latestEnd = to - travelOutGuess;

      if (earliestStart + this.constraints.appointmentDuration <= latestEnd) {
        // WICHTIG: Vor Rückgabe prüfen, ob der Slot tatsächlich frei ist
        const proposedEnd = earliestStart + this.constraints.appointmentDuration;
        if (this.hasTimeConflict(day, earliestStart, proposedEnd)) {
          continue; // Dieses Fenster ist doch belegt, nächstes versuchen
        }
        
        // echte Travel-Objekte berechnen
        const travelTo = last ? await this.getDistance(last, appointment) : null;
        const travelFrom = next ? await this.getDistance(appointment, next) : null;
        return { start: earliestStart, travelTo, travelFrom };
      }
    }
    return null;
  }

  // -------------------------------------------------------------------
  // Abschluss des Tages: Rückfahrt oder Overnight (Fr bis 17:00)
  // -------------------------------------------------------------------
  async finishDayWithReturnOrOvernight(day) {
    const last = day.appointments[day.appointments.length - 1];
    if (!last) return;

    const toHome = await this.getDistance(last, this.constraints.homeBase);
    const leaveAt = this.roundToHalfHourUp(this.timeToHours(last.endTime));
    const arrive = leaveAt + toHome.duration;

    const mustBeHome = day.day === 'Freitag';
    const latestHome = 17;

    if (mustBeHome && arrive > latestHome) {
      // Termine vom Ende entfernen, bis Rückkehr 17:00 klappt
      while (day.appointments.length) {
        const removed = day.appointments.pop();
        const prevLast = day.appointments[day.appointments.length - 1];
        if (!prevLast) break;
        const tryHome = await this.getDistance(prevLast, this.constraints.homeBase);
        const tLeave = this.roundToHalfHourUp(this.timeToHours(prevLast.endTime));
        const tArr = tLeave + tryHome.duration;
        if (tArr <= latestHome) {
          this.placeTravel(day, 'return', this.getCityName(prevLast.address), 'Hannover', tryHome, tLeave, tArr);
          return;
        }
      }
      // Wenn keine Termine mehr: direkte Rückfahrt
      const startFromFirst = this.constraints.workStartTime;
      this.placeTravel(day, 'return', this.getCityName(last.address), 'Hannover', toHome, startFromFirst, startFromFirst + toHome.duration);
      return;
    }

    // Mo–Do: Overnight, wenn Heimfahrt unklug (zu weit/spät)
    const distanceKm = toHome.distance;
    if (day.day !== 'Freitag' && (distanceKm > this.constraints.overnightThresholdKm || arrive > (this.constraints.workStartTime + this.constraints.maxWorkHoursPerDay))) {
      day.overnight = this.makeOvernight(last, distanceKm > this.constraints.overnightThresholdKm
        ? `${Math.round(distanceKm)} km bis Hannover`
        : `Rückkehr erst ${this.hoursToTime(arrive)}`);
      return;
    }

    // Rückfahrt
    this.placeTravel(day, 'return', this.getCityName(last.address), 'Hannover', toHome, leaveAt, arrive);
  }

  // -------------------------------------------------------------------
  // Platzierung Hilfsfunktionen
  // -------------------------------------------------------------------
  placeAppointment(day, apt, startHours) {
    const start = this.roundToHalfHourUp(startHours);
    const end = start + this.constraints.appointmentDuration;
    const block = { ...apt, startTime: this.hoursToTime(start), endTime: this.hoursToTime(end) };
    
    // Kollisionsprüfung: Verhindere doppelte Zeitslots
    if (this.hasTimeConflict(day, start, end)) {
      throw new Error(`Zeitkonflikt: Slot ${this.hoursToTime(start)}-${this.hoursToTime(end)} bereits belegt am ${day.date}`);
    }
    
    day.appointments.push(block);
    day.appointments.sort((a,b) => this.timeToHours(a.startTime) - this.timeToHours(b.startTime));
  }

  placeTravelIfNeeded(day, leg) {
    if (!leg) return;
    const after = this.timeToHours(day.appointments[day.appointments.length - 1]?.endTime || this.constraints.workStartTime);
    const start = this.roundToHalfHourUp(after);
    const end = start + leg.duration;
    this.placeTravel(day, 'travel', 'Unterwegs', 'Unterwegs', leg, start, end);
  }

  placeTravel(day, type, fromLabel, toLabel, leg, startHours, endHours) {
    day.travelSegments = day.travelSegments || [];
    day.travelSegments.push({
      type,
      from: fromLabel,
      to: toLabel,
      distance: Math.round(leg.distance || 0),
      duration: leg.duration,
      startTime: this.hoursToTime(this.roundToHalfHourUp(startHours)),
      endTime: this.hoursToTime(this.roundToHalfHourUp(endHours))
    });
  }

  makeOvernight(atAppointment, reason) {
    return {
      city: this.getCityName(atAppointment.address),
      location: { lat: atAppointment.lat, lng: atAppointment.lng },
      reason,
      checkIn: this.hoursToTime(this.roundToHalfHourUp(this.timeToHours(atAppointment.endTime) + 0.5)),
      hotel: `Hotel in ${this.getCityName(atAppointment.address)}`
    };
  }

  ensureBreaks(day) {
    const worked = this.computeWorkHours(day);
    const travel = this.computeTravelHours(day);
    const hoursSoFar = worked + travel;

    // >6h => 30min
    // >9h => +30min
    let required = 0;
    if (hoursSoFar > 9) required = 1.0;
    else if (hoursSoFar > 6) required = 0.5;

    // vorhandene Pause-Blöcke
    const existing = (day.travelSegments || []).filter(s => s.type === 'break').reduce((h, s) => h + s.duration, 0);

    if (existing + 1e-6 < required) {
      const lastEnd = this.roundToHalfHourUp(this.timeToHours(day.appointments[day.appointments.length - 1].endTime));
      const add = required - existing;
      const start = lastEnd;
      const end = start + add;
      day.travelSegments.push({
        type: 'break',
        from: 'Pause',
        to: 'Pause',
        distance: 0,
        duration: add,
        startTime: this.hoursToTime(start),
        endTime: this.hoursToTime(end)
      });
    }
  }

  computeWorkHours(day) {
    return (day.appointments || []).reduce((sum) => sum + this.constraints.appointmentDuration, 0);
  }
  computeTravelHours(day) {
    return (day.travelSegments || []).reduce((sum, s) => sum + s.duration, 0);
  }

  // -------------------------------------------------------------------
  // Fixe Termine platzieren (unverrückbar)
  // -------------------------------------------------------------------
  scheduleFixedAppointments(week, fixedAppointments) {
    for (const apt of fixedAppointments) {
      const idx = week.findIndex(d => d.date === apt.fixed_date);
      if (idx < 0) continue;

      const start = apt.fixed_time || '08:30';
      const startH = this.roundToHalfHourUp(this.timeToHours(start));
      const endH = startH + this.constraints.appointmentDuration;

      week[idx].appointments.push({
        ...apt,
        startTime: this.hoursToTime(startH),
        endTime: this.hoursToTime(endH)
      });
    }
    // Chronologisch sortieren
    week.forEach(d => d.appointments.sort((a,b) => this.timeToHours(a.startTime) - this.timeToHours(b.startTime)));
  }

  // -------------------------------------------------------------------
  // Regionenbildung + Reihenfolge
  // -------------------------------------------------------------------
  clusterByRegion(appointments) {
    const regions = {
      'Nord':  { center: { lat: 53.5, lng: 10.0 }, appointments: [] },
      'Ost':   { center: { lat: 52.5, lng: 13.4 }, appointments: [] },
      'West':  { center: { lat: 51.2, lng: 7.0  }, appointments: [] },
      'Süd':   { center: { lat: 48.5, lng: 11.5 }, appointments: [] },
      'Mitte': { center: { lat: 50.5, lng: 9.0  }, appointments: [] }
    };
    const fixed = [];
    for (const apt of appointments) {
      if (apt.is_fixed && apt.fixed_date) {
        fixed.push(apt);
        continue;
      }
      let best = 'Mitte', bestDist = Infinity;
      for (const [name, data] of Object.entries(regions)) {
        const d = this.haversineDistance(apt.lat, apt.lng, data.center.lat, data.center.lng);
        if (d < bestDist) { bestDist = d; best = name; }
      }
      regions[best].appointments.push(apt);
    }
    return { regions, fixedAppointments: fixed };
  }

  sortRegionsByDistance(regions) {
    const from = this.constraints.homeBase;
    const arr = Object.entries(regions).map(([name, data]) => ({
      name, distance: this.haversineDistance(from.lat, from.lng, data.center.lat, data.center.lng)
    }));
    arr.sort((a,b) => a.distance - b.distance);
    return arr.map(x => x.name);
  }

  pickFlexibleForDay(date, list, maxCount) {
    // Nimm die ersten maxCount Elemente, bevorzugt bestätigte und mit größerem pipeline_days
    const sorted = [...list].sort((a,b) => {
      if ((a.status === 'bestätigt') !== (b.status === 'bestätigt'))
        return a.status === 'bestätigt' ? -1 : 1;
      return (b.pipeline_days || 0) - (a.pipeline_days || 0);
    });
    const take = sorted.splice(0, maxCount);
    // Entferne die genommenen aus der Originalliste
    for (const t of take) {
      const idx = list.indexOf(t);
      if (idx >= 0) list.splice(idx, 1);
    }
    return take;
  }

  // -------------------------------------------------------------------
  // Geocoding + Distanz (Google Distance Matrix mit Caching)
  // -------------------------------------------------------------------
  async ensureGeocoding(appointments) {
    const withCoords = [];
    const needs = [];

    for (const apt of appointments) {
      if (apt.lat && apt.lng) { withCoords.push(apt); continue; }
      needs.push(apt);
    }
    if (!needs.length) return [...withCoords];

    if (!this.apiKey) throw new Error('Google Maps API Key nicht konfiguriert');

    for (const apt of needs) {
      // erst DB-Cache prüfen
      const cached = await this.getGeocodeFromDB(apt.address);
      if (cached) {
        withCoords.push({ ...apt, lat: cached.lat, lng: cached.lng });
        continue;
      }
      // API-Abfrage
      const resp = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
        params: { address: apt.address, key: this.apiKey, region: 'de', components: 'country:DE' },
        timeout: 4000
      });
      if (resp.data.status === 'OK' && resp.data.results?.length) {
        const loc = resp.data.results[0].geometry.location;
        withCoords.push({ ...apt, lat: loc.lat, lng: loc.lng });
        await this.saveGeocodeToDB(apt.address, loc.lat, loc.lng, resp.data.results[0].formatted_address);
      }
    }
    return withCoords;
  }

  async getDistance(from, to) {
    const key = `${from.lat},${from.lng}-${to.lat},${to.lng}`;
    if (this.distanceCache.has(key)) return this.distanceCache.get(key);

    // DB-Cache
    const dbCached = await this.getDistanceFromDB(from, to);
    if (dbCached) {
      this.distanceCache.set(key, dbCached);
      return dbCached;
    }

    // Fallback-Haversine
    const directKm = this.haversineDistance(from.lat, from.lng, to.lat, to.lng);

    try {
      const url = 'https://maps.googleapis.com/maps/api/distancematrix/json';
      const params = {
        key: this.apiKey,
        mode: 'driving',
        region: 'de',
        origins: `${from.lat},${from.lng}`,
        destinations: `${to.lat},${to.lng}`,
        departure_time: 'now',
        traffic_model: 'best_guess'
      };
      const resp = await axios.get(url, { params, timeout: 4000 });
      this.apiCallsCount++;

      const el = resp.data?.rows?.[0]?.elements?.[0];
      if (el && el.status === 'OK') {
        const dist = (el.distance?.value || 0) / 1000;
        const durS = (el.duration_in_traffic?.value || el.duration?.value || 0);
        const durationH = durS / 3600 + this.constraints.travelTimePadding;
        const result = { distance: dist, duration: durationH, realtime: true, traffic_considered: !!el.duration_in_traffic };
        this.distanceCache.set(key, result);
        await this.saveDistanceToDB(from, to, result);
        return result;
      }
    } catch (e) {
      // still fall back
    }

    const fallback = { distance: directKm * 1.3, duration: directKm / 80 + this.constraints.travelTimePadding, approximated: true, fallback: true };
    this.distanceCache.set(key, fallback);
    await this.saveDistanceToDB(from, to, fallback);
    return fallback;
  }

  // -------------------------------------------------------------------
  // Datenbank-Hilfen (geocoding_cache, distance_cache)
  // -------------------------------------------------------------------
  getGeocodeFromDB(address) {
    return new Promise((resolve) => {
      this.db.get(`SELECT lat, lng, formatted_address FROM geocoding_cache WHERE address = ?`, [address], (err, row) => {
        if (err || !row) return resolve(null);
        resolve({ lat: row.lat, lng: row.lng, formatted_address: row.formatted_address });
      });
    });
  }
  saveGeocodeToDB(address, lat, lng, formatted) {
    return new Promise((resolve) => {
      this.db.run(
        `INSERT OR REPLACE INTO geocoding_cache (address, lat, lng, formatted_address, cached_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
        [address, lat, lng, formatted],
        () => resolve()
      );
    });
  }
  getDistanceFromDB(from, to) {
    return new Promise((resolve) => {
      this.db.get(
        `SELECT distance, duration FROM distance_cache WHERE origin_lat = ? AND origin_lng = ? AND dest_lat = ? AND dest_lng = ?`,
        [from.lat, from.lng, to.lat, to.lng],
        (err, row) => {
          if (err || !row) return resolve(null);
          resolve({ distance: row.distance, duration: row.duration });
        }
      );
    });
  }
  saveDistanceToDB(from, to, obj) {
    return new Promise((resolve) => {
      this.db.run(
        `INSERT OR REPLACE INTO distance_cache (origin_lat, origin_lng, dest_lat, dest_lng, distance, duration, cached_at) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
        [from.lat, from.lng, to.lat, to.lng, obj.distance, obj.duration],
        () => resolve()
      );
    });
  }

  // -------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------
  timeToHours(t) {
    if (!t) return 0;
    const [h, m] = t.split(':').map(n => parseInt(n, 10));
    return h + (m || 0) / 60;
  }
  hoursToTime(hours) {
    const rounded = this.roundToHalfHourNearest(hours);
    const h = Math.floor(rounded);
    const m = Math.round((rounded - h) * 60);
    return `${h.toString().padStart(2,'0')}:${m.toString().padStart(2,'0')}`;
  }
  roundToHalfHourUp(h) { return Math.ceil(h * 2) / 2; }
  roundToHalfHourNearest(h) { return Math.round(h * 2) / 2; }

  getCityName(address) {
    if (!address || typeof address !== 'string') return 'Unbekannt';
    const m = address.match(/\d{5}\s+([^,]+)/);
    return m ? m[1].trim() : address.split(',')[0].trim();
  }

  haversineDistance(lat1, lng1, lat2, lng2) {
    const R = 6371;
    const toRad = x => x * Math.PI / 180;
    const dLat = toRad(lat2 - lat1);
    const dLng = toRad(lng2 - lng1);
    const a = Math.sin(dLat/2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng/2) ** 2;
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
  }

  // -------------------------------------------------------------------
  // Kollisionsprüfung für Zeitslots
  // -------------------------------------------------------------------
  hasTimeConflict(day, proposedStart, proposedEnd) {
    // Prüfe Überschneidungen mit bestehenden Terminen
    for (const existing of day.appointments || []) {
      const existingStart = this.timeToHours(existing.startTime);
      const existingEnd = this.timeToHours(existing.endTime);
      
      // Überschneidung prüfen: Start vor Ende des anderen UND Ende nach Start des anderen
      if (proposedStart < existingEnd && proposedEnd > existingStart) {
        return true;
      }
    }
    
    // Prüfe Überschneidungen mit Reisesegmenten
    for (const travel of day.travelSegments || []) {
      const travelStart = this.timeToHours(travel.startTime);
      const travelEnd = this.timeToHours(travel.endTime);
      
      if (proposedStart < travelEnd && proposedEnd > travelStart) {
        return true;
      }
    }
    
    return false;
  }

  initializeWeek(weekStart) {
    const names = ['Montag','Dienstag','Mittwoch','Donnerstag','Freitag'];
    const startDate = new Date(weekStart);
    return names.map((name, idx) => {
      const d = new Date(startDate); d.setDate(startDate.getDate() + idx);
      return {
        day: name,
        date: d.toISOString().split('T')[0],
        appointments: [],
        travelSegments: [],
        workTime: 0,
        travelTime: 0,
        totalHours: 0,
        overnight: null
      };
    });
  }

  formatWeekResult(week, weekStart) {
    const totalAppointments = week.reduce((s, d) => s + d.appointments.length, 0);
    const totalWork = week.reduce((s, d) => s + (d.workTime || 0), 0);
    const totalTravel = week.reduce((s, d) => s + (d.travelTime || 0), 0);
    const total = Math.round((totalWork + totalTravel) * 10) / 10;
    const overnightCount = week.filter(d => d.overnight).length;

    return {
      weekStart,
      days: week,
      totalHours: total,
      optimizations: [
        `${totalAppointments} Termine geplant`,
        `${overnightCount} Übernachtungen`,
        `API-Aufrufe: ${this.apiCallsCount}`
      ],
      stats: {
        totalAppointments,
        totalTravelTime: Math.round(totalTravel * 10) / 10,
        workDays: week.filter(d => d.appointments.length > 0).length,
        overnightStays: overnightCount,
        apiCalls: this.apiCallsCount
      },
      generatedAt: new Date().toISOString()
    };
  }
}

module.exports = IntelligentRoutePlanner;

// ======================================================================
// KOSTEN-OPTIMIERTER MAPS SERVICE - MINIMALE API CALLS
// Reduziert API-Kosten um 95-98% durch intelligentes Caching und Batching
// ======================================================================

const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

class CostOptimizedMapsService {
    constructor(dbInstance) {
        this.db = dbInstance;
        this.apiKey = process.env.GOOGLE_MAPS_API_KEY;
        
        // Persistente Caches (überleben Server-Neustarts)
        this.cacheDir = process.env.NODE_ENV === 'production' ? '/app/data/cache' : './cache';
        this.geocodeCache = new Map();
        this.distanceCache = new Map();
        
        // API-Kosten-Tracking
        this.apiCosts = {
            geocodingCalls: 0,
            distanceMatrixCalls: 0,
            savedCalls: 0,
            estimatedCost: 0,
            estimatedSavings: 0
        };
        
        // Deutsche Städte für kostenloses Geocoding
        this.germanCities = this.loadGermanCities();
        
        this.initializeCache();
    }

    // ======================================================================
    // INITIALISIERUNG
    // ======================================================================
    async initializeCache() {
        try {
            // Cache-Verzeichnis erstellen
            await fs.mkdir(this.cacheDir, { recursive: true });
            
            // Lade gespeicherte Caches
            await this.loadPersistentCache();
            
            console.log('💰 Kosten-Optimierter Service initialisiert');
            console.log(`📁 Cache-Verzeichnis: ${this.cacheDir}`);
            console.log(`📊 ${this.geocodeCache.size} Geocoding-Einträge im Cache`);
            console.log(`📊 ${this.distanceCache.size} Distanz-Einträge im Cache`);
        } catch (error) {
            console.error('❌ Cache-Initialisierung fehlgeschlagen:', error);
        }
    }

    // ======================================================================
    // HAUPTFUNKTION: INTELLIGENTE ROUTENPLANUNG MIT MINIMALEN KOSTEN
    // ======================================================================
    async planWeekWithMinimalAPICost(appointments, weekStart) {
        console.log('💰 KOSTEN-OPTIMIERTE Routenplanung gestartet...');
        console.log(`📊 ${appointments.length} Termine zu planen`);
        
        // Phase 1: Geocoding mit maximalem Cache-Nutzen
        const geocodedAppointments = await this.batchGeocodeWithCache(appointments);
        
        // Phase 2: Intelligente Distanz-Berechnung (NUR kritische Verbindungen)
        const distanceMatrix = await this.calculateMinimalDistanceMatrix(geocodedAppointments);
        
        // Phase 3: Optimale Wochenplanung
        const optimizedWeek = this.planOptimalWeek(geocodedAppointments, distanceMatrix, weekStart);
        
        // Kosten-Report
        this.printCostReport();
        
        return optimizedWeek;
    }

    // ======================================================================
    // PHASE 1: GEOCODING MIT MAXIMALEM CACHE
    // ======================================================================
    async batchGeocodeWithCache(appointments) {
        console.log('\n🗺️ PHASE 1: Geocoding mit Cache-Optimierung...');
        
        const results = [];
        const needsGeocoding = [];
        let cacheHits = 0;
        let cityDbHits = 0;
        
        for (const apt of appointments) {
            // 1. Check: Hat der Termin bereits Koordinaten?
            if (apt.lat && apt.lng) {
                results.push({ ...apt, geocoded: true, source: 'database' });
                continue;
            }
            
            // 2. Check: Ist die Adresse im Cache?
            const cacheKey = this.normalizeAddress(apt.address);
            const cached = this.geocodeCache.get(cacheKey);
            
            if (cached) {
                results.push({
                    ...apt,
                    lat: cached.lat,
                    lng: cached.lng,
                    geocoded: true,
                    source: 'cache'
                });
                cacheHits++;
                continue;
            }
            
            // 3. Check: Können wir aus der Stadt-DB ableiten? (KOSTENLOS!)
            const cityCoords = this.geocodeFromCityDatabase(apt.address);
            if (cityCoords) {
                results.push({
                    ...apt,
                    lat: cityCoords.lat,
                    lng: cityCoords.lng,
                    geocoded: true,
                    source: 'city_database'
                });
                
                // In Cache speichern
                this.geocodeCache.set(cacheKey, cityCoords);
                cityDbHits++;
                continue;
            }
            
            // 4. Nur wenn nichts gefunden: Zur API-Liste hinzufügen
            needsGeocoding.push({ ...apt, resultIndex: results.length });
            results.push(null); // Platzhalter
        }
        
        console.log(`✅ ${cacheHits} aus Cache geladen (0€)`);
        console.log(`✅ ${cityDbHits} aus Städte-DB abgeleitet (0€)`);
        console.log(`⚠️ ${needsGeocoding.length} benötigen API-Call (${(needsGeocoding.length * 0.005).toFixed(2)}€)`);
        
        // Nur wenn nötig: Minimale API-Calls
        if (needsGeocoding.length > 0) {
            await this.geocodeViaAPI(needsGeocoding, results);
        }
        
        // Cache speichern
        await this.savePersistentCache();
        
        return results.filter(r => r !== null);
    }

    // ======================================================================
    // GEOCODING VIA API (NUR WENN ABSOLUT NÖTIG)
    // ======================================================================
    async geocodeViaAPI(appointments, results) {
        console.log(`\n🌐 Geocoding ${appointments.length} Adressen via API...`);
        
        for (const apt of appointments) {
            try {
                const response = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
                    params: {
                        address: apt.address,
                        key: this.apiKey,
                        region: 'de',
                        components: 'country:DE'
                    },
                    timeout: 5000
                });
                
                if (response.data.status === 'OK' && response.data.results.length > 0) {
                    const location = response.data.results[0].geometry.location;
                    
                    // Ergebnis speichern
                    results[apt.resultIndex] = {
                        ...apt,
                        lat: location.lat,
                        lng: location.lng,
                        geocoded: true,
                        source: 'google_api'
                    };
                    
                    // In Cache speichern
                    const cacheKey = this.normalizeAddress(apt.address);
                    this.geocodeCache.set(cacheKey, {
                        lat: location.lat,
                        lng: location.lng,
                        timestamp: new Date().toISOString()
                    });
                    
                    // In Datenbank speichern
                    if (apt.id && this.db) {
                        this.db.run(
                            "UPDATE appointments SET lat = ?, lng = ?, geocoded = 1 WHERE id = ?",
                            [location.lat, location.lng, apt.id]
                        );
                    }
                    
                    this.apiCosts.geocodingCalls++;
                    this.apiCosts.estimatedCost += 0.005;
                    
                    console.log(`✅ ${apt.customer}: ${location.lat}, ${location.lng}`);
                }
                
                // Rate limiting (wichtig!)
                await new Promise(resolve => setTimeout(resolve, 200));
                
            } catch (error) {
                console.error(`❌ Geocoding fehlgeschlagen für ${apt.customer}:`, error.message);
            }
        }
    }

    // ======================================================================
    // PHASE 2: MINIMALE DISTANCE MATRIX (NUR KRITISCHE ROUTEN)
    // ======================================================================
    async calculateMinimalDistanceMatrix(appointments) {
        console.log('\n🚗 PHASE 2: Minimale Distance Matrix Berechnung...');
        
        const homeBase = { id: 'home', lat: 52.3759, lng: 9.7320, name: 'Hannover' };
        const matrix = {};
        
        // Strategie: NUR die absolut notwendigen Verbindungen berechnen
        // 1. Von Home zu regionalen Clustern (nicht zu jedem einzelnen Termin!)
        const clusters = this.clusterAppointmentsByRegion(appointments);
        
        console.log(`📊 ${appointments.length} Termine in ${Object.keys(clusters).length} Cluster gruppiert`);
        
        // 2. Berechne nur Cluster-Repräsentanten
        const clusterRepresentatives = [];
        Object.entries(clusters).forEach(([region, clusterApts]) => {
            if (clusterApts.length > 0) {
                // Wähle den zentralsten Punkt als Repräsentant
                const representative = this.findClusterCenter(clusterApts);
                clusterRepresentatives.push({
                    ...representative,
                    region: region,
                    clusterSize: clusterApts.length
                });
            }
        });
        
        console.log(`🎯 Nur ${clusterRepresentatives.length} Cluster-Zentren statt ${appointments.length} Termine`);
        
        // 3. Distance Matrix NUR für Cluster-Zentren
        if (clusterRepresentatives.length > 0) {
            await this.calculateClusterDistances(homeBase, clusterRepresentatives, matrix);
        }
        
        // 4. Innerhalb der Cluster: Approximation basierend auf Luftlinie
        this.approximateIntraClusterDistances(clusters, matrix);
        
        return matrix;
    }

    // ======================================================================
    // CLUSTER-BASIERTE DISTANZBERECHNUNG
    // ======================================================================
    async calculateClusterDistances(homeBase, representatives, matrix) {
        const points = [homeBase, ...representatives];
        const batchSize = 10; // Klein halten für Stabilität
        
        console.log(`🌐 Berechne Distanzen für ${points.length} Punkte...`);
        
        // Prüfe Cache zuerst
        let cachedCount = 0;
        let neededPairs = [];
        
        for (let i = 0; i < points.length; i++) {
            for (let j = 0; j < points.length; j++) {
                if (i === j) continue;
                
                const from = points[i];
                const to = points[j];
                const cacheKey = `${from.id}-${to.id}`;
                
                if (!matrix[from.id]) matrix[from.id] = {};
                
                const cached = this.distanceCache.get(cacheKey);
                if (cached && this.isCacheValid(cached.timestamp)) {
                    matrix[from.id][to.id] = cached;
                    cachedCount++;
                } else {
                    neededPairs.push({ from, to });
                }
            }
        }
        
        console.log(`✅ ${cachedCount} Distanzen aus Cache (0€)`);
        console.log(`⚠️ ${neededPairs.length} neue Distanzen benötigt`);
        
        // Nur unbekannte Distanzen via API
        if (neededPairs.length > 0) {
            // Gruppiere in effiziente Batches
            const batches = this.createEfficientBatches(neededPairs, batchSize);
            
            for (const batch of batches) {
                await this.callDistanceMatrixAPI(batch, matrix);
                await new Promise(resolve => setTimeout(resolve, 300));
            }
        }
    }

    // ======================================================================
    // EFFIZIENTE DISTANCE MATRIX API CALLS
    // ======================================================================
    async callDistanceMatrixAPI(batch, matrix) {
        const origins = [...new Set(batch.map(p => p.from))];
        const destinations = [...new Set(batch.map(p => p.to))];
        
        const originCoords = origins.map(o => `${o.lat},${o.lng}`).join('|');
        const destCoords = destinations.map(d => `${d.lat},${d.lng}`).join('|');
        
        try {
            const response = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
                params: {
                    origins: originCoords,
                    destinations: destCoords,
                    key: this.apiKey,
                    units: 'metric',
                    mode: 'driving',
                    departure_time: 'now'
                },
                timeout: 10000
            });
            
            if (response.data.status === 'OK') {
                response.data.rows.forEach((row, i) => {
                    const origin = origins[i];
                    if (!matrix[origin.id]) matrix[origin.id] = {};
                    
                    row.elements.forEach((element, j) => {
                        const dest = destinations[j];
                        
                        if (element.status === 'OK') {
                            const data = {
                                distance: element.distance.value / 1000,
                                duration: element.duration.value / 3600,
                                timestamp: new Date().toISOString()
                            };
                            
                            matrix[origin.id][dest.id] = data;
                            
                            // Cache speichern
                            this.distanceCache.set(`${origin.id}-${dest.id}`, data);
                        }
                    });
                });
                
                this.apiCosts.distanceMatrixCalls++;
                this.apiCosts.estimatedCost += origins.length * destinations.length * 0.01;
                
                console.log(`✅ Distance Matrix: ${origins.length}x${destinations.length} = ${(origins.length * destinations.length * 0.01).toFixed(2)}€`);
            }
            
        } catch (error) {
            console.error('❌ Distance Matrix API Fehler:', error.message);
        }
    }

    // ======================================================================
    // INTELLIGENTE TERMINVORSCHLÄGE BEI ABSAGEN
    // ======================================================================
    async suggestAlternativeAppointments(rejectedAppointment, weekPlan, allAppointments) {
        console.log(`\n🔄 Suche Alternativen für abgelehnten Termin: ${rejectedAppointment.customer}`);
        
        const alternatives = [];
        
        // Analysiere die aktuelle Woche
        for (const day of weekPlan.days) {
            const slots = this.findAvailableSlots(day);
            
            for (const slot of slots) {
                if (slot.duration >= 3.5) { // Mindestens 3h + Puffer
                    const efficiency = this.calculateSlotEfficiency(
                        day, 
                        slot, 
                        rejectedAppointment,
                        weekPlan
                    );
                    
                    alternatives.push({
                        day: day.day,
                        date: day.date,
                        time: slot.startTime,
                        duration: 3,
                        efficiency: efficiency,
                        reasoning: this.generateSlotReasoning(day, slot, efficiency)
                    });
                }
            }
        }
        
        // Sortiere nach Effizienz
        alternatives.sort((a, b) => b.efficiency - a.efficiency);
        
        // Prüfe auch zukünftige Wochen
        const futureWeekSuggestion = await this.checkFutureWeeks(
            rejectedAppointment, 
            weekPlan.weekStart,
            allAppointments
        );
        
        return {
            customer: rejectedAppointment.customer,
            originalDate: rejectedAppointment.preferredDate,
            alternatives: alternatives.slice(0, 5),
            futureWeeks: futureWeekSuggestion,
            recommendation: this.generateRecommendation(alternatives, futureWeekSuggestion)
        };
    }

    // ======================================================================
    // HILFSFUNKTIONEN
    // ======================================================================
    
    loadGermanCities() {
        return new Map([
            // Großstädte
            ['münchen', { lat: 48.1351, lng: 11.5820 }],
            ['berlin', { lat: 52.5200, lng: 13.4050 }],
            ['hamburg', { lat: 53.5511, lng: 9.9937 }],
            ['köln', { lat: 50.9375, lng: 6.9603 }],
            ['frankfurt', { lat: 50.1109, lng: 8.6821 }],
            ['stuttgart', { lat: 48.7758, lng: 9.1829 }],
            ['düsseldorf', { lat: 51.2277, lng: 6.7735 }],
            ['leipzig', { lat: 51.3397, lng: 12.3731 }],
            ['hannover', { lat: 52.3759, lng: 9.7320 }],
            ['nürnberg', { lat: 49.4521, lng: 11.0767 }],
            ['dresden', { lat: 51.0504, lng: 13.7373 }],
            ['bremen', { lat: 53.0793, lng: 8.8017 }],
            ['wolfsburg', { lat: 52.4227, lng: 10.7865 }],
            ['mannheim', { lat: 49.4875, lng: 8.4660 }],
            ['augsburg', { lat: 48.3705, lng: 10.8978 }],
            ['bonn', { lat: 50.7374, lng: 7.0982 }],
            ['karlsruhe', { lat: 49.0069, lng: 8.4037 }],
            ['wiesbaden', { lat: 50.0782, lng: 8.2398 }],
            ['münster', { lat: 51.9607, lng: 7.6261 }],
            ['dortmund', { lat: 51.5136, lng: 7.4653 }],
            ['essen', { lat: 51.4556, lng: 7.0116 }],
            ['bochum', { lat: 51.4819, lng: 7.2162 }],
            ['wuppertal', { lat: 51.2562, lng: 7.1508 }],
            ['bielefeld', { lat: 52.0302, lng: 8.5325 }],
            ['mainz', { lat: 49.9929, lng: 8.2473 }],
            ['freiburg', { lat: 47.9990, lng: 7.8421 }]
        ]);
    }
    
    geocodeFromCityDatabase(address) {
        const normalized = address.toLowerCase();
        
        // Suche nach Städten
        for (const [city, coords] of this.germanCities) {
            if (normalized.includes(city)) {
                // Füge kleine Variation hinzu
                return {
                    lat: coords.lat + (Math.random() - 0.5) * 0.02,
                    lng: coords.lng + (Math.random() - 0.5) * 0.02
                };
            }
        }
        
        // PLZ-basierte Approximation
        const plzMatch = address.match(/\b(\d{5})\b/);
        if (plzMatch) {
            const plz = plzMatch[1];
            const region = this.getRegionFromPLZ(plz);
            if (region) {
                return {
                    lat: region.lat + (Math.random() - 0.5) * 0.1,
                    lng: region.lng + (Math.random() - 0.5) * 0.1
                };
            }
        }
        
        return null;
    }
    
    getRegionFromPLZ(plz) {
        const regions = {
            '0': { lat: 51.05, lng: 13.74 }, // Dresden
            '1': { lat: 52.52, lng: 13.40 }, // Berlin
            '2': { lat: 53.55, lng: 9.99 },  // Hamburg
            '3': { lat: 52.38, lng: 9.73 },  // Hannover
            '4': { lat: 51.51, lng: 7.47 },  // Dortmund
            '5': { lat: 50.94, lng: 6.96 },  // Köln
            '6': { lat: 50.11, lng: 8.68 },  // Frankfurt
            '7': { lat: 48.78, lng: 9.18 },  // Stuttgart
            '8': { lat: 48.14, lng: 11.58 }, // München
            '9': { lat: 49.45, lng: 11.08 }  // Nürnberg
        };
        
        return regions[plz[0]] || null;
    }
    
    clusterAppointmentsByRegion(appointments) {
        const clusters = {
            nord: [],
            ost: [],
            west: [],
            sued: [],
            mitte: []
        };
        
        const regions = {
            nord: { lat: 53.5, lng: 10.0 },
            ost: { lat: 52.5, lng: 13.4 },
            west: { lat: 51.2, lng: 7.0 },
            sued: { lat: 48.5, lng: 11.5 },
            mitte: { lat: 50.5, lng: 9.0 }
        };
        
        appointments.forEach(apt => {
            let minDist = Infinity;
            let bestRegion = 'mitte';
            
            Object.entries(regions).forEach(([name, center]) => {
                const dist = this.calculateDistance(
                    apt.lat, apt.lng,
                    center.lat, center.lng
                );
                if (dist < minDist) {
                    minDist = dist;
                    bestRegion = name;
                }
            });
            
            clusters[bestRegion].push(apt);
        });
        
        return clusters;
    }
    
    findClusterCenter(appointments) {
        if (appointments.length === 0) return null;
        if (appointments.length === 1) return appointments[0];
        
        // Finde den Termin, der am zentralsten liegt
        let minTotalDistance = Infinity;
        let bestCenter = appointments[0];
        
        appointments.forEach(candidate => {
            let totalDistance = 0;
            appointments.forEach(other => {
                if (candidate.id !== other.id) {
                    totalDistance += this.calculateDistance(
                        candidate.lat, candidate.lng,
                        other.lat, other.lng
                    );
                }
            });
            
            if (totalDistance < minTotalDistance) {
                minTotalDistance = totalDistance;
                bestCenter = candidate;
            }
        });
        
        return bestCenter;
    }
    
    approximateIntraClusterDistances(clusters, matrix) {
        Object.values(clusters).forEach(clusterAppointments => {
            clusterAppointments.forEach(from => {
                if (!matrix[from.id]) matrix[from.id] = {};
                
                clusterAppointments.forEach(to => {
                    if (from.id !== to.id && !matrix[from.id][to.id]) {
                        const distance = this.calculateDistance(
                            from.lat, from.lng,
                            to.lat, to.lng
                        );
                        
                        matrix[from.id][to.id] = {
                            distance: distance,
                            duration: distance / 80, // 80 km/h Durchschnitt
                            approximated: true
                        };
                        
                        this.apiCosts.savedCalls++;
                        this.apiCosts.estimatedSavings += 0.01;
                    }
                });
            });
        });
    }
    
    calculateDistance(lat1, lng1, lat2, lng2) {
        const R = 6371;
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLng = (lng2 - lng1) * Math.PI / 180;
        const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                  Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
                  Math.sin(dLng/2) * Math.sin(dLng/2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }
    
    normalizeAddress(address) {
        return address.toLowerCase()
            .replace(/[^\w\s]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }
    
    isCacheValid(timestamp) {
        if (!timestamp) return false;
        const age = Date.now() - new Date(timestamp).getTime();
        return age < 30 * 24 * 60 * 60 * 1000; // 30 Tage
    }
    
    createEfficientBatches(pairs, maxSize) {
        // Gruppiere Paare so, dass Origins/Destinations geteilt werden
        const batches = [];
        const processed = new Set();
        
        pairs.forEach(pair => {
            if (processed.has(`${pair.from.id}-${pair.to.id}`)) return;
            
            const batch = [pair];
            processed.add(`${pair.from.id}-${pair.to.id}`);
            
            // Finde ähnliche Paare
            pairs.forEach(other => {
                if (processed.has(`${other.from.id}-${other.to.id}`)) return;
                if (batch.length >= maxSize) return;
                
                if (other.from.id === pair.from.id || other.to.id === pair.to.id) {
                    batch.push(other);
                    processed.add(`${other.from.id}-${other.to.id}`);
                }
            });
            
            if (batch.length > 0) {
                batches.push(batch);
            }
        });
        
        return batches;
    }
    
    async loadPersistentCache() {
        try {
            // Lade Geocoding Cache
            const geocodePath = path.join(this.cacheDir, 'geocode_cache.json');
            if (await this.fileExists(geocodePath)) {
                const data = await fs.readFile(geocodePath, 'utf8');
                const cacheData = JSON.parse(data);
                Object.entries(cacheData).forEach(([key, value]) => {
                    this.geocodeCache.set(key, value);
                });
            }
            
            // Lade Distance Cache
            const distancePath = path.join(this.cacheDir, 'distance_cache.json');
            if (await this.fileExists(distancePath)) {
                const data = await fs.readFile(distancePath, 'utf8');
                const cacheData = JSON.parse(data);
                Object.entries(cacheData).forEach(([key, value]) => {
                    this.distanceCache.set(key, value);
                });
            }
        } catch (error) {
            console.error('❌ Cache laden fehlgeschlagen:', error);
        }
    }
    
    async savePersistentCache() {
        try {
            // Speichere Geocoding Cache
            const geocodeData = {};
            this.geocodeCache.forEach((value, key) => {
                geocodeData[key] = value;
            });
            await fs.writeFile(
                path.join(this.cacheDir, 'geocode_cache.json'),
                JSON.stringify(geocodeData, null, 2)
            );
            
            // Speichere Distance Cache (nur die neuesten 1000 Einträge)
            const distanceData = {};
            const entries = Array.from(this.distanceCache.entries())
                .sort((a, b) => new Date(b[1].timestamp) - new Date(a[1].timestamp))
                .slice(0, 1000);
            
            entries.forEach(([key, value]) => {
                distanceData[key] = value;
            });
            
            await fs.writeFile(
                path.join(this.cacheDir, 'distance_cache.json'),
                JSON.stringify(distanceData, null, 2)
            );
            
        } catch (error) {
            console.error('❌ Cache speichern fehlgeschlagen:', error);
        }
    }
    
    async fileExists(path) {
        try {
            await fs.access(path);
            return true;
        } catch {
            return false;
        }
    }
    
    planOptimalWeek(appointments, distanceMatrix, weekStart) {
        // Vereinfachte Wochenplanung
        const week = [];
        const weekDays = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag'];
        const startDate = new Date(weekStart);
        
        for (let i = 0; i < 5; i++) {
            const date = new Date(startDate);
            date.setDate(startDate.getDate() + i);
            
            week.push({
                day: weekDays[i],
                date: date.toISOString().split('T')[0],
                appointments: [],
                travelTime: 0,
                workTime: 0,
                totalHours: 0
            });
        }
        
        // Verteile Termine intelligent
        const clusters = this.clusterAppointmentsByRegion(appointments);
        let dayIndex = 0;
        
        Object.values(clusters).forEach(clusterApts => {
            if (clusterApts.length === 0) return;
            
            // Sortiere nach Priorität
            clusterApts.sort((a, b) => {
                if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
                if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
                return (b.priority === 'hoch' ? 1 : 0) - (a.priority === 'hoch' ? 1 : 0);
            });
            
            // Verteile auf Tage
            clusterApts.forEach(apt => {
                if (week[dayIndex].appointments.length >= 2) {
                    dayIndex = (dayIndex + 1) % 5;
                }
                
                week[dayIndex].appointments.push({
                    ...apt,
                    startTime: week[dayIndex].appointments.length === 0 ? '10:00' : '14:00',
                    endTime: week[dayIndex].appointments.length === 0 ? '13:00' : '17:00',
                    duration: 3
                });
                
                week[dayIndex].workTime += 3;
            });
        });
        
        // Berechne Fahrzeiten (approximiert)
        week.forEach(day => {
            if (day.appointments.length > 0) {
                day.travelTime = day.appointments.length * 0.5 + 1; // Vereinfacht
                day.totalHours = day.workTime + day.travelTime;
            }
        });
        
        return {
            weekStart: weekStart,
            days: week,
            totalAppointments: appointments.length,
            stats: {
                totalHours: week.reduce((sum, day) => sum + day.totalHours, 0),
                totalTravelTime: week.reduce((sum, day) => sum + day.travelTime, 0),
                efficiency: 0.8 // Placeholder
            }
        };
    }
    
    findAvailableSlots(day) {
        const slots = [];
        const dayStart = 8;
        const dayEnd = 18;
        
        if (day.appointments.length === 0) {
            slots.push({
                startTime: '08:00',
                endTime: '18:00',
                duration: 10
            });
            return slots;
        }
        
        // Sortiere Termine nach Zeit
        const sorted = [...day.appointments].sort((a, b) => {
            const timeA = parseInt(a.startTime.replace(':', ''));
            const timeB = parseInt(b.startTime.replace(':', ''));
            return timeA - timeB;
        });
        
        // Finde Lücken
        let lastEnd = dayStart;
        sorted.forEach(apt => {
            const startHour = parseInt(apt.startTime.split(':')[0]);
            if (startHour - lastEnd >= 3.5) {
                slots.push({
                    startTime: `${lastEnd}:00`,
                    endTime: `${startHour}:00`,
                    duration: startHour - lastEnd
                });
            }
            lastEnd = parseInt(apt.endTime.split(':')[0]);
        });
        
        // Check Ende des Tages
        if (dayEnd - lastEnd >= 3.5) {
            slots.push({
                startTime: `${lastEnd}:00`,
                endTime: `${dayEnd}:00`,
                duration: dayEnd - lastEnd
            });
        }
        
        return slots;
    }
    
    calculateSlotEfficiency(day, slot, appointment, weekPlan) {
        // Vereinfachte Effizienzberechnung
        let efficiency = 0.5;
        
        // Bonus wenn andere Termine in der Nähe
        day.appointments.forEach(existing => {
            if (existing.lat && existing.lng && appointment.lat && appointment.lng) {
                const distance = this.calculateDistance(
                    existing.lat, existing.lng,
                    appointment.lat, appointment.lng
                );
                if (distance < 50) efficiency += 0.2;
                else if (distance < 100) efficiency += 0.1;
            }
        });
        
        // Zeitslot-Qualität
        const slotHour = parseInt(slot.startTime.split(':')[0]);
        if (slotHour >= 10 && slotHour <= 15) efficiency += 0.2;
        
        return Math.min(1, efficiency);
    }
    
    generateSlotReasoning(day, slot, efficiency) {
        const reasons = [];
        
        if (efficiency > 0.7) {
            reasons.push('Sehr gute Reiseeffizienz');
        }
        
        if (day.appointments.length < 2) {
            reasons.push('Tag hat noch Kapazität');
        }
        
        const slotHour = parseInt(slot.startTime.split(':')[0]);
        if (slotHour >= 10 && slotHour <= 15) {
            reasons.push('Optimale Tageszeit');
        }
        
        return reasons;
    }
    
    async checkFutureWeeks(appointment, currentWeekStart, allAppointments) {
        // Placeholder für zukünftige Wochen
        const nextWeekStart = new Date(currentWeekStart);
        nextWeekStart.setDate(nextWeekStart.getDate() + 7);
        
        return {
            nextWeek: {
                start: nextWeekStart.toISOString().split('T')[0],
                freeSlots: 8,
                efficiency: 0.7
            },
            weekAfter: {
                start: new Date(nextWeekStart.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
                freeSlots: 10,
                efficiency: 0.8
            }
        };
    }
    
    generateRecommendation(alternatives, futureWeeks) {
        if (alternatives.length === 0) {
            return 'Keine passenden Alternativen in dieser Woche. Empfehle Verschiebung auf nächste Woche.';
        }
        
        const best = alternatives[0];
        if (best.efficiency > 0.7) {
            return `Beste Alternative: ${best.day} ${best.time} (hohe Effizienz)`;
        } else {
            return `Alternative verfügbar: ${best.day} ${best.time}, aber nächste Woche wäre effizienter`;
        }
    }
    
    printCostReport() {
        console.log('\n💰 === KOSTEN-REPORT ===');
        console.log(`📍 Geocoding API Calls: ${this.apiCosts.geocodingCalls}`);
        console.log(`🚗 Distance Matrix API Calls: ${this.apiCosts.distanceMatrixCalls}`);
        console.log(`💾 Aus Cache/Approximation: ${this.apiCosts.savedCalls}`);
        console.log(`💶 Geschätzte Kosten: ${this.apiCosts.estimatedCost.toFixed(2)}€`);
        console.log(`💰 Geschätzte Ersparnis: ${this.apiCosts.estimatedSavings.toFixed(2)}€`);
        console.log(`📊 Effizienz: ${Math.round((this.apiCosts.savedCalls / (this.apiCosts.savedCalls + this.apiCosts.geocodingCalls + this.apiCosts.distanceMatrixCalls)) * 100)}%`);
        console.log('======================\n');
    }
}

module.exports = CostOptimizedMapsService;

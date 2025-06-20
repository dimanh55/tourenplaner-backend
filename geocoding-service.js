// ======================================================================
// ENHANCED GEOCODING SERVICE FÜR DEUTSCHE ADRESSEN
// Datei: geocoding-service.js
// ======================================================================

const axios = require('axios');

class EnhancedGeocodingService {
    constructor(dbInstance) {
        this.apiKey = process.env.GOOGLE_MAPS_API_KEY;
        if (!this.apiKey) {
            throw new Error('Google Maps API Key nicht konfiguriert');
        }
        this.requestCount = 0;
        this.cache = new Map(); // Simple in-memory cache
        this.googleApiDisabled = false;
        this.db = dbInstance;

        if (this.db) {
            this.db.run(
                `CREATE TABLE IF NOT EXISTS geocoding_cache (
                    address TEXT PRIMARY KEY,
                    lat REAL NOT NULL,
                    lng REAL NOT NULL,
                    formatted_address TEXT,
                    accuracy TEXT,
                    method TEXT,
                    cached_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )`,
                (err) => {
                    if (err) {
                        console.error('❌ Geocoding Cache Tabelle konnte nicht erstellt werden:', err.message);
                    } else {
                        console.log('✅ Geocoding Cache Tabelle verifiziert');
                    }
                }
            );
        }
        
        // Deutsche Städte mit präzisen Koordinaten
        this.germanCitiesDatabase = new Map([
            // Großstädte
            ['berlin', { lat: 52.5200, lng: 13.4050, name: 'Berlin' }],
            ['hamburg', { lat: 53.5511, lng: 9.9937, name: 'Hamburg' }],
            ['münchen', { lat: 48.1351, lng: 11.5820, name: 'München' }],
            ['köln', { lat: 50.9375, lng: 6.9603, name: 'Köln' }],
            ['frankfurt am main', { lat: 50.1109, lng: 8.6821, name: 'Frankfurt am Main' }],
            ['frankfurt', { lat: 50.1109, lng: 8.6821, name: 'Frankfurt am Main' }],
            ['stuttgart', { lat: 48.7758, lng: 9.1829, name: 'Stuttgart' }],
            ['düsseldorf', { lat: 51.2277, lng: 6.7735, name: 'Düsseldorf' }],
            ['leipzig', { lat: 51.3397, lng: 12.3731, name: 'Leipzig' }],
            ['hannover', { lat: 52.3759, lng: 9.7320, name: 'Hannover' }],
            ['nürnberg', { lat: 49.4521, lng: 11.0767, name: 'Nürnberg' }],
            ['bremen', { lat: 53.0793, lng: 8.8017, name: 'Bremen' }],
            ['dresden', { lat: 51.0504, lng: 13.7373, name: 'Dresden' }],
            ['dortmund', { lat: 51.5136, lng: 7.4653, name: 'Dortmund' }],
            ['essen', { lat: 51.4556, lng: 7.0116, name: 'Essen' }],
            ['bochum', { lat: 51.4819, lng: 7.2162, name: 'Bochum' }],
            ['wuppertal', { lat: 51.2562, lng: 7.1508, name: 'Wuppertal' }],
            ['bielefeld', { lat: 52.0302, lng: 8.5325, name: 'Bielefeld' }],
            ['bonn', { lat: 50.7374, lng: 7.0982, name: 'Bonn' }],
            ['münster', { lat: 51.9607, lng: 7.6261, name: 'Münster' }],
            
            // Mittelstädte
            ['karlsruhe', { lat: 49.0069, lng: 8.4037, name: 'Karlsruhe' }],
            ['mannheim', { lat: 49.4875, lng: 8.4660, name: 'Mannheim' }],
            ['augsburg', { lat: 48.3705, lng: 10.8978, name: 'Augsburg' }],
            ['wiesbaden', { lat: 50.0782, lng: 8.2398, name: 'Wiesbaden' }],
            ['mönchengladbach', { lat: 51.1805, lng: 6.4428, name: 'Mönchengladbach' }],
            ['braunschweig', { lat: 52.2689, lng: 10.5268, name: 'Braunschweig' }],
            ['chemnitz', { lat: 50.8278, lng: 12.9214, name: 'Chemnitz' }],
            ['kiel', { lat: 54.3233, lng: 10.1228, name: 'Kiel' }],
            ['aachen', { lat: 50.7753, lng: 6.0839, name: 'Aachen' }],
            ['halle', { lat: 51.4969, lng: 11.9687, name: 'Halle (Saale)' }],
            ['magdeburg', { lat: 52.1205, lng: 11.6276, name: 'Magdeburg' }],
            ['freiburg', { lat: 47.9990, lng: 7.8421, name: 'Freiburg im Breisgau' }],
            ['krefeld', { lat: 51.3388, lng: 6.5853, name: 'Krefeld' }],
            ['lübeck', { lat: 53.8655, lng: 10.6866, name: 'Lübeck' }],
            ['erfurt', { lat: 50.9848, lng: 11.0299, name: 'Erfurt' }],
            ['mainz', { lat: 49.9929, lng: 8.2473, name: 'Mainz' }],
            ['rostock', { lat: 54.0887, lng: 12.1338, name: 'Rostock' }],
            ['kassel', { lat: 51.3127, lng: 9.4797, name: 'Kassel' }],
            ['hagen', { lat: 51.3670, lng: 7.4637, name: 'Hagen' }],
            ['hamm', { lat: 51.6792, lng: 7.8145, name: 'Hamm' }],
            ['saarbrücken', { lat: 49.2401, lng: 6.9969, name: 'Saarbrücken' }],
            ['mülheim', { lat: 51.4266, lng: 6.8826, name: 'Mülheim an der Ruhr' }],
            ['potsdam', { lat: 52.3906, lng: 13.0645, name: 'Potsdam' }],
            ['ludwigshafen', { lat: 49.4774, lng: 8.4452, name: 'Ludwigshafen' }],
            ['oldenburg', { lat: 53.1435, lng: 8.2146, name: 'Oldenburg' }],
            ['leverkusen', { lat: 51.0459, lng: 6.9891, name: 'Leverkusen' }],
            ['osnabrück', { lat: 52.2799, lng: 8.0472, name: 'Osnabrück' }],
            ['solingen', { lat: 51.1657, lng: 7.0678, name: 'Solingen' }],
            ['heidelberg', { lat: 49.3988, lng: 8.6724, name: 'Heidelberg' }],
            ['herne', { lat: 51.5386, lng: 7.2253, name: 'Herne' }],
            ['neuss', { lat: 51.1979, lng: 6.6854, name: 'Neuss' }],
            ['darmstadt', { lat: 49.8728, lng: 8.6512, name: 'Darmstadt' }],
            ['paderborn', { lat: 51.7189, lng: 8.7575, name: 'Paderborn' }],
            ['regensburg', { lat: 49.0134, lng: 12.1016, name: 'Regensburg' }],
            ['ingolstadt', { lat: 48.7665, lng: 11.4257, name: 'Ingolstadt' }],
            ['würzburg', { lat: 49.7913, lng: 9.9534, name: 'Würzburg' }],
            ['fürth', { lat: 49.4771, lng: 10.9886, name: 'Fürth' }],
            ['wolfsburg', { lat: 52.4227, lng: 10.7865, name: 'Wolfsburg' }],
            ['offenbach', { lat: 50.0955, lng: 8.7761, name: 'Offenbach am Main' }],
            ['ulm', { lat: 48.3974, lng: 9.9934, name: 'Ulm' }],
            ['heilbronn', { lat: 49.1427, lng: 9.2109, name: 'Heilbronn' }],
            ['pforzheim', { lat: 48.8914, lng: 8.6942, name: 'Pforzheim' }],
            ['göttingen', { lat: 51.5412, lng: 9.9158, name: 'Göttingen' }],
            ['bottrop', { lat: 51.5216, lng: 6.9289, name: 'Bottrop' }],
            ['trier', { lat: 49.7596, lng: 6.6441, name: 'Trier' }],
            ['recklinghausen', { lat: 51.6142, lng: 7.1956, name: 'Recklinghausen' }],
            ['reutlingen', { lat: 48.4919, lng: 9.2108, name: 'Reutlingen' }],
            ['bremerhaven', { lat: 53.5396, lng: 8.5806, name: 'Bremerhaven' }]
        ]);

        // PLZ-Bereiche für grobe Lokalisierung
        this.plzRegions = new Map([
            ['0', { lat: 51.0504, lng: 13.7373, region: 'Sachsen (Dresden)' }],
            ['1', { lat: 52.5200, lng: 13.4050, region: 'Berlin/Brandenburg' }],
            ['2', { lat: 53.5511, lng: 9.9937, region: 'Hamburg/Schleswig-Holstein' }],
            ['3', { lat: 52.3759, lng: 9.7320, region: 'Niedersachsen' }],
            ['4', { lat: 51.5136, lng: 7.4653, region: 'Nordrhein-Westfalen' }],
            ['5', { lat: 50.9375, lng: 6.9603, region: 'NRW/Rheinland-Pfalz' }],
            ['6', { lat: 50.1109, lng: 8.6821, region: 'Hessen/Rheinland-Pfalz' }],
            ['7', { lat: 48.7758, lng: 9.1829, region: 'Baden-Württemberg' }],
            ['8', { lat: 48.1351, lng: 11.5820, region: 'Bayern (München)' }],
            ['9', { lat: 49.4521, lng: 11.0767, region: 'Bayern/Thüringen' }]
        ]);

        console.log('🗺️ Enhanced Geocoding Service initialisiert');
        console.log(`📊 ${this.germanCitiesDatabase.size} deutsche Städte in Datenbank`);
        console.log(`🔑 Google Maps API: ${this.apiKey ? 'Verfügbar' : 'Nicht verfügbar'}`);
    }

    // ======================================================================
    // HAUPT-GEOCODING FUNKTION
    // ======================================================================
    async geocodeAddress(address) {
        if (!address || typeof address !== 'string' || address.trim().length === 0) {
            throw new Error('Ungültige Adresse');
        }

        const cleanAddress = address.trim();
        const cacheKey = cleanAddress.toLowerCase();

        if (this.cache.has(cacheKey)) {
            console.log(`📋 Memory Cache Hit: ${address}`);
            return this.cache.get(cacheKey);
        }

        const dbCached = await this.getGeocodingFromDB(cleanAddress);
        if (dbCached) {
            console.log(`💾 DB Cache Hit: ${address}`);
            this.cache.set(cacheKey, dbCached);
            return dbCached;
        }

        console.log(`🔍 Geocoding: ${address}`);

        let result = null;
        let method = 'unknown';

        try {
            // Schritt 1: Google Maps API versuchen
            result = await this.geocodeWithGoogleMaps(cleanAddress);
            method = 'google_maps';
            console.log(`✅ Google Maps: ${address} → ${result.lat}, ${result.lng}`);
        } catch (googleError) {
            console.warn(`⚠️ Google Maps fehlgeschlagen für "${address}": ${googleError.message}`);
            
            try {
                // Schritt 2: Intelligente Adress-Analyse
                result = await this.geocodeWithIntelligentAnalysis(cleanAddress);
                method = 'intelligent_analysis';
                console.log(`🧠 Intelligente Analyse: ${address} → ${result.lat}, ${result.lng}`);
            } catch (analysisError) {
                console.warn(`⚠️ Intelligente Analyse fehlgeschlagen: ${analysisError.message}`);
                
                try {
                    // Schritt 3: PLZ-basierte Lokalisierung
                    result = this.geocodeWithPLZ(cleanAddress);
                    method = 'plz_based';
                    console.log(`📮 PLZ-basiert: ${address} → ${result.lat}, ${result.lng}`);
                } catch (plzError) {
                    // Schritt 4: Fallback auf Deutschland-Zentrum
                    result = {
                        lat: 51.1657, 
                        lng: 10.4515, 
                        formatted_address: 'Deutschland (Fallback)',
                        accuracy: 'country'
                    };
                    method = 'fallback';
                    console.log(`🏳️ Fallback: ${address} → Deutschland-Zentrum`);
                }
            }
        }

        // Ergebnis anreichern
        const enrichedResult = {
            ...result,
            geocoding_method: method,
            original_address: address,
            processed_at: new Date().toISOString()
        };

        this.cache.set(cacheKey, enrichedResult);

        if (result) {
            this.saveGeocodingToDB(cleanAddress, enrichedResult);
        }

        return enrichedResult;
    }

    // ======================================================================
    // GOOGLE MAPS API GEOCODING
    // ======================================================================
    async geocodeWithGoogleMaps(address) {
        if (this.googleApiDisabled) {
            throw new Error('Google Maps API deaktiviert');
        }
        if (!this.apiKey || this.apiKey === 'YOUR_API_KEY_HERE') {
            this.googleApiDisabled = true;
            throw new Error('Google Maps API Key nicht verfügbar');
        }

        this.requestCount++;
        console.log(`🌐 Google Maps API Request #${this.requestCount}: ${address}`);

        try {
            const response = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
                params: {
                    address: address,
                    key: this.apiKey,
                    region: 'de',
                    components: 'country:DE',
                    language: 'de'
                },
                timeout: 8000
            });

            if (response.data.status === 'OK' && response.data.results.length > 0) {
                const result = response.data.results[0];
                const location = result.geometry.location;
                
                // Prüfe ob Ergebnis in Deutschland liegt
                if (location.lat < 47.2 || location.lat > 55.1 || location.lng < 5.8 || location.lng > 15.1) {
                    throw new Error('Ergebnis liegt außerhalb Deutschlands');
                }

                return {
                    lat: location.lat,
                    lng: location.lng,
                    formatted_address: result.formatted_address,
                    accuracy: result.geometry.location_type?.toLowerCase() || 'approximate',
                    place_id: result.place_id,
                    components: this.parseAddressComponents(result.address_components)
                };
            } else {
                throw new Error(`Google Maps API Status: ${response.data.status} - ${response.data.error_message || 'Unbekannter Fehler'}`);
            }
        } catch (error) {
            if (error.code === 'ECONNABORTED') {
                throw new Error('Google Maps API Timeout');
            } else if (error.response?.status === 429) {
                throw new Error('Google Maps API Rate Limit erreicht');
            } else if (error.response?.status === 403) {
                this.googleApiDisabled = true;
                throw new Error('Google Maps API Key ungültig oder deaktiviert');
            } else {
                if (error.response?.data?.status === 'REQUEST_DENIED') {
                    this.googleApiDisabled = true;
                }
                throw new Error(`Google Maps API Fehler: ${error.message}`);
            }
        }
    }

    // ======================================================================
    // INTELLIGENTE ADRESS-ANALYSE (OHNE API)
    // ======================================================================
    async geocodeWithIntelligentAnalysis(address) {
        console.log(`🧠 Starte intelligente Analyse für: ${address}`);

        // Adresse normalisieren und analysieren
        const normalized = this.normalizeGermanAddress(address);
        console.log(`🔄 Normalisiert: ${JSON.stringify(normalized)}`);

        // 1. Direkte Stadtsuche
        if (normalized.city) {
            const cityKey = normalized.city.toLowerCase();
            if (this.germanCitiesDatabase.has(cityKey)) {
                const cityData = this.germanCitiesDatabase.get(cityKey);
                return {
                    lat: cityData.lat + (Math.random() - 0.5) * 0.02, // Kleine Variation für Genauigkeit
                    lng: cityData.lng + (Math.random() - 0.5) * 0.02,
                    formatted_address: `${normalized.street ? normalized.street + ', ' : ''}${cityData.name}${normalized.plz ? ' (' + normalized.plz + ')' : ''}`,
                    accuracy: 'city',
                    confidence: 'high'
                };
            }
        }

        // 2. Ähnlichkeitssuche für Städte
        if (normalized.city && normalized.city.length >= 3) {
            const similarCity = this.findSimilarCity(normalized.city);
            if (similarCity) {
                console.log(`🎯 Ähnliche Stadt gefunden: ${normalized.city} → ${similarCity.name}`);
                return {
                    lat: similarCity.lat + (Math.random() - 0.5) * 0.02,
                    lng: similarCity.lng + (Math.random() - 0.5) * 0.02,
                    formatted_address: `${normalized.street ? normalized.street + ', ' : ''}${similarCity.name}${normalized.plz ? ' (' + normalized.plz + ')' : ''} (ähnlich zu ${normalized.city})`,
                    accuracy: 'approximate',
                    confidence: 'medium'
                };
            }
        }

        // 3. PLZ-basierte Lokalisierung als Fallback
        if (normalized.plz) {
            return this.geocodeWithPLZ(address);
        }

        throw new Error('Keine intelligente Analyse möglich');
    }

    // ======================================================================
    // DEUTSCHE ADRESS-NORMALISIERUNG
    // ======================================================================
    normalizeGermanAddress(address) {
        const normalized = {
            original: address,
            street: null,
            houseNumber: null,
            plz: null,
            city: null,
            state: null
        };

        // PLZ extrahieren (5 Ziffern)
        const plzMatch = address.match(/\b(\d{5})\b/);
        if (plzMatch) {
            normalized.plz = plzMatch[1];
        }

        // Stadt extrahieren (nach PLZ oder am Ende)
        let cityMatch = null;
        if (normalized.plz) {
            // Stadt nach PLZ suchen
            cityMatch = address.match(new RegExp(`${normalized.plz}\\s+([^,]+)`));
            if (cityMatch) {
                normalized.city = cityMatch[1].trim();
            }
        } else {
            // Stadt am Ende der Adresse suchen
            const parts = address.split(',').map(p => p.trim());
            if (parts.length > 1) {
                normalized.city = parts[parts.length - 1];
            }
        }

        // Straße und Hausnummer extrahieren
        const streetMatch = address.match(/^([^,\d]*\d*[^,]*?)(?:,|\s+\d{5})/);
        if (streetMatch) {
            const streetPart = streetMatch[1].trim();
            const houseMatch = streetPart.match(/^(.+?)\s+(\d+.*)$/);
            if (houseMatch) {
                normalized.street = houseMatch[1].trim();
                normalized.houseNumber = houseMatch[2].trim();
            } else {
                normalized.street = streetPart;
            }
        }

        return normalized;
    }

    // ======================================================================
    // PLZ-BASIERTE LOKALISIERUNG
    // ======================================================================
    geocodeWithPLZ(address) {
        const plzMatch = address.match(/\b(\d{5})\b/);
        if (!plzMatch) {
            throw new Error('Keine PLZ in Adresse gefunden');
        }

        const plz = plzMatch[1];
        const firstDigit = plz[0];
        
        if (this.plzRegions.has(firstDigit)) {
            const regionData = this.plzRegions.get(firstDigit);
            
            // Genauere Koordinaten basierend auf PLZ-Bereich
            const latOffset = (parseInt(plz.substring(1, 3)) - 50) * 0.01;
            const lngOffset = (parseInt(plz.substring(3, 5)) - 50) * 0.01;
            
            return {
                lat: regionData.lat + latOffset,
                lng: regionData.lng + lngOffset,
                formatted_address: `${plz} ${regionData.region}`,
                accuracy: 'postal_code',
                confidence: 'medium',
                plz_region: regionData.region
            };
        }

        throw new Error(`Unbekannte PLZ-Region: ${firstDigit}`);
    }

    // ======================================================================
    // ÄHNLICHKEITSSUCHE FÜR STÄDTE
    // ======================================================================
    findSimilarCity(searchCity) {
        const searchLower = searchCity.toLowerCase();
        let bestMatch = null;
        let bestScore = 0;

        for (const [cityKey, cityData] of this.germanCitiesDatabase) {
            const score = this.calculateSimilarity(searchLower, cityKey);
            if (score > bestScore && score > 0.6) { // Mindestähnlichkeit 60%
                bestScore = score;
                bestMatch = cityData;
            }
        }

        return bestMatch;
    }

    // ======================================================================
    // STRING-ÄHNLICHKEIT BERECHNEN (LEVENSHTEIN)
    // ======================================================================
    calculateSimilarity(str1, str2) {
        const matrix = [];
        const len1 = str1.length;
        const len2 = str2.length;

        if (len1 === 0) return len2 === 0 ? 1 : 0;
        if (len2 === 0) return 0;

        // Matrix initialisieren
        for (let i = 0; i <= len2; i++) {
            matrix[i] = [i];
        }
        for (let j = 0; j <= len1; j++) {
            matrix[0][j] = j;
        }

        // Levenshtein-Distanz berechnen
        for (let i = 1; i <= len2; i++) {
            for (let j = 1; j <= len1; j++) {
                if (str2.charAt(i - 1) === str1.charAt(j - 1)) {
                    matrix[i][j] = matrix[i - 1][j - 1];
                } else {
                    matrix[i][j] = Math.min(
                        matrix[i - 1][j - 1] + 1, // substitution
                        matrix[i][j - 1] + 1,     // insertion
                        matrix[i - 1][j] + 1      // deletion
                    );
                }
            }
        }

        // Ähnlichkeit als Prozentsatz
        const maxLen = Math.max(len1, len2);
        return (maxLen - matrix[len2][len1]) / maxLen;
    }

    // ======================================================================
    // ADRESSKOMPONENTEN PARSEN
    // ======================================================================
    parseAddressComponents(components) {
        const parsed = {};
        
        for (const component of components) {
            const types = component.types;
            
            if (types.includes('street_number')) {
                parsed.street_number = component.long_name;
            } else if (types.includes('route')) {
                parsed.street = component.long_name;
            } else if (types.includes('locality')) {
                parsed.city = component.long_name;
            } else if (types.includes('postal_code')) {
                parsed.postal_code = component.long_name;
            } else if (types.includes('administrative_area_level_1')) {
                parsed.state = component.long_name;
            } else if (types.includes('country')) {
                parsed.country = component.long_name;
            }
        }
        
        return parsed;
    }

    // ======================================================================
    // BATCH-GEOCODING FÜR MEHRERE ADRESSEN
    // ======================================================================
    async geocodeMultipleAddresses(addresses, options = {}) {
        const { 
            maxConcurrent = 5, 
            delayBetweenRequests = 200,
            onProgress = null 
        } = options;

        console.log(`🔄 Batch-Geocoding für ${addresses.length} Adressen...`);
        
        const results = [];
        const batches = [];
        
        // Adressen in Batches aufteilen
        for (let i = 0; i < addresses.length; i += maxConcurrent) {
            batches.push(addresses.slice(i, i + maxConcurrent));
        }

        let processed = 0;
        
        for (const batch of batches) {
            const batchPromises = batch.map(async (address, index) => {
                try {
                    const result = await this.geocodeAddress(address);
                    processed++;
                    
                    if (onProgress) {
                        onProgress({
                            processed,
                            total: addresses.length,
                            current: address,
                            success: true
                        });
                    }
                    
                    return { address, result, success: true };
                } catch (error) {
                    processed++;
                    console.error(`❌ Geocoding fehlgeschlagen für "${address}": ${error.message}`);
                    
                    if (onProgress) {
                        onProgress({
                            processed,
                            total: addresses.length,
                            current: address,
                            success: false,
                            error: error.message
                        });
                    }
                    
                    return { address, error: error.message, success: false };
                }
            });

            const batchResults = await Promise.all(batchPromises);
            results.push(...batchResults);

            // Pause zwischen Batches
            if (delayBetweenRequests > 0 && batches.indexOf(batch) < batches.length - 1) {
                await new Promise(resolve => setTimeout(resolve, delayBetweenRequests));
            }
        }

        const successful = results.filter(r => r.success).length;
        const failed = results.length - successful;
        
        console.log(`✅ Batch-Geocoding abgeschlossen: ${successful} erfolgreich, ${failed} fehlgeschlagen`);
        
        return {
            results,
            summary: {
                total: addresses.length,
                successful,
                failed,
                successRate: (successful / addresses.length) * 100
            }
        };
    }

    // ======================================================================
    // CACHE-MANAGEMENT
    // ======================================================================
    clearCache() {
        this.cache.clear();
        console.log('🧹 Geocoding Cache geleert');
    }

    getCacheStats() {
        return {
            size: this.cache.size,
            requestCount: this.requestCount,
            hitRate: this.cache.size > 0 ? (this.cache.size / this.requestCount) * 100 : 0
        };
    }

    async getGeocodingFromDB(address) {
        if (!this.db) return null;

        return new Promise((resolve) => {
            this.db.get(
                `SELECT lat, lng, formatted_address, accuracy, method 
                 FROM geocoding_cache 
                 WHERE address = ? 
                 AND cached_at > datetime('now', '-90 days')`,
                [address.toLowerCase()],
                (err, row) => {
                    if (err || !row) {
                        resolve(null);
                    } else {
                        resolve({
                            lat: row.lat,
                            lng: row.lng,
                            formatted_address: row.formatted_address,
                            accuracy: row.accuracy,
                            geocoding_method: row.method,
                            cached: true
                        });
                    }
                }
            );
        });
    }

    async saveGeocodingToDB(address, result) {
        if (!this.db) return;

        this.db.run(
            `INSERT OR REPLACE INTO geocoding_cache 
             (address, lat, lng, formatted_address, accuracy, method, cached_at)
             VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`,
            [
                address.toLowerCase(),
                result.lat,
                result.lng,
                result.formatted_address || '',
                result.accuracy || 'unknown',
                result.geocoding_method || 'unknown'
            ],
            (err) => {
                if (err) console.error('Geocoding Cache-Speicherfehler:', err);
            }
        );
    }

    // ======================================================================
    // KOORDINATEN VALIDIERUNG
    // ======================================================================
    isValidGermanCoordinates(lat, lng) {
        // Deutschland Bounding Box
        return lat >= 47.2 && lat <= 55.1 && lng >= 5.8 && lng <= 15.1;
    }
}

module.exports = EnhancedGeocodingService;

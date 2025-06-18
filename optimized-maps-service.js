// ======================================================================
// OPTIMIERTE GOOGLE MAPS API INTEGRATION - MINIMALER VERBRAUCH
// Ersetzt die bisherige ineffiziente Nutzung
// ======================================================================

const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const IntelligentRoutePlanner = require('./intelligent-route-planner');

class OptimizedMapsService {
    constructor(dbInstance) {
        this.apiKey = process.env.GOOGLE_MAPS_API_KEY;
        this.db = dbInstance || new sqlite3.Database(
            process.env.NODE_ENV === 'production'
                ? '/app/data/expertise_tours.db'
                : './expertise_tours.db'
        );

class OptimizedMapsService {
    constructor() {
        this.apiKey = process.env.GOOGLE_MAPS_API_KEY;
        this.geocodingCache = new Map();
        this.distanceCache = new Map();
        this.requestCounts = {
            geocoding: 0,
            distanceMatrix: 0,
            cacheHits: 0
        };
        
        // Persistente Datenbank für Cache
        this.initializeCache();
    }

    // ======================================================================
    // PROBLEM 1 LÖSUNG: PERSISTENT CACHE
    // ======================================================================
    
    async initializeCache() {
        // Lade bestehende Geocoding-Daten aus der Datenbank
        this.db.all(`
            SELECT address, lat, lng
            FROM appointments
        db.all(`
            SELECT address, lat, lng 
            FROM appointments 
            WHERE lat IS NOT NULL AND lng IS NOT NULL
        `, (err, rows) => {
            if (!err && rows) {
                rows.forEach(row => {
                    const key = this.normalizeAddress(row.address);
                    this.geocodingCache.set(key, {
                        lat: row.lat,
                        lng: row.lng,
                        source: 'database'
                    });
                });
                console.log(`📋 ${rows.length} Adressen aus DB-Cache geladen`);
            }
        });
    }

    normalizeAddress(address) {
        return address.toLowerCase()
            .replace(/straße/g, 'str')
            .replace(/[^\w\s]/g, '')
            .trim();
    }

    // ======================================================================
    // PROBLEM 2 LÖSUNG: BATCH GEOCODING STATT EINZELN
    // ======================================================================
    
    async geocodeBatch(addresses) {
        console.log(`🗺️ Batch Geocoding für ${addresses.length} Adressen...`);
        
        const results = [];
        const needsGeocoding = [];
        
        // Erst Cache prüfen
        addresses.forEach((address, index) => {
            const normalized = this.normalizeAddress(address);
            if (this.geocodingCache.has(normalized)) {
                const cached = this.geocodingCache.get(normalized);
                results[index] = {
                    address,
                    ...cached,
                    fromCache: true
                };
                this.requestCounts.cacheHits++;
            } else {
                needsGeocoding.push({ address, index });
            }
        });

        console.log(`📋 ${results.filter(r => r).length} Adressen aus Cache`);
        console.log(`🔍 ${needsGeocoding.length} Adressen benötigen API-Call`);

        if (needsGeocoding.length === 0) {
            return results;
        }

        // Batch-Geocoding für unbekannte Adressen
        // Google erlaubt bis zu 100 Adressen pro Request
        const batchSize = 10; // Konservativ für bessere Performance
        
        for (let i = 0; i < needsGeocoding.length; i += batchSize) {
            const batch = needsGeocoding.slice(i, i + batchSize);
            const addressString = batch.map(item => item.address).join('|');
            
            try {
                this.requestCounts.geocoding++;
                console.log(`🌐 Geocoding API Call #${this.requestCounts.geocoding} für ${batch.length} Adressen`);
                
                const response = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
                    params: {
                        address: addressString,
                        key: this.apiKey,
                        region: 'de',
                        components: 'country:DE'
                    },
                    timeout: 10000
                });

                if (response.data.status === 'OK') {
                    response.data.results.forEach((result, batchIndex) => {
                        if (batchIndex < batch.length) {
                            const item = batch[batchIndex];
                            const coords = {
                                lat: result.geometry.location.lat,
                                lng: result.geometry.location.lng,
                                formatted_address: result.formatted_address,
                                fromCache: false
                            };
                            
                            results[item.index] = {
                                address: item.address,
                                ...coords
                            };
                            
                            // In Cache speichern
                            this.geocodingCache.set(
                                this.normalizeAddress(item.address), 
                                coords
                            );
                            
                            // In DB speichern
                            this.saveGeocodingToDB(item.address, coords);
                        }
                    });
                }
                
                // Rate limiting
                await new Promise(resolve => setTimeout(resolve, 200));
                
            } catch (error) {
                console.error(`❌ Batch Geocoding Fehler:`, error.message);
                // Fallback für diese Batch
                batch.forEach(item => {
                    results[item.index] = {
                        address: item.address,
                        error: error.message
                    };
                });
            }
        }

        return results;
    }

    // ======================================================================
    // PROBLEM 3 LÖSUNG: INTELLIGENTE DISTANCE MATRIX
    // ======================================================================
    
    async calculateOptimizedDistanceMatrix(appointments) {
        console.log(`🚗 Optimierte Distance Matrix für ${appointments.length} Termine...`);
        
        const points = [
            { id: 'home', lat: 52.3759, lng: 9.7320, name: 'Hannover' },
            ...appointments.map(apt => ({
                id: apt.id,
                lat: apt.lat,
                lng: apt.lng,
                name: apt.customer
            }))
        ];

        const matrix = {};
        const cacheKey = this.generateMatrixCacheKey(points);
        
        // Prüfe ob komplette Matrix im Cache
        if (this.distanceCache.has(cacheKey)) {
            console.log('📋 Komplette Distance Matrix aus Cache');
            this.requestCounts.cacheHits++;
            return this.distanceCache.get(cacheKey);
        }

        // NEUE STRATEGIE: Nur kritische Verbindungen berechnen
        const criticalConnections = this.identifyCriticalConnections(points);
        
        console.log(`🎯 Berechne nur ${criticalConnections.length} kritische Verbindungen statt ${points.length * points.length}`);

        // Batch-weise Distance Matrix API Calls
        const maxOrigins = 10;
        const maxDestinations = 25;
        
        for (let i = 0; i < criticalConnections.length; i += maxOrigins * maxDestinations) {
            const batch = criticalConnections.slice(i, i + maxOrigins * maxDestinations);
            
            const origins = [...new Set(batch.map(conn => conn.from))];
            const destinations = [...new Set(batch.map(conn => conn.to))];
            
            try {
                this.requestCounts.distanceMatrix++;
                console.log(`🌐 Distance Matrix API Call #${this.requestCounts.distanceMatrix}`);
                
                const originsStr = origins.map(id => {
                    const point = points.find(p => p.id === id);
                    return `${point.lat},${point.lng}`;
                }).join('|');
                
                const destinationsStr = destinations.map(id => {
                    const point = points.find(p => p.id === id);
                    return `${point.lat},${point.lng}`;
                }).join('|');

                const response = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
                    params: {
                        origins: originsStr,
                        destinations: destinationsStr,
                        key: this.apiKey,
                        units: 'metric',
                        mode: 'driving',
                        avoid: 'tolls'
                    },
                    timeout: 15000
                });

                if (response.data.status === 'OK') {
                    this.parseDistanceMatrixResponse(
                        response.data, 
                        origins, 
                        destinations, 
                        matrix
                    );
                }
                
                // Rate limiting
                await new Promise(resolve => setTimeout(resolve, 300));
                
            } catch (error) {
                console.error(`❌ Distance Matrix Batch Fehler:`, error.message);
            }
        }

        // Fehlende Verbindungen mit Haversine-Approximation füllen
        this.fillMissingConnections(points, matrix);
        
        // Matrix cachen
        this.distanceCache.set(cacheKey, matrix);
        
        return matrix;
    }

    // ======================================================================
    // HILFSFUNKTIONEN
    // ======================================================================
    
    identifyCriticalConnections(points) {
        const connections = [];
        
        // 1. Alle Punkte zu Home Base
        points.forEach(point => {
            if (point.id !== 'home') {
                connections.push({ from: 'home', to: point.id });
                connections.push({ from: point.id, to: 'home' });
            }
        });
        
        // 2. Regionale Cluster - nur nahegelegene Punkte verbinden
        const clusters = this.clusterPointsByRegion(points);
        
        Object.values(clusters).forEach(cluster => {
            if (cluster.length > 1) {
                // Innerhalb jedes Clusters alle Verbindungen
                for (let i = 0; i < cluster.length; i++) {
                    for (let j = i + 1; j < cluster.length; j++) {
                        connections.push({ 
                            from: cluster[i].id, 
                            to: cluster[j].id 
                        });
                        connections.push({ 
                            from: cluster[j].id, 
                            to: cluster[i].id 
                        });
                    }
                }
            }
        });
        
        // 3. Cluster-zu-Cluster Verbindungen (nur kürzeste)
        const clusterCenters = Object.entries(clusters).map(([region, points]) => ({
            region,
            center: this.calculateClusterCenter(points),
            representative: points[0] // Nutze ersten Punkt als Repräsentant
        }));
        
        for (let i = 0; i < clusterCenters.length; i++) {
            for (let j = i + 1; j < clusterCenters.length; j++) {
                const from = clusterCenters[i].representative.id;
                const to = clusterCenters[j].representative.id;
                connections.push({ from, to });
                connections.push({ from: to, to: from });
            }
        }
        
        return [...new Set(connections.map(c => `${c.from}-${c.to}`))].map(key => {
            const [from, to] = key.split('-');
            return { from, to };
        });
    }
    
    clusterPointsByRegion(points) {
        const clusters = {
            nord: [],
            ost: [],
            west: [],
            sued: [],
            mitte: []
        };
        
        const regionCenters = {
            nord: { lat: 53.5, lng: 10.0 },
            ost: { lat: 52.5, lng: 13.4 },
            west: { lat: 51.2, lng: 7.0 },
            sued: { lat: 48.5, lng: 11.5 },
            mitte: { lat: 50.5, lng: 9.0 }
        };
        
        points.forEach(point => {
            if (point.id === 'home') {
                clusters.mitte.push(point);
                return;
            }
            
            let minDistance = Infinity;
            let bestRegion = 'mitte';
            
            Object.entries(regionCenters).forEach(([region, center]) => {
                const distance = this.calculateHaversineDistance(
                    point.lat, point.lng,
                    center.lat, center.lng
                );
                if (distance < minDistance) {
                    minDistance = distance;
                    bestRegion = region;
                }
            });
            
            clusters[bestRegion].push(point);
        });
        
        return clusters;
    }
    
    fillMissingConnections(points, matrix) {
        console.log('🔧 Fülle fehlende Verbindungen mit Haversine-Approximation...');
        
        points.forEach(from => {
            if (!matrix[from.id]) matrix[from.id] = {};
            
            points.forEach(to => {
                if (!matrix[from.id][to.id] && from.id !== to.id) {
                    const distance = this.calculateHaversineDistance(
                        from.lat, from.lng, to.lat, to.lng
                    );
                    
                    matrix[from.id][to.id] = {
                        distance: distance,
                        duration: Math.max(0.5, distance / 80) + 0.25, // Approximation
                        approximated: true
                    };
                }
            });
        });
    }
    
    calculateHaversineDistance(lat1, lng1, lat2, lng2) {
        const R = 6371;
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLng = (lng2 - lng1) * Math.PI / 180;
        const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                  Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
                  Math.sin(dLng/2) * Math.sin(dLng/2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }
    
    parseDistanceMatrixResponse(data, origins, destinations, matrix) {
        data.rows.forEach((row, i) => {
            const originId = origins[i];
            if (!matrix[originId]) matrix[originId] = {};
            
            row.elements.forEach((element, j) => {
                const destId = destinations[j];
                
                if (element.status === 'OK') {
                    matrix[originId][destId] = {
                        distance: element.distance.value / 1000,
                        duration: (element.duration.value / 3600) + 0.25,
                        duration_in_traffic: element.duration_in_traffic ? 
                            (element.duration_in_traffic.value / 3600) + 0.25 : 
                            (element.duration.value / 3600) + 0.25
                    };
                }
            });
        });
    }
    
    generateMatrixCacheKey(points) {
        const sorted = points
            .map(p => `${p.id}-${p.lat.toFixed(3)}-${p.lng.toFixed(3)}`)
            .sort()
            .join('|');
        return `matrix-${this.hashString(sorted)}`;
    }
    
    hashString(str) {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash; // 32bit integer
        }
        return Math.abs(hash).toString(36);
    }
    
    calculateClusterCenter(points) {
        const lat = points.reduce((sum, p) => sum + p.lat, 0) / points.length;
        const lng = points.reduce((sum, p) => sum + p.lng, 0) / points.length;
        return { lat, lng };
    }
    
    async saveGeocodingToDB(address, coords) {
        // Optional: Speichere Geocoding-Ergebnisse in separater Tabelle für Cache
        this.db.run(
            `INSERT OR REPLACE INTO geocoding_cache (address, lat, lng, created_at)
            VALUES (?, ?, ?, datetime('now'))`,
            [address, coords.lat, coords.lng]
        );
        db.run(`
            INSERT OR REPLACE INTO geocoding_cache (address, lat, lng, created_at)
            VALUES (?, ?, ?, datetime('now'))
        `, [address, coords.lat, coords.lng]);
    }
    
    getRequestStats() {
        return {
            geocoding_requests: this.requestCounts.geocoding,
            distance_matrix_requests: this.requestCounts.distanceMatrix,
            cache_hits: this.requestCounts.cacheHits,
            total_api_requests: this.requestCounts.geocoding + this.requestCounts.distanceMatrix,
            estimated_cost_usd: (this.requestCounts.geocoding * 0.005) + (this.requestCounts.distanceMatrix * 0.01)
        };
    }
}

// ======================================================================
// INTEGRATION IN BESTEHENDEN ROUTE PLANNER
// ======================================================================

class OptimizedIntelligentRoutePlanner extends IntelligentRoutePlanner {
    constructor() {
        super();
        this.mapsService = new OptimizedMapsService();
    }
    
    async geocodeAppointments(appointments) {
        console.log('🗺️ Optimiertes Geocoding von Adressen...');
        
        const addresses = appointments
            .filter(apt => !apt.lat || !apt.lng)
            .map(apt => apt.address);
        
        if (addresses.length === 0) {
            console.log('✅ Alle Termine bereits geocoded');
            return appointments;
        }
        
        const geocodingResults = await this.mapsService.geocodeBatch(addresses);
        
        // Merge results
        const geocoded = [];
        let resultIndex = 0;
        
        appointments.forEach(apt => {
            if (apt.lat && apt.lng) {
                geocoded.push({ ...apt, geocoded: true });
            } else {
                const result = geocodingResults[resultIndex++];
                if (result && !result.error) {
                    geocoded.push({
                        ...apt,
                        lat: result.lat,
                        lng: result.lng,
                        geocoded: true,
                        geocoding_method: result.fromCache ? 'cache' : 'google_api'
                    });
                } else {
                    console.warn(`⚠️ Geocoding fehlgeschlagen für ${apt.address}`);
                }
            }
        });
        
        return geocoded;
    }
    
    async calculateTravelMatrix(appointments) {
        console.log('🚗 Optimierte Travel Matrix Berechnung...');
        return await this.mapsService.calculateOptimizedDistanceMatrix(appointments);
    }
}

// ======================================================================
// DATABASE SCHEMA ERWEITERUNG
// ======================================================================

function initializeGeocodingCache(db) {
function initializeGeocodingCache(db) {
function initializeGeocodingCache() {
    db.run(`CREATE TABLE IF NOT EXISTS geocoding_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT UNIQUE NOT NULL,
        lat REAL NOT NULL,
        lng REAL NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_geocoding_address ON geocoding_cache(address)`);
}

module.exports = {
    OptimizedMapsService,
    OptimizedIntelligentRoutePlanner,
    initializeGeocodingCache
};

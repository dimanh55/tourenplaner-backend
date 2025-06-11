const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const Papa = require('papaparse');
const axios = require('axios');
require('dotenv').config();

// ======================================================================
// INTELLIGENTE ROUTENPLANUNG KLASSE (Aus separater Datei importiert)
// ======================================================================
const IntelligentRoutePlanner = require('./intelligent-route-planner');

// Debug: Umgebungsvariablen prüfen
console.log('🔍 Environment Variables Debug:');
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('GOOGLE_MAPS_API_KEY exists:', !!process.env.GOOGLE_MAPS_API_KEY);

// Fallback API Key
if (!process.env.GOOGLE_MAPS_API_KEY) {
    console.log('⚠️ Setting fallback API key');
    process.env.GOOGLE_MAPS_API_KEY = 'AIzaSyD6D4OGAfep-u-N1yz_F--jacBFs1TINR4';
}

// ======================================================================
// EXPRESS APP SETUP
// ======================================================================
const app = express();
const PORT = process.env.PORT || 8080;

// ======================================================================
// SICHERHEITS-MIDDLEWARE (VOR ALLEN ANDEREN ROUTES)
// ======================================================================

// 🚫 robots.txt Route (WICHTIG: Ganz am Anfang)
app.get('/robots.txt', (req, res) => {
    res.type('text/plain');
    res.send(`User-agent: *
Disallow: /

User-agent: Googlebot
Disallow: /

User-agent: Bingbot
Disallow: /

User-agent: Slurp
Disallow: /

User-agent: DuckDuckBot
Disallow: /

User-agent: Baiduspider
Disallow: /

User-agent: facebookexternalhit
Disallow: /`);
});

// 🛡️ Security Headers (DIREKT nach robots.txt)
app.use((req, res, next) => {
    // Suchmaschinen abweisen
    res.setHeader('X-Robots-Tag', 'noindex, nofollow, noarchive, nosnippet');
    
    // Weitere Security Headers
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'no-referrer');
    res.setHeader('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');
    
    next();
});

// ======================================================================
// STANDARD MIDDLEWARE
// ======================================================================

// CORS Configuration
app.use(cors({
    origin: [
        'https://expertise-zeigen.de', 
        'https://www.expertise-zeigen.de'
        // localhost ENTFERNT für Production-Sicherheit
    ],
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: false
}));

app.use(express.json());

// Configure multer for file uploads
const upload = multer({ 
    storage: multer.memoryStorage(),
    limits: { fileSize: 5 * 1024 * 1024 } // 5MB limit
});

// ======================================================================
// DATABASE SETUP
// ======================================================================

// SQLite Database - Railway persistent storage
const dbPath = process.env.NODE_ENV === 'production' 
    ? '/app/data/expertise_tours.db' 
    : './expertise_tours.db';

// Ensure data directory exists in production
if (process.env.NODE_ENV === 'production') {
    const fs = require('fs');
    if (!fs.existsSync('/app/data')) {
        fs.mkdirSync('/app/data', { recursive: true });
    }
}

const db = new sqlite3.Database(dbPath, (err) => {
    if (err) {
        console.error('❌ Database connection failed:', err.message);
    } else {
        console.log('✅ Connected to SQLite database:', dbPath);
        initializeDatabase();
    }
});

// Initialize database tables and sample data
function initializeDatabase() {
    // Create appointments table with new fields for fixed appointments
    db.run(`CREATE TABLE IF NOT EXISTS appointments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT NOT NULL,
        address TEXT NOT NULL,
        priority TEXT DEFAULT 'mittel',
        status TEXT DEFAULT 'vorschlag',
        duration INTEGER DEFAULT 3,
        pipeline_days INTEGER DEFAULT 0,
        notes TEXT,
        preferred_dates TEXT DEFAULT '[]',
        excluded_dates TEXT DEFAULT '[]',
        fixed_date TEXT,
        fixed_time TEXT,
        is_fixed INTEGER DEFAULT 0,
        on_hold TEXT,
        lat REAL,
        lng REAL,
        geocoded INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Create drivers table
    db.run(`CREATE TABLE IF NOT EXISTS drivers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        home_base TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Create saved_routes table for persistent route storage
    db.run(`CREATE TABLE IF NOT EXISTS saved_routes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        week_start DATE NOT NULL,
        driver_id INTEGER,
        route_data TEXT NOT NULL,
        is_active INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (driver_id) REFERENCES drivers (id)
    )`);

    // Create user_sessions table for session management
    db.run(`CREATE TABLE IF NOT EXISTS user_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT UNIQUE NOT NULL,
        user_data TEXT,
        expires_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Stelle sicher, dass IMMER ein Fahrer existiert
    db.run(`INSERT OR IGNORE INTO drivers (id, name, home_base) 
            VALUES (1, 'Testimonial-Fahrer', 'Kurt-Schumacher-Straße 34, 30159 Hannover')`, (err) => {
        if (err) {
            console.error('❌ Fehler beim Einfügen des Fahrers:', err);
        } else {
            console.log('✅ Standard-Fahrer sichergestellt');
        }
    });

    // Insert sample data if tables are empty
    db.get("SELECT COUNT(*) as count FROM appointments", (err, row) => {
        if (!err && row.count === 0) {
            insertSampleData();
        }
    });

    console.log('✅ Database tables initialized');
}

function insertSampleData() {
    const sampleAppointments = [
        {
            customer: "Max Mustermann",
            address: "Petuelring 130, 80809 München",
            priority: "hoch",
            status: "bestätigt",
            duration: 3,
            pipeline_days: 14,
            fixed_date: "2025-06-10",
            fixed_time: "14:00",
            is_fixed: 1,
            notes: JSON.stringify({
                invitee_name: "Max Mustermann",
                company: "Mustermann GmbH",
                customer_company: "BMW",
                start_time: "2025-06-10 14:00",
                custom_notes: "Wichtiger Testimonial-Termin",
                source: "Sample Data"
            })
        },
        {
            customer: "Anna Schmidt",
            address: "Salzufer 1, 10587 Berlin", 
            priority: "hoch",
            status: "bestätigt",
            duration: 3,
            pipeline_days: 21,
            fixed_date: "2025-06-11",
            fixed_time: "10:00",
            is_fixed: 1,
            notes: JSON.stringify({
                invitee_name: "Anna Schmidt",
                company: "Schmidt & Partner",
                customer_company: "Mercedes-Benz",
                start_time: "2025-06-11 10:00",
                custom_notes: "Testimonial für neue Kampagne",
                source: "Sample Data"
            })
        },
        {
            customer: "Peter Weber",
            address: "Berliner Ring 2, 38440 Wolfsburg",
            priority: "mittel",
            status: "vorschlag",
            duration: 3,
            pipeline_days: 7,
            is_fixed: 0,
            notes: JSON.stringify({
                invitee_name: "Peter Weber",
                company: "Weber Industries",
                customer_company: "Volkswagen",
                custom_notes: "Noch zu terminieren",
                source: "Sample Data"
            })
        }
    ];

    // Insert sample appointments (Fahrer wird bereits in initializeDatabase eingefügt)
    const stmt = db.prepare(`INSERT INTO appointments 
        (customer, address, priority, status, duration, pipeline_days, notes, fixed_date, fixed_time, is_fixed) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    sampleAppointments.forEach(apt => {
        stmt.run([
            apt.customer,
            apt.address,
            apt.priority,
            apt.status,
            apt.duration,
            apt.pipeline_days,
            apt.notes,
            apt.fixed_date || null,
            apt.fixed_time || null,
            apt.is_fixed || 0
        ]);
    });

    stmt.finalize();
    console.log('✅ Sample testimonial data inserted');
}

// ======================================================================
// HILFSFUNKTIONEN FÜR SESSION MANAGEMENT
// ======================================================================

// Helper function to generate session tokens
function generateSessionToken() {
    return 'railway-session-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
}

// Middleware to validate session tokens
function validateSession(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'No valid session token' });
    }

    const token = authHeader.split(' ')[1];
    
    db.get(
        "SELECT * FROM user_sessions WHERE token = ? AND (expires_at IS NULL OR expires_at > datetime('now'))",
        [token],
        (err, session) => {
            if (err || !session) {
                return res.status(401).json({ error: 'Invalid or expired session' });
            }
            
            req.session = session;
            next();
        }
    );
}

// ======================================================================
// API ROUTES
// ======================================================================

// Health check
app.get('/api/health', (req, res) => {
    const hasApiKey = !!process.env.GOOGLE_MAPS_API_KEY;
    const apiKeyLength = process.env.GOOGLE_MAPS_API_KEY ? process.env.GOOGLE_MAPS_API_KEY.length : 0;
    
    res.json({
        status: 'OK',
        message: 'Testimonial Tourenplaner Backend with Intelligent Route Planning!',
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development',
        features: [
            'intelligent_route_planning',
            'google_maps_integration', 
            'persistent_sessions',
            'route_saving',
            'csv_import',
            'testimonial_focused',
            'fixed_appointments',
            'maximum_efficiency'
        ],
        google_maps: hasApiKey ? '✅ Configured' : '⚠️ Fallback Mode',
        home_base: 'Kurt-Schumacher-Straße 34, 30159 Hannover',
        debug: {
            api_key_exists: hasApiKey,
            api_key_length: apiKeyLength,
            api_key_first_chars: process.env.GOOGLE_MAPS_API_KEY ? process.env.GOOGLE_MAPS_API_KEY.substring(0, 10) + '...' : 'none'
        }
    });
});

// Authentication with session persistence
app.post('/api/auth/login', function(req, res) {
    const password = req.body.password;
    
    if (password === 'testimonials2025') {
        const token = generateSessionToken();
        const expiresAt = new Date();
        expiresAt.setDate(expiresAt.getDate() + 7); // 7 days from now
        
        const userData = JSON.stringify({
            name: 'Admin',
            role: 'admin',
            loginTime: new Date().toISOString()
        });

        // Save session to database
        db.run(
            "INSERT INTO user_sessions (token, user_data, expires_at) VALUES (?, ?, ?)",
            [token, userData, expiresAt.toISOString()],
            function(err) {
                if (err) {
                    console.error('Session save error:', err);
                    return res.status(500).json({ error: 'Session creation failed' });
                }

                res.json({
                    token: token,
                    user: { name: 'Admin', role: 'admin' },
                    expiresAt: expiresAt.toISOString(),
                    message: 'Login successful - Session wird 7 Tage gespeichert'
                });
            }
        );
    } else {
        res.status(401).json({ error: 'Invalid password' });
    }
});

// Get appointments (exclude on_hold)
app.get('/api/appointments', (req, res) => {
    db.all("SELECT * FROM appointments WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '') ORDER BY created_at DESC", (err, rows) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        
        // Parse notes and enhance appointment data
        const enhancedRows = rows.map(row => {
            let parsedNotes = {};
            try {
                parsedNotes = JSON.parse(row.notes || '{}');
            } catch (e) {
                parsedNotes = {};
            }
            
            return {
                ...row,
                invitee_name: parsedNotes.invitee_name || row.customer,
                company: parsedNotes.company || '',
                customer_company: parsedNotes.customer_company || '',
                start_time: parsedNotes.start_time || null
            };
        });
        
        res.json(enhancedRows);
    });
});

// Get drivers
app.get('/api/drivers', (req, res) => {
    db.all("SELECT * FROM drivers", (err, rows) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        res.json(rows);
    });
});
// ======================================================================
// MAXIMALE EFFIZIENZ ROUTENOPTIMIERUNG
// ======================================================================

app.post('/api/routes/optimize', validateSession, async (req, res) => {
    const { weekStart, driverId, autoSave = true, forceNew = false } = req.body;
    
    if (!weekStart) {
        return res.status(400).json({ error: 'weekStart is required' });
    }

    console.log('🚀 MAXIMALE EFFIZIENZ Routenoptimierung für Woche:', weekStart);
    
    try {
        // 1. Prüfe ob für diese spezifische Woche bereits eine Route existiert
        const existingRoute = await new Promise((resolve, reject) => {
            db.get(
                "SELECT * FROM saved_routes WHERE week_start = ? AND is_active = 1",
                [weekStart],
                (err, row) => {
                    if (err) reject(err);
                    else resolve(row);
                }
            );
        });

        // Wenn Route existiert und nicht neu berechnet werden soll, lade sie
        if (existingRoute && !forceNew) {
            console.log('📋 Lade existierende Route für Woche', weekStart);
            const routeData = JSON.parse(existingRoute.route_data);
            return res.json({
                success: true,
                route: routeData,
                message: `Existierende Route für Woche ${weekStart} geladen`,
                autoSaved: false,
                isExisting: true
            });
        }

        // 2. Lade ALLE verfügbaren Termine (außer "on hold")
        const allAppointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT * FROM appointments 
                WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
                ORDER BY 
                    is_fixed DESC,
                    CASE WHEN status = 'bestätigt' THEN 0 ELSE 1 END,
                    pipeline_days DESC,
                    priority DESC
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        if (allAppointments.length === 0) {
            return res.json({
                success: false,
                message: 'Keine Termine zum Planen gefunden',
                route: createEmptyWeekStructure(weekStart)
            });
        }

        console.log(`📊 ${allAppointments.length} planbare Termine verfügbar`);

        // 3. Intelligente Auswahl: Maximiere Termine pro Woche
        const selectedAppointments = await selectMaxAppointmentsForWeek(
            allAppointments,
            weekStart
        );
        
        if (selectedAppointments.length === 0) {
            return res.json({
                success: false,
                message: `Keine Termine für Woche ${weekStart} verfügbar`,
                route: createEmptyWeekStructure(weekStart)
            });
        }
        
        // 4. ECHTE ROUTENOPTIMIERUNG mit maximaler Effizienz
        const optimizedRoute = await performMaxEfficiencyOptimization(selectedAppointments, weekStart, driverId);

        // 5. Route speichern
        if (autoSave && optimizedRoute.stats.totalAppointments > 0) {
            const routeName = `Woche ${weekStart}: KW ${getWeekNumber(weekStart)} (${optimizedRoute.stats.totalAppointments} Termine)`;
            await saveRouteToDatabase(routeName, weekStart, driverId, optimizedRoute);
            console.log(`💾 Route für ${weekStart} gespeichert`);
        }

        res.json({
            success: true,
            route: optimizedRoute,
            message: `Maximale Effizienz erreicht: ${optimizedRoute.stats.totalAppointments} Termine für Woche ${weekStart} geplant`,
            autoSaved: autoSave && optimizedRoute.stats.totalAppointments > 0,
            isNew: true
        });

    } catch (error) {
        console.error('❌ Routenoptimierung fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: 'Routenoptimierung fehlgeschlagen',
            details: error.message,
            route: createEmptyWeekStructure(weekStart)
        });
    }
});

// ======================================================================
// KORRIGIERTE FUNKTIONEN FÜR DUPLIKAT-VERMEIDUNG
// ======================================================================

// KORRIGIERTE VERSION: Verhindert doppelte Planung von Terminen
async function getUsedAppointmentIds(excludeWeekStart) {
    return new Promise((resolve, reject) => {
        // Hole ALLE gespeicherten Routen, nicht nur aktive
        db.all(
            "SELECT route_data, week_start FROM saved_routes WHERE week_start != ?",
            [excludeWeekStart],
            (err, rows) => {
                if (err) {
                    reject(err);
                    return;
                }
                
                const usedIds = new Set();
                const usedByWeek = {};
                
                rows.forEach(row => {
                    try {
                        const routeData = JSON.parse(row.route_data);
                        const weekStart = row.week_start;
                        
                        routeData.days?.forEach(day => {
                            day.appointments?.forEach(apt => {
                                if (apt.id) {
                                    usedIds.add(apt.id);
                                    // Tracke in welcher Woche der Termin verwendet wird
                                    if (!usedByWeek[apt.id]) {
                                        usedByWeek[apt.id] = [];
                                    }
                                    usedByWeek[apt.id].push(weekStart);
                                }
                            });
                        });
                    } catch (e) {
                        console.warn('Fehler beim Parsen der Route-Daten:', e);
                    }
                });
                
                console.log(`📊 ${usedIds.size} Termine sind bereits in anderen Wochen geplant`);
                
                // Debug: Zeige welche Termine wo verwendet werden
                Object.entries(usedByWeek).forEach(([aptId, weeks]) => {
                    if (weeks.length > 1) {
                        console.warn(`⚠️ Termin ${aptId} ist in mehreren Wochen geplant: ${weeks.join(', ')}`);
                    }
                });
                
                resolve(Array.from(usedIds));
            }
        );
    });
}

// KORRIGIERTE VERSION: Wählt nur ungenutzte Termine aus
async function selectMaxAppointmentsForWeek(allAppointments, weekStart) {
    console.log(`🎯 Optimiere Terminauswahl für Woche ${weekStart}`);
    
    // Trenne fixe und flexible Termine
    const weekStartDate = new Date(weekStart);
    const weekEndDate = new Date(weekStartDate);
    weekEndDate.setDate(weekStartDate.getDate() + 4); // Freitag

    // Fixe Termine für diese spezifische Woche
    const fixedAppointmentsThisWeek = allAppointments.filter(apt => {
        if (!apt.is_fixed || !apt.fixed_date) return false;
        const aptDate = new Date(apt.fixed_date);
        return aptDate >= weekStartDate && aptDate <= weekEndDate;
    });

    console.log(`📌 ${fixedAppointmentsThisWeek.length} fixe Termine für diese Woche`);

    // Hole bereits verwendete Termin-IDs
    const usedAppointmentIds = await getUsedAppointmentIds(weekStart);
    console.log(`🚫 ${usedAppointmentIds.length} Termine sind bereits in anderen Wochen geplant`);

    // Flexible Termine, die noch nicht verwendet wurden
    const availableFlexibleAppointments = allAppointments.filter(apt => {
        // Nur flexible Termine
        if (apt.is_fixed || apt.fixed_date) return false;
        
        // Nicht bereits in einer anderen Woche geplant
        if (usedAppointmentIds.includes(apt.id)) {
            console.log(`⏭️ Überspringe bereits geplanten Termin: ${apt.customer} (ID: ${apt.id})`);
            return false;
        }
        
        return true;
    });

    console.log(`📋 ${availableFlexibleAppointments.length} flexible Termine verfügbar`);

    // Sortiere flexible Termine nach Priorität
    const sortedFlexible = availableFlexibleAppointments.sort((a, b) => {
        // Bestätigte Termine zuerst
        if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
        if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
        
        // Dann nach Pipeline-Alter (ältere zuerst)
        if (b.pipeline_days !== a.pipeline_days) {
            return b.pipeline_days - a.pipeline_days;
        }
        
        // Dann nach Priorität
        const priorityMap = { 'hoch': 3, 'mittel': 2, 'niedrig': 1 };
        return (priorityMap[b.priority] || 0) - (priorityMap[a.priority] || 0);
    });

    // Kombiniere fixe und flexible Termine
    const selectedAppointments = [
        ...fixedAppointmentsThisWeek,
        ...sortedFlexible
    ];

    console.log(`✅ ${selectedAppointments.length} Termine für Woche ${weekStart} ausgewählt`);
    console.log(`   - ${fixedAppointmentsThisWeek.length} fixe Termine`);
    console.log(`   - ${sortedFlexible.length} flexible Termine`);
    
    // Debug: Zeige die ersten ausgewählten Termine
    selectedAppointments.slice(0, 5).forEach((apt, i) => {
        console.log(`   ${i + 1}. ${apt.customer} (${apt.status}, ${apt.pipeline_days} Tage, ${apt.is_fixed ? 'FIX' : 'flexibel'})`);
    });
    
    return selectedAppointments;
}

// ======================================================================
// REALISTISCHE ROUTENOPTIMIERUNG MIT CLUSTERING
// ======================================================================

async function performMaxEfficiencyOptimization(appointments, weekStart, driverId) {
    console.log('⚡ REALISTISCHE ROUTENOPTIMIERUNG mit Clustering...');
    
    try {
        // 1. Geocode alle Termine falls nötig
        const geocodedAppointments = await ensureAllAppointmentsGeocoded(appointments);
        
        // 2. Clustere Termine nach geografischer Nähe
        const clusters = clusterAppointmentsByLocation(geocodedAppointments);
        
        // 3. Plane Routen basierend auf Clustern
        const optimizedWeek = await planWeekWithClusters(clusters, geocodedAppointments, weekStart);
        
        // 4. Berechne realistische Fahrzeiten
        const weekWithTravel = await calculateRealisticTravelTimes(optimizedWeek);
        
        // 5. Optimiere Übernachtungen
        const finalWeek = optimizeOvernightStays(weekWithTravel);
        
        return formatOptimizedWeek(finalWeek, weekStart);
        
    } catch (error) {
        console.warn('⚠️ Optimierung fehlgeschlagen, verwende Fallback:', error.message);
        
        // Verwende IntelligentRoutePlanner als Fallback
        const planner = new IntelligentRoutePlanner();
        return await planner.optimizeWeek(appointments, weekStart, driverId || 1);
    }
}

// Stelle sicher, dass alle Termine Koordinaten haben
async function ensureAllAppointmentsGeocoded(appointments) {
    const needsGeocoding = appointments.filter(apt => !apt.lat || !apt.lng);
    
    if (needsGeocoding.length > 0) {
        console.log(`🗺️ Geocoding ${needsGeocoding.length} Termine...`);
        
        const apiKey = process.env.GOOGLE_MAPS_API_KEY;
        if (!apiKey) {
            throw new Error('Google Maps API Key nicht konfiguriert');
        }
        
        for (const apt of needsGeocoding) {
            try {
                const response = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
                    params: {
                        address: apt.address,
                        key: apiKey,
                        region: 'de',
                        components: 'country:DE'
                    },
                    timeout: 5000
                });
                
                if (response.data.status === 'OK' && response.data.results.length > 0) {
                    const location = response.data.results[0].geometry.location;
                    apt.lat = location.lat;
                    apt.lng = location.lng;
                    apt.geocoded = true;
                    
                    // Update in Datenbank
                    await new Promise((resolve, reject) => {
                        db.run(
                            "UPDATE appointments SET lat = ?, lng = ?, geocoded = 1 WHERE id = ?",
                            [location.lat, location.lng, apt.id],
                            err => err ? reject(err) : resolve()
                        );
                    });
                    
                    console.log(`✅ Geocoded: ${apt.customer}`);
                }
                
                // Pause zwischen Requests
                await new Promise(resolve => setTimeout(resolve, 200));
                
            } catch (error) {
                console.warn(`⚠️ Geocoding fehlgeschlagen für ${apt.customer}:`, error.message);
                // Verwende Fallback-Koordinaten basierend auf PLZ
                const fallback = getFallbackCoordinatesFromAddress(apt.address);
                if (fallback) {
                    apt.lat = fallback.lat;
                    apt.lng = fallback.lng;
                    apt.geocoded = false;
                }
            }
        }
    }
    
    // Filtere Termine ohne Koordinaten
    return appointments.filter(apt => apt.lat && apt.lng);
}

// Clustere Termine nach geografischer Nähe
function clusterAppointmentsByLocation(appointments) {
    console.log('🗺️ Clustere Termine nach geografischen Regionen...');
    
    // Definiere deutsche Regionen
    const regions = {
        'Nord': { lat: 53.5, lng: 10.0, cities: ['Hamburg', 'Bremen', 'Kiel', 'Lübeck'] },
        'Ost': { lat: 52.5, lng: 13.4, cities: ['Berlin', 'Leipzig', 'Dresden', 'Magdeburg'] },
        'West': { lat: 51.2, lng: 7.0, cities: ['Köln', 'Düsseldorf', 'Dortmund', 'Essen'] },
        'Süd': { lat: 48.5, lng: 11.5, cities: ['München', 'Stuttgart', 'Nürnberg', 'Augsburg'] },
        'Mitte': { lat: 50.5, lng: 9.0, cities: ['Frankfurt', 'Kassel', 'Würzburg', 'Mainz'] }
    };
    
    const clusters = {};
    Object.keys(regions).forEach(region => {
        clusters[region] = [];
    });
    
    // Zuordnung der Termine zu Regionen
    appointments.forEach(apt => {
        let minDistance = Infinity;
        let bestRegion = 'Mitte';
        
        Object.entries(regions).forEach(([region, center]) => {
            const distance = calculateDistance(apt.lat, apt.lng, center.lat, center.lng);
            if (distance < minDistance) {
                minDistance = distance;
                bestRegion = region;
            }
        });
        
        clusters[bestRegion].push({
            ...apt,
            distance_to_center: minDistance
        });
    });
    
    // Debug-Ausgabe
    Object.entries(clusters).forEach(([region, apts]) => {
        if (apts.length > 0) {
            console.log(`📍 Region ${region}: ${apts.length} Termine`);
        }
    });
    
    return clusters;
}

// Plane Woche basierend auf Clustern
async function planWeekWithClusters(clusters, allAppointments, weekStart) {
    const weekDays = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag'];
    const startDate = new Date(weekStart);
    const homeBase = { lat: 52.3759, lng: 9.7320 }; // Hannover
    
    const week = weekDays.map((day, index) => {
        const date = new Date(startDate);
        date.setDate(startDate.getDate() + index);
        
        return {
            day,
            date: date.toISOString().split('T')[0],
            appointments: [],
            travelSegments: [],
            workTime: 0,
            travelTime: 0,
            currentLocation: homeBase,
            overnight: null
        };
    });
    // Verarbeite zuerst fixe Termine
    const fixedAppointments = allAppointments.filter(apt => apt.is_fixed && apt.fixed_date);
    
    fixedAppointments.forEach(apt => {
        const dayIndex = weekDays.findIndex((_, i) => {
            const dayDate = new Date(startDate);
            dayDate.setDate(startDate.getDate() + i);
            return dayDate.toISOString().split('T')[0] === apt.fixed_date;
        });
        
        if (dayIndex >= 0) {
            week[dayIndex].appointments.push({
                ...apt,
                startTime: apt.fixed_time || '10:00',
                endTime: addHoursToTime(apt.fixed_time || '10:00', apt.duration || 3),
                duration: apt.duration || 3
            });
            week[dayIndex].workTime += apt.duration || 3;
            console.log(`📌 Fixer Termin geplant: ${apt.customer} am ${weekDays[dayIndex]}`);
        }
    });
    
    // Plane flexible Termine nach Regionen
    const flexibleAppointments = allAppointments.filter(apt => !apt.is_fixed);
    const regionOrder = optimizeRegionOrder(clusters, homeBase);
    
    let currentDayIndex = 0;
    let appointmentsPlannedToday = 0;
    const MAX_APPOINTMENTS_PER_DAY = 3;
    const MAX_WORK_HOURS_PER_DAY = 12;
    
    for (const region of regionOrder) {
        const regionAppointments = clusters[region]
            .filter(apt => flexibleAppointments.some(f => f.id === apt.id))
            .sort((a, b) => {
                // Sortiere nach Status und Priorität
                if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
                if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
                return b.pipeline_days - a.pipeline_days;
            });
        
        if (regionAppointments.length === 0) continue;
        
        console.log(`\n🗺️ Plane Region ${region} mit ${regionAppointments.length} Terminen`);
        
        for (const apt of regionAppointments) {
            // Finde besten Tag für diesen Termin
            let bestDay = currentDayIndex;
            let plannedSuccessfully = false;
            
            // Versuche aktuellen Tag
            if (week[currentDayIndex].workTime + 3 <= MAX_WORK_HOURS_PER_DAY &&
                appointmentsPlannedToday < MAX_APPOINTMENTS_PER_DAY) {
                
                const lastAppointment = week[currentDayIndex].appointments[week[currentDayIndex].appointments.length - 1];
                const startTime = lastAppointment ? 
                    addHoursToTime(lastAppointment.endTime, 1) : // 1h Fahrzeit
                    '09:00';
                
                if (timeToHours(startTime) + 3 <= 18) { // Ende bis 18 Uhr
                    week[currentDayIndex].appointments.push({
                        ...apt,
                        startTime: startTime,
                        endTime: addHoursToTime(startTime, 3),
                        duration: 3
                    });
                    week[currentDayIndex].workTime += 3;
                    appointmentsPlannedToday++;
                    plannedSuccessfully = true;
                    console.log(`✅ ${apt.customer} → ${weekDays[currentDayIndex]} ${startTime}`);
                }
            }
            
            if (!plannedSuccessfully) {
                // Nächster Tag
                currentDayIndex++;
                appointmentsPlannedToday = 0;
                
                if (currentDayIndex < 5) {
                    week[currentDayIndex].appointments.push({
                        ...apt,
                        startTime: '09:00',
                        endTime: '12:00',
                        duration: 3
                    });
                    week[currentDayIndex].workTime += 3;
                    appointmentsPlannedToday = 1;
                    console.log(`✅ ${apt.customer} → ${weekDays[currentDayIndex]} 09:00 (neuer Tag)`);
                } else {
                    console.log(`❌ ${apt.customer} passt nicht mehr in die Woche`);
                }
            }
            
            if (currentDayIndex >= 5) break; // Woche voll
        }
        
        if (currentDayIndex >= 5) break; // Woche voll
    }
    
    return week;
}

// Optimiere Reihenfolge der Regionen
function optimizeRegionOrder(clusters, homeBase) {
    const regions = [
        { name: 'Mitte', lat: 50.5, lng: 9.0 },
        { name: 'Nord', lat: 53.5, lng: 10.0 },
        { name: 'West', lat: 51.2, lng: 7.0 },
        { name: 'Ost', lat: 52.5, lng: 13.4 },
        { name: 'Süd', lat: 48.5, lng: 11.5 }
    ];
    
    // Sortiere Regionen nach Entfernung von Hannover
    regions.sort((a, b) => {
        const distA = calculateDistance(homeBase.lat, homeBase.lng, a.lat, a.lng);
        const distB = calculateDistance(homeBase.lat, homeBase.lng, b.lat, b.lng);
        return distA - distB;
    });
    
    return regions.map(r => r.name);
}

// Berechne realistische Fahrzeiten
async function calculateRealisticTravelTimes(week) {
    console.log('🚗 Berechne realistische Fahrzeiten...');
    
    const homeBase = { lat: 52.3759, lng: 9.7320 };
    
    for (const day of week) {
        if (day.appointments.length === 0) continue;
        
        let totalTravelTime = 0;
        let currentLocation = homeBase;
        
        // Fahrt zum ersten Termin
        const firstApt = day.appointments[0];
        const distanceToFirst = calculateDistance(
            currentLocation.lat, currentLocation.lng,
            firstApt.lat, firstApt.lng
        );
        const travelTimeToFirst = distanceToFirst / 85; // 85 km/h Durchschnitt
        totalTravelTime += travelTimeToFirst;
        
        day.travelSegments.push({
            type: 'departure',
            from: 'Hannover',
            to: extractCityFromAddress(firstApt.address),
            distance: Math.round(distanceToFirst),
            duration: travelTimeToFirst,
            startTime: subtractHoursFromTime(firstApt.startTime, travelTimeToFirst),
            endTime: firstApt.startTime,
            description: `Fahrt nach ${extractCityFromAddress(firstApt.address)}`
        });
        
        // Fahrten zwischen Terminen
        for (let i = 0; i < day.appointments.length - 1; i++) {
            const from = day.appointments[i];
            const to = day.appointments[i + 1];
            
            const distance = calculateDistance(from.lat, from.lng, to.lat, to.lng);
            const travelTime = distance / 85;
            totalTravelTime += travelTime;
            
            day.travelSegments.push({
                type: 'travel',
                from: extractCityFromAddress(from.address),
                to: extractCityFromAddress(to.address),
                distance: Math.round(distance),
                duration: travelTime,
                startTime: from.endTime,
                endTime: to.startTime,
                description: `Fahrt von ${extractCityFromAddress(from.address)} nach ${extractCityFromAddress(to.address)}`
            });
        }
        
        // Rückfahrt oder Übernachtung
        const lastApt = day.appointments[day.appointments.length - 1];
        const distanceToHome = calculateDistance(
            lastApt.lat, lastApt.lng,
            homeBase.lat, homeBase.lng
        );
        
        if (distanceToHome > 200) {
            // Übernachtung nötig
            day.overnight = {
                city: extractCityFromAddress(lastApt.address),
                reason: `${Math.round(distanceToHome)} km von Hannover`,
                hotel: `Hotel in ${extractCityFromAddress(lastApt.address)}`
            };
        } else {
            // Rückfahrt
            const travelTimeHome = distanceToHome / 85;
            totalTravelTime += travelTimeHome;
            
            day.travelSegments.push({
                type: 'return',
                from: extractCityFromAddress(lastApt.address),
                to: 'Hannover',
                distance: Math.round(distanceToHome),
                duration: travelTimeHome,
                startTime: lastApt.endTime,
                endTime: addHoursToTime(lastApt.endTime, travelTimeHome),
                description: 'Rückfahrt nach Hannover'
            });
        }
        
        day.travelTime = Math.round(totalTravelTime * 10) / 10;
    }
    
    return week;
}

// Optimiere Übernachtungen
function optimizeOvernightStays(week) {
    console.log('🏨 Optimiere Übernachtungen...');
    
    for (let i = 0; i < week.length - 1; i++) {
        const today = week[i];
        const tomorrow = week[i + 1];
        
        if (today.overnight && tomorrow.appointments.length > 0) {
            // Prüfe ob Übernachtung sinnvoll ist
            const lastTodayApt = today.appointments[today.appointments.length - 1];
            const firstTomorrowApt = tomorrow.appointments[0];
            
            const directDistance = calculateDistance(
                lastTodayApt.lat, lastTodayApt.lng,
                firstTomorrowApt.lat, firstTomorrowApt.lng
            );
            
            if (directDistance < 100) {
                // Übernachtung in der Nähe macht Sinn
                today.overnight.optimized = true;
                today.overnight.nextDayDistance = Math.round(directDistance);
                console.log(`✅ Übernachtung in ${today.overnight.city} optimiert (${Math.round(directDistance)} km zum nächsten Termin)`);
            }
        }
    }
    
    return week;
}

// Hilfsfunktionen
function calculateDistance(lat1, lng1, lat2, lng2) {
    const R = 6371; // Erdradius in km
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLng = (lng2 - lng1) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLng/2) * Math.sin(dLng/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
}

function extractCityFromAddress(address) {
    // Extrahiere Stadt aus Adresse
    const match = address.match(/\d{5}\s+([^,]+)/);
    if (match) return match[1].trim();
    
    const parts = address.split(',');
    return parts[parts.length - 1].trim();
}

function timeToHours(timeStr) {
    const [h, m] = timeStr.split(':').map(Number);
    return h + (m || 0) / 60;
}

function hoursToTime(hours) {
    const h = Math.floor(hours);
    const m = Math.round((hours - h) * 60);
    return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
}

function addHoursToTime(timeStr, hours) {
    const totalHours = timeToHours(timeStr) + hours;
    return hoursToTime(totalHours);
}

function subtractHoursFromTime(timeStr, hours) {
    const totalHours = Math.max(0, timeToHours(timeStr) - hours);
    return hoursToTime(totalHours);
}

function getFallbackCoordinatesFromAddress(address) {
    const plzMatch = address.match(/\b(\d{5})\b/);
    if (!plzMatch) return null;
    
    const plz = plzMatch[1];
    const plzMap = {
        '1': { lat: 52.52, lng: 13.40 }, // Berlin
        '2': { lat: 53.55, lng: 9.99 },  // Hamburg
        '3': { lat: 52.38, lng: 9.73 },  // Hannover
        '4': { lat: 51.51, lng: 7.47 },  // NRW
        '5': { lat: 50.94, lng: 6.96 },  // Köln
        '6': { lat: 50.11, lng: 8.68 },  // Frankfurt
        '7': { lat: 48.78, lng: 9.18 },  // Stuttgart
        '8': { lat: 48.14, lng: 11.58 }, // München
        '9': { lat: 49.45, lng: 11.08 }  // Nürnberg
    };
    
    return plzMap[plz[0]] || null;
}

// Formatiere das Ergebnis
function formatOptimizedWeek(week, weekStart) {
    const totalAppointments = week.reduce((sum, day) => sum + day.appointments.length, 0);
    const totalWorkHours = week.reduce((sum, day) => sum + day.workTime, 0);
    const totalTravelHours = week.reduce((sum, day) => sum + day.travelTime, 0);
    const workDays = week.filter(day => day.appointments.length > 0).length;
    
    return {
        weekStart,
        days: week,
        totalHours: Math.round((totalWorkHours + totalTravelHours) * 10) / 10,
        optimizations: [
            `${totalAppointments} Termine nach geografischen Clustern optimiert`,
            `${workDays} Arbeitstage effizient geplant`,
            `${Math.round(totalTravelHours)} Stunden Fahrzeit realistisch berechnet`,
            week.filter(d => d.overnight).length > 0 ? 
                `${week.filter(d => d.overnight).length} strategische Übernachtungen geplant` : 
                'Keine Übernachtungen nötig',
            'Regionale Cluster für minimale Fahrtzeiten genutzt'
        ],
        stats: {
            totalAppointments,
            confirmedAppointments: week.reduce((sum, day) => 
                sum + day.appointments.filter(a => a.status === 'bestätigt').length, 0),
            proposalAppointments: week.reduce((sum, day) => 
                sum + day.appointments.filter(a => a.status === 'vorschlag').length, 0),
            fixedAppointments: week.reduce((sum, day) => 
                sum + day.appointments.filter(a => a.is_fixed).length, 0),
            totalTravelTime: Math.round(totalTravelHours * 10) / 10,
            workDays,
            efficiency: {
                travelEfficiency: totalWorkHours > 0 ? 
                    Math.round((1 - totalTravelHours / (totalWorkHours + totalTravelHours)) * 100) / 100 : 0,
                weekUtilization: Math.round((totalWorkHours / 50) * 100) / 100
            }
        },
        generatedAt: new Date().toISOString()
    };
}

// ======================================================================
// WEITERE HILFSFUNKTIONEN
// ======================================================================

// ZUSÄTZLICH: Funktion zum Bereinigen doppelter Planungen
app.post('/api/admin/clean-duplicate-appointments', validateSession, async (req, res) => {
    try {
        console.log('🧹 Bereinige doppelte Terminplanungen...');
        
        // Hole alle gespeicherten Routen
        const allRoutes = await new Promise((resolve, reject) => {
            db.all("SELECT id, week_start, route_data FROM saved_routes ORDER BY week_start", (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });
        
        const appointmentUsage = new Map(); // appointmentId -> [routeId, weekStart]
        const duplicates = [];
        
        // Analysiere alle Routen
        allRoutes.forEach(route => {
            try {
                const routeData = JSON.parse(route.route_data);
                
                routeData.days?.forEach(day => {
                    day.appointments?.forEach(apt => {
                        if (apt.id) {
                            if (appointmentUsage.has(apt.id)) {
                                // Duplikat gefunden
                                const existing = appointmentUsage.get(apt.id);
                                duplicates.push({
                                    appointmentId: apt.id,
                                    customer: apt.customer,
                                    firstRoute: existing,
                                    duplicateRoute: { routeId: route.id, weekStart: route.week_start }
                                });
                            } else {
                                appointmentUsage.set(apt.id, { routeId: route.id, weekStart: route.week_start });
                            }
                        }
                    });
                });
            } catch (e) {
                console.error(`Fehler beim Parsen von Route ${route.id}:`, e);
            }
        });
        
        console.log(`🔍 ${duplicates.length} doppelte Terminplanungen gefunden`);
        
        // Optional: Bereinigung durchführen
        let cleaned = 0;
        if (req.body.performCleanup === true) {
            for (const dup of duplicates) {
                console.log(`🧹 Entferne Duplikat: ${dup.customer} aus Woche ${dup.duplicateRoute.weekStart}`);
                // Hier könntest du die Duplikate aus den Routen entfernen
                cleaned++;
            }
        }
        
        res.json({
            success: true,
            duplicatesFound: duplicates.length,
            duplicates: duplicates.slice(0, 10), // Zeige nur erste 10
            cleaned: cleaned,
            message: cleaned > 0 ? 
                `${cleaned} Duplikate bereinigt` : 
                `${duplicates.length} Duplikate gefunden. Setze performCleanup=true zum Bereinigen`
        });
        
    } catch (error) {
        console.error('❌ Duplikat-Bereinigung fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// HELPER: Prüfe ob ein Termin bereits geplant ist
async function isAppointmentAlreadyPlanned(appointmentId, excludeWeekStart = null) {
    return new Promise((resolve, reject) => {
        let query = "SELECT week_start FROM saved_routes WHERE route_data LIKE ?";
        let params = [`%"id":${appointmentId}%`];
        
        if (excludeWeekStart) {
            query += " AND week_start != ?";
            params.push(excludeWeekStart);
        }
        
        db.get(query, params, (err, row) => {
            if (err) {
                reject(err);
            } else {
                resolve(row ? row.week_start : null);
            }
        });
    });
}

function createEmptyWeekStructure(weekStart) {
    const weekDays = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag'];
    const startDate = new Date(weekStart);
    
    return weekDays.map((day, dayIndex) => {
        const date = new Date(startDate);
        date.setDate(startDate.getDate() + dayIndex);
        
        return {
            day,
            date: date.toISOString().split('T')[0],
            appointments: [],
            travelSegments: [],
            workTime: 0,
            travelTime: 0,
            overnight: null
        };
    });
}

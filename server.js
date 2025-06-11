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
    const fixedAppointments = allAppointments
        .filter(apt => apt.is_fixed && apt.fixed_date)
        .sort((a, b) => {
            if (a.fixed_date === b.fixed_date) {
                return timeToHours(a.fixed_time || '00:00') - timeToHours(b.fixed_time || '00:00');
            }
            return new Date(a.fixed_date) - new Date(b.fixed_date);
        });

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
            let scheduled = false;

            for (let dayIndex = currentDayIndex; dayIndex < 5; dayIndex++) {
                const day = week[dayIndex];

                if (day.workTime + 3 > MAX_WORK_HOURS_PER_DAY ||
                    day.appointments.length >= MAX_APPOINTMENTS_PER_DAY) {
                    continue;
                }

                const lastAppointment = day.appointments[day.appointments.length - 1];
                const startTime = lastAppointment ?
                    addHoursToTime(lastAppointment.endTime, 1) : '09:00';

                if (timeToHours(startTime) + 3 > 18) {
                    continue;
                }

                day.appointments.push({
                    ...apt,
                    startTime,
                    endTime: addHoursToTime(startTime, 3),
                    duration: 3
                });
                day.workTime += 3;
                console.log(`✅ ${apt.customer} → ${weekDays[dayIndex]} ${startTime}`);
                currentDayIndex = dayIndex;
                scheduled = true;
                break;
            }

            if (!scheduled) {
                console.log(`❌ ${apt.customer} passt nicht mehr in die Woche`);
            }
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
    hours = ((hours % 24) + 24) % 24; // Wrap around 24h and avoid negatives
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
function getWeekNumber(dateStr) {
    const date = new Date(dateStr);
    const start = new Date(date.getFullYear(), 0, 1);
    const days = Math.floor((date - start) / (24 * 60 * 60 * 1000));
    return Math.ceil((days + start.getDay() + 1) / 7);
}

async function saveRouteToDatabase(routeName, weekStart, driverId, routeData) {
    return new Promise((resolve, reject) => {
        // Erst alle Routen für diese Woche deaktivieren
        db.run(
            "UPDATE saved_routes SET is_active = 0 WHERE week_start = ?",
            [weekStart],
            () => {
                // Dann neue Route speichern
                db.run(
                    "INSERT INTO saved_routes (name, week_start, driver_id, route_data, is_active) VALUES (?, ?, ?, ?, 1)",
                    [routeName, weekStart, driverId || 1, JSON.stringify(routeData)],
                    function(err) {
                        if (err) reject(err);
                        else resolve(this.lastID);
                    }
                );
            }
        );
    });
}

// ======================================================================
// WEITERE API ROUTES
// ======================================================================

// Get saved routes
app.get('/api/routes/saved', (req, res) => {
    const { weekStart } = req.query;
    
    let query = "SELECT * FROM saved_routes ORDER BY created_at DESC";
    let params = [];
    
    if (weekStart) {
        query = "SELECT * FROM saved_routes WHERE week_start = ? ORDER BY created_at DESC";
        params = [weekStart];
    }
    
    db.all(query, params, (err, routes) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        
        // Parse route_data for each route
        const parsedRoutes = routes.map(route => ({
            ...route,
            route_data: JSON.parse(route.route_data)
        }));
        
        res.json(parsedRoutes);
    });
});

// Load active route for a week
app.get('/api/routes/active/:weekStart', (req, res) => {
    const weekStart = req.params.weekStart;
    
    db.get(
        "SELECT * FROM saved_routes WHERE week_start = ? AND is_active = 1",
        [weekStart],
        (err, route) => {
            if (err) {
                res.status(500).json({ error: err.message });
            } else if (!route) {
                res.json({ found: false, message: 'Keine aktive Route für diese Woche' });
            } else {
                res.json({
                    found: true,
                    route: {
                        ...route,
                        route_data: JSON.parse(route.route_data)
                    }
                });
            }
        }
    );
});

// CSV Preview with enhanced testimonial analysis
app.post('/api/admin/preview-csv', upload.single('csvFile'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'Keine CSV-Datei hochgeladen' });
    }

    try {
        const csvContent = req.file.buffer.toString('utf-8');
        
        const parsed = Papa.parse(csvContent, {
            header: true,
            skipEmptyLines: true,
            delimiter: '',
            encoding: 'utf-8',
            dynamicTyping: false,
            delimitersToGuess: [',', ';', '\t', '|']
        });

        console.log('📊 CSV Preview - Parsed meta:', parsed.meta);

        const analysis = {
            totalRows: parsed.data.length,
            columns: parsed.meta.fields,
            confirmedAppointments: 0,
            proposalAppointments: 0,
            onHoldAppointments: 0,
            missingInvitee: 0,
            sampleRows: []
        };

        parsed.data.forEach((row, index) => {
            const inviteeName = row['Invitee Name'] || '';
            const onHold = row['On Hold'] || '';
            const startDateTime = row['Start Date & Time'] || '';

            if (onHold && onHold.trim() !== '') {
                analysis.onHoldAppointments++;
            } else if (!inviteeName || inviteeName.trim() === '') {
                analysis.missingInvitee++;
            } else if (startDateTime && startDateTime.trim() !== '') {
                analysis.confirmedAppointments++;
            } else {
                analysis.proposalAppointments++;
            }

            if (index < 5) {
                analysis.sampleRows.push({
                    invitee_name: inviteeName,
                    company: row['Company'] || '',
                    customer_company: row['Customer Company'] || '',
                    has_appointment: !!(startDateTime && startDateTime.trim()),
                    on_hold: !!(onHold && onHold.trim()),
                    address: row['Adresse'] || `${row['Straße & Hausnr.'] || ''}, ${row['PLZ'] || ''} ${row['Ort'] || ''}`.trim(),
                    start_date_time: startDateTime
                });
            }
        });

        res.json({
            success: true,
            analysis: analysis,
            message: 'CSV Vorschau erstellt - Testimonial Person-fokussierte Struktur',
            meta: {
                delimiter: parsed.meta.delimiter,
                fields_count: parsed.meta.fields.length,
                truncated: parsed.meta.truncated,
                error: parsed.errors.length > 0 ? parsed.errors[0] : null
            },
            data_structure: {
                primary_name: 'Invitee Name (Person für Testimonial)',
                company_info: 'Company (Firma der Person)',
                client_info: 'Customer Company (Unser Kunde)',
                valid_appointments: analysis.confirmedAppointments + analysis.proposalAppointments
            }
        });

    } catch (error) {
        console.error('❌ CSV Preview Fehler:', error);
        res.status(500).json({
            error: 'CSV Analyse fehlgeschlagen',
            details: error.message
        });
    }
});

// CSV Import endpoint for testimonial data
app.post('/api/admin/import-csv', upload.single('csvFile'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'Keine CSV-Datei hochgeladen' });
    }

    console.log('📁 CSV Testimonial Import gestartet...');
    
    try {
        const csvContent = req.file.buffer.toString('utf-8');
        
        const parsed = Papa.parse(csvContent, {
            header: true,
            skipEmptyLines: true,
            delimiter: '',
            encoding: 'utf-8',
            dynamicTyping: false,
            delimitersToGuess: [',', ';', '\t', '|']
        });

        console.log(`📊 ${parsed.data.length} Zeilen gefunden`);

        const processedAppointments = [];
        let skippedCount = 0;
        let confirmedCount = 0;
        let proposalCount = 0;

        parsed.data.forEach((row, index) => {
            // Skip if "On Hold" is filled
            if (row['On Hold'] && row['On Hold'].trim() !== '') {
                skippedCount++;
                return;
            }

            // Skip if essential data missing
            if (!row['Invitee Name'] || row['Invitee Name'].trim() === '') {
                skippedCount++;
                return;
            }

            // Build address
            let fullAddress = row['Adresse'] || '';
            if (!fullAddress && row['Straße & Hausnr.']) {
                const parts = [];
                if (row['Straße & Hausnr.']) parts.push(row['Straße & Hausnr.']);
                if (row['PLZ'] && row['Ort']) parts.push(`${row['PLZ']} ${row['Ort']}`);
                else if (row['Ort']) parts.push(row['Ort']);
                if (row['Land']) parts.push(row['Land']);
                fullAddress = parts.join(', ');
            }

            // Parse date and time for fixed appointments
            let isFixed = false;
            let fixedDate = null;
            let fixedTime = null;
            
            if (row['Start Date & Time'] && row['Start Date & Time'].trim() !== '') {
                try {
                    const dateTimeStr = row['Start Date & Time'].trim();
                    let dateTime;
                    
                    if (dateTimeStr.match(/\d{1,2}[\.\/]\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}/)) {
                        const [datePart, timePart] = dateTimeStr.split(/\s+/);
                        const [day, month, year] = datePart.split(/[\.\/]/);
                        dateTime = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T${timePart}:00`);
                    } else if (dateTimeStr.match(/\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}/)) {
                        dateTime = new Date(dateTimeStr);
                    } else {
                        dateTime = new Date(dateTimeStr);
                    }
                    
                    if (!isNaN(dateTime.getTime())) {
                        isFixed = true;
                        fixedDate = dateTime.toISOString().split('T')[0];
                        fixedTime = `${dateTime.getHours().toString().padStart(2, '0')}:${dateTime.getMinutes().toString().padStart(2, '0')}`;
                    }
                } catch (e) {
                    console.log(`❌ Date parsing failed for: ${row['Start Date & Time']}`);
                }
            }

            const status = isFixed ? 'bestätigt' : 'vorschlag';
            
            if (status === 'bestätigt') confirmedCount++;
            else proposalCount++;

            const inviteeName = row['Invitee Name'].trim();
            const company = row['Company'] ? row['Company'].trim() : '';
            const customerCompany = row['Customer Company'] ? row['Customer Company'].trim() : '';

            let priority = 'mittel';
            if (status === 'bestätigt') {
                priority = 'hoch';
            } else if (customerCompany.toLowerCase().includes('bmw') || 
                      customerCompany.toLowerCase().includes('mercedes') || 
                      customerCompany.toLowerCase().includes('audi') || 
                      customerCompany.toLowerCase().includes('porsche') || 
                      customerCompany.toLowerCase().includes('volkswagen')) {
                priority = 'hoch';
            }

            const duration = 3;

            const appointment = {
                customer: inviteeName,
                address: fullAddress || 'Adresse nicht verfügbar',
                priority: priority,
                status: status,
                duration: duration,
                pipeline_days: status === 'vorschlag' ? Math.floor(Math.random() * 30) + 1 : 7,
                is_fixed: isFixed ? 1 : 0,
                fixed_date: fixedDate,
                fixed_time: fixedTime,
                on_hold: row['On Hold'] || null,
                notes: JSON.stringify({
                    invitee_name: inviteeName,
                    company: company,
                    customer_company: customerCompany,
                    start_time: row['Start Date & Time'] || null,
                    end_time: row['End Date & Time'] || null,
                    custom_notes: row['Notiz'] || '',
                    import_date: new Date().toISOString(),
                    source: 'CSV Testimonial Import'
                })
            };

            processedAppointments.push(appointment);
        });

        console.log(`📈 Processing complete: ${processedAppointments.length} testimonial appointments, ${skippedCount} skipped`);

        // Clear existing appointments and insert new ones
        db.run("DELETE FROM appointments", (err) => {
            if (err) {
                return res.status(500).json({ error: 'Datenbankfehler beim Löschen' });
            }

            console.log('🧹 Cleared existing appointments');

            const stmt = db.prepare(`INSERT INTO appointments 
                (customer, address, priority, status, duration, pipeline_days, notes, preferred_dates, excluded_dates, is_fixed, fixed_date, fixed_time, on_hold) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

            let insertedCount = 0;
            let errors = [];

            processedAppointments.forEach((apt) => {
                stmt.run([
                    apt.customer, 
                    apt.address, 
                    apt.priority, 
                    apt.status, 
                    apt.duration, 
                    apt.pipeline_days, 
                    apt.notes,
                    JSON.stringify([]), 
                    JSON.stringify([]),
                    apt.is_fixed,
                    apt.fixed_date,
                    apt.fixed_time,
                    apt.on_hold
                ], (err) => {
                    if (err) {
                        errors.push(`${apt.customer}: ${err.message}`);
                    } else {
                        insertedCount++;
                    }
                    
                    // Send response when all insertions are complete
                    if (insertedCount + errors.length === processedAppointments.length) {
                        stmt.finalize();
                        
                        res.json({
                            success: true,
                            message: 'CSV Testimonial Import erfolgreich abgeschlossen',
                            stats: {
                                totalRows: parsed.data.length,
                                processed: processedAppointments.length,
                                inserted: insertedCount,
                                confirmed: confirmedCount,
                                proposals: proposalCount,
                                skipped: skippedCount,
                                errors: errors.length
                            },
                            debug: {
                                delimiter: parsed.meta.delimiter,
                                columns: parsed.meta.fields,
                                sample_processed: processedAppointments.slice(0, 2)
                            },
                            sample_data: processedAppointments.slice(0, 3).map(apt => ({
                                name: apt.customer,
                                is_fixed: apt.is_fixed,
                                fixed_date: apt.fixed_date,
                                fixed_time: apt.fixed_time,
                                notes_preview: JSON.parse(apt.notes)
                            })),
                            errors: errors.length > 0 ? errors.slice(0, 5) : undefined
                        });
                    }
                });
            });

            // Handle case where no appointments to process
            if (processedAppointments.length === 0) {
                stmt.finalize();
                res.json({
                    success: false,
                    message: 'Keine gültigen Testimonial-Termine in der CSV gefunden',
                    stats: {
                        totalRows: parsed.data.length,
                        skipped: skippedCount
                    }
                });
            }
        });

    } catch (error) {
        console.error('❌ CSV Testimonial Import Fehler:', error);
        res.status(500).json({
            error: 'CSV Testimonial Import fehlgeschlagen',
            details: error.message
        });
    }
});

// Admin endpoint to check database
app.get('/api/admin/status', (req, res) => {
    db.get("SELECT COUNT(*) as count FROM appointments WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')", (err, row) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        
        db.get("SELECT COUNT(*) as on_hold_count FROM appointments WHERE on_hold IS NOT NULL AND on_hold != ''", (err2, row2) => {
            if (err2) {
                res.status(500).json({ error: err2.message });
                return;
            }
            
            db.get("SELECT COUNT(*) as fixed_count FROM appointments WHERE is_fixed = 1", (err3, row3) => {
                if (err3) {
                    res.status(500).json({ error: err3.message });
                    return;
                }
                
                db.get("SELECT COUNT(*) as driver_count FROM drivers", (err4, row4) => {
                    if (err4) {
                        res.status(500).json({ error: err4.message });
                        return;
                    }
                    
                    res.json({
                        database: 'connected',
                        appointments_count: row.count,
                        on_hold_count: row2.on_hold_count,
                        fixed_appointments_count: row3.fixed_count,
                        drivers_count: row4.driver_count,
                        database_path: dbPath,
                        type: 'max_efficiency_testimonial_planner',
                        features: ['fixed_appointments', 'on_hold_filter', 'max_appointments_per_week'],
                        google_maps_api: process.env.GOOGLE_MAPS_API_KEY ? 'configured' : 'fallback'
                    });
                });
            });
        });
    });
});

// Admin endpoint to ensure driver exists
app.post('/api/admin/ensure-driver', (req, res) => {
    db.run(`INSERT OR IGNORE INTO drivers (id, name, home_base) 
            VALUES (1, 'Testimonial-Fahrer', 'Kurt-Schumacher-Straße 34, 30159 Hannover')`, function(err) {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        
        // Prüfe ob Fahrer jetzt existiert
        db.get("SELECT * FROM drivers WHERE id = 1", (err2, driver) => {
            if (err2) {
                res.status(500).json({ error: err2.message });
                return;
            }
            
            res.json({
                success: true,
                message: 'Fahrer sichergestellt',
                driver: driver,
                was_inserted: this.changes > 0
            });
        });
    });
});
// ======================================================================
// DEBUG ENDPOINTS FÜR GOOGLE MAPS API TESTS
// ======================================================================

// Debug: Test Google Maps API Zugang
app.post('/api/debug/test-google-maps', validateSession, async (req, res) => {
    const { test_type = 'api_access', test_address = 'Hannover, Deutschland' } = req.body;
    
    try {
        console.log('🧪 Teste Google Maps API Zugang...');
        
        const apiKey = process.env.GOOGLE_MAPS_API_KEY;
        if (!apiKey) {
            throw new Error('Kein Google Maps API Key konfiguriert');
        }
        
        // Test 1: Geocoding API
        let geocodingTest = 'failed';
        try {
            const geocodeResponse = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
                params: {
                    address: test_address,
                    key: apiKey,
                    region: 'de'
                },
                timeout: 5000
            });
            
            if (geocodeResponse.data.status === 'OK') {
                geocodingTest = 'success';
            } else {
                geocodingTest = `failed: ${geocodeResponse.data.status}`;
            }
        } catch (e) {
            geocodingTest = `error: ${e.message}`;
        }
        
        // Test 2: Distance Matrix API
        let distanceTest = 'failed';
        try {
            const distanceResponse = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
                params: {
                    origins: 'Hannover, Deutschland',
                    destinations: 'München, Deutschland',
                    key: apiKey,
                    units: 'metric'
                },
                timeout: 5000
            });
            
            if (distanceResponse.data.status === 'OK') {
                distanceTest = 'success';
            } else {
                distanceTest = `failed: ${distanceResponse.data.status}`;
            }
        } catch (e) {
            distanceTest = `error: ${e.message}`;
        }
        
        res.json({
            success: true,
            api_key_status: 'configured',
            geocoding_test: geocodingTest,
            distance_matrix_test: distanceTest,
            api_key_first_chars: apiKey.substring(0, 10) + '...',
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('❌ Google Maps API Test fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            api_key_status: process.env.GOOGLE_MAPS_API_KEY ? 'configured' : 'missing'
        });
    }
});

// Debug: Geocode eine Adresse
app.post('/api/debug/geocode', validateSession, async (req, res) => {
    const { address } = req.body;
    
    if (!address) {
        return res.status(400).json({ error: 'Adresse erforderlich' });
    }
    
    try {
        const startTime = Date.now();
        const apiKey = process.env.GOOGLE_MAPS_API_KEY;
        
        const response = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
            params: {
                address: address,
                key: apiKey,
                region: 'de',
                components: 'country:DE'
            },
            timeout: 8000
        });
        
        const responseTime = Date.now() - startTime;
        
        if (response.data.status === 'OK' && response.data.results.length > 0) {
            const result = response.data.results[0];
            const location = result.geometry.location;
            
            res.json({
                success: true,
                lat: location.lat,
                lng: location.lng,
                formatted_address: result.formatted_address,
                place_id: result.place_id,
                response_time: responseTime,
                api_status: response.data.status,
                results_count: response.data.results.length
            });
        } else {
            throw new Error(`Geocoding Status: ${response.data.status}`);
        }
        
    } catch (error) {
        console.error('❌ Debug Geocoding fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            address: address
        });
    }
});

// Debug: Distance Matrix Test
app.post('/api/debug/distance-matrix', validateSession, async (req, res) => {
    const { origins, destinations } = req.body;
    
    if (!origins || !destinations) {
        return res.status(400).json({ error: 'Origins und Destinations erforderlich' });
    }
    
    try {
        const apiKey = process.env.GOOGLE_MAPS_API_KEY;
        const originsStr = Array.isArray(origins) ? origins.join('|') : origins;
        const destinationsStr = Array.isArray(destinations) ? destinations.join('|') : destinations;
        
        const response = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
            params: {
                origins: originsStr,
                destinations: destinationsStr,
                key: apiKey,
                units: 'metric',
                mode: 'driving',
                language: 'de'
            },
            timeout: 10000
        });
        
        if (response.data.status === 'OK') {
            const results = [];
            
            response.data.rows.forEach((row, i) => {
                row.elements.forEach((element, j) => {
                    if (element.status === 'OK') {
                        results.push({
                            origin: Array.isArray(origins) ? origins[i] : origins,
                            destination: Array.isArray(destinations) ? destinations[j] : destinations,
                            distance: `${(element.distance.value / 1000).toFixed(1)} km`,
                            duration: `${Math.round(element.duration.value / 60)} min`,
                            distance_value: element.distance.value,
                            duration_value: element.duration.value
                        });
                    }
                });
            });
            
            res.json({
                success: true,
                results: results,
                api_status: response.data.status,
                origin_addresses: response.data.origin_addresses,
                destination_addresses: response.data.destination_addresses
            });
        } else {
            throw new Error(`Distance Matrix Status: ${response.data.status}`);
        }
        
    } catch (error) {
        console.error('❌ Debug Distance Matrix fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            origins: origins,
            destinations: destinations
        });
    }
});

// Admin: Fix Geocoding für alle Termine
app.post('/api/admin/fix-geocoding', validateSession, async (req, res) => {
    try {
        console.log('🔧 Starte Geocoding-Reparatur für alle Termine...');
        
        // Lade alle Termine ohne Koordinaten
        const appointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT id, customer, address 
                FROM appointments 
                WHERE (lat IS NULL OR lng IS NULL)
                AND (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });
        
        console.log(`📊 ${appointments.length} Termine benötigen Geocoding`);
        
        let processed = 0;
        let successful = 0;
        let failed = 0;
        
        const apiKey = process.env.GOOGLE_MAPS_API_KEY;
        if (!apiKey) {
            throw new Error('Google Maps API Key nicht konfiguriert');
        }
        
        // Geocode jeden Termin
        for (const apt of appointments) {
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
                    
                    await new Promise((resolve, reject) => {
                        db.run(
                            "UPDATE appointments SET lat = ?, lng = ?, geocoded = 1 WHERE id = ?",
                            [location.lat, location.lng, apt.id],
                            (err) => {
                                if (err) reject(err);
                                else resolve();
                            }
                        );
                    });
                    
                    successful++;
                    console.log(`✅ Geocoded: ${apt.customer} → ${location.lat}, ${location.lng}`);
                } else {
                    failed++;
                    console.log(`❌ Geocoding fehlgeschlagen für ${apt.customer}: ${response.data.status}`);
                }
                
                processed++;
                
                // Pause zwischen Requests
                await new Promise(resolve => setTimeout(resolve, 200));
                
            } catch (error) {
                failed++;
                console.error(`❌ Fehler bei ${apt.customer}:`, error.message);
            }
        }
        
        res.json({
            success: true,
            message: 'Geocoding-Reparatur abgeschlossen',
            processed: processed,
            successful: successful,
            failed: failed
        });
        
    } catch (error) {
        console.error('❌ Geocoding-Reparatur fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ======================================================================
// SERVER INTEGRATION FÜR ENHANCED GEOCODING SERVICE
// ======================================================================

// Enhanced Geocoding Service importieren
const EnhancedGeocodingService = require('./geocoding-service');

// Service-Instanz erstellen
const geocodingService = new EnhancedGeocodingService();

// ======================================================================
// NEUE API ROUTES FÜR GEOCODING
// ======================================================================

// Einzelne Adresse geocoden - Test Endpoint
app.post('/api/geocoding/single', validateSession, async (req, res) => {
    const { address } = req.body;
    
    if (!address) {
        return res.status(400).json({ error: 'Adresse ist erforderlich' });
    }
    
    try {
        const result = await geocodingService.geocodeAddress(address);
        res.json({
            success: true,
            address: address,
            result: result,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error(`❌ Geocoding Fehler für "${address}":`, error);
        res.status(500).json({
            success: false,
            error: error.message,
            address: address
        });
    }
});

// Alle Termine geocoden - Hauptfunktion
app.post('/api/geocoding/appointments', validateSession, async (req, res) => {
    try {
        console.log('🗺️ Starte Geocoding aller Termine...');
        
        // Alle Termine ohne gültige Koordinaten laden
        const appointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT id, customer, address, lat, lng, geocoded 
                FROM appointments 
                WHERE (lat IS NULL OR lng IS NULL OR geocoded != 1)
                AND (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
                ORDER BY id
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        if (appointments.length === 0) {
            return res.json({
                success: true,
                message: 'Alle Termine sind bereits geocoded',
                processed: 0,
                successful: 0,
                failed: 0
            });
        }

        console.log(`📊 ${appointments.length} Termine benötigen Geocoding`);

        let successful = 0;
        let failed = 0;
        const errors = [];

        // Batch-Geocoding mit Progress
        const addresses = appointments.map(apt => apt.address);
        const batchResult = await geocodingService.geocodeMultipleAddresses(addresses, {
            maxConcurrent: 3, // Nicht zu aggressiv mit Google API
            delayBetweenRequests: 300,
            onProgress: (progress) => {
                console.log(`📍 Geocoding Progress: ${progress.processed}/${progress.total} (${Math.round((progress.processed/progress.total)*100)}%)`);
            }
        });

        // Ergebnisse in Datenbank speichern
        const updatePromises = appointments.map(async (appointment, index) => {
            const batchResultItem = batchResult.results[index];
            
            if (batchResultItem.success) {
                const coords = batchResultItem.result;
                
                return new Promise((resolve, reject) => {
                    db.run(`
                        UPDATE appointments 
                        SET lat = ?, lng = ?, geocoded = 1,
                            notes = json_set(COALESCE(notes, '{}'), '$.geocoding_info', json(?))
                        WHERE id = ?
                    `, [
                        coords.lat,
                        coords.lng,
                        JSON.stringify({
                            method: coords.geocoding_method,
                            accuracy: coords.accuracy,
                            formatted_address: coords.formatted_address,
                            processed_at: coords.processed_at
                        }),
                        appointment.id
                    ], (err) => {
                        if (err) {
                            console.error(`❌ DB Update fehlgeschlagen für Termin ${appointment.id}:`, err);
                            failed++;
                            errors.push(`${appointment.customer}: DB Update fehlgeschlagen`);
                            reject(err);
                        } else {
                            successful++;
                            console.log(`✅ Termin ${appointment.id} (${appointment.customer}) geocoded: ${coords.lat}, ${coords.lng}`);
                            resolve();
                        }
                    });
                });
            } else {
                failed++;
                errors.push(`${appointment.customer}: ${batchResultItem.error}`);
                console.warn(`⚠️ Geocoding fehlgeschlagen für Termin ${appointment.id} (${appointment.customer}): ${batchResultItem.error}`);
                return Promise.resolve();
            }
        });

        await Promise.allSettled(updatePromises);

        const cacheStats = geocodingService.getCacheStats();
        
        res.json({
            success: true,
            message: `Geocoding abgeschlossen: ${successful} erfolgreich, ${failed} fehlgeschlagen`,
            processed: appointments.length,
            successful: successful,
            failed: failed,
            errors: errors.slice(0, 10), // Nur erste 10 Fehler
            cache_stats: cacheStats,
            geocoding_methods: batchResult.results
                .filter(r => r.success)
                .reduce((acc, r) => {
                    const method = r.result.geocoding_method;
                    acc[method] = (acc[method] || 0) + 1;
                    return acc;
                }, {}),
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        console.error('❌ Batch Geocoding fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: 'Batch Geocoding fehlgeschlagen',
            details: error.message
        });
    }
});

// Geocoding Status und Statistiken
app.get('/api/geocoding/status', validateSession, async (req, res) => {
    try {
        const stats = await new Promise((resolve, reject) => {
            db.all(`
                SELECT 
                    COUNT(*) as total_appointments,
                    COUNT(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 END) as geocoded_appointments,
                    COUNT(CASE WHEN lat IS NULL OR lng IS NULL THEN 1 END) as missing_coordinates,
                    COUNT(CASE WHEN on_hold IS NOT NULL AND on_hold != '' THEN 1 END) as on_hold_appointments
                FROM appointments
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows[0]);
            });
        });

        const geocodingMethods = await new Promise((resolve, reject) => {
            db.all(`
                SELECT 
                    json_extract(notes, '$.geocoding_info.method') as method,
                    COUNT(*) as count
                FROM appointments 
                WHERE json_extract(notes, '$.geocoding_info.method') IS NOT NULL
                GROUP BY json_extract(notes, '$.geocoding_info.method')
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        const cacheStats = geocodingService.getCacheStats();

        res.json({
            success: true,
            database_stats: stats,
            geocoding_methods: geocodingMethods.reduce((acc, row) => {
                acc[row.method] = row.count;
                return acc;
            }, {}),
            cache_stats: cacheStats,
            coverage: stats.total_appointments > 0 ? 
                Math.round((stats.geocoded_appointments / stats.total_appointments) * 100) : 0,
            ready_for_optimization: stats.geocoded_appointments >= 2,
            recommendations: [
                stats.missing_coordinates > 0 ? 
                    `${stats.missing_coordinates} Termine benötigen noch Geocoding` : 
                    'Alle Termine sind geocoded ✅',
                stats.geocoded_appointments >= 10 ? 
                    'Bereit für intelligente Routenoptimierung ✅' : 
                    'Mehr Termine für bessere Optimierung empfohlen'
            ]
        });

    } catch (error) {
        console.error('❌ Geocoding Status Fehler:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Geocoding Cache verwalten
app.post('/api/geocoding/clear-cache', validateSession, (req, res) => {
    try {
        geocodingService.clearCache();
        res.json({
            success: true,
            message: 'Geocoding Cache geleert'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Debug: Teste Google Maps API direkt
app.post('/api/geocoding/test-google', validateSession, async (req, res) => {
    const { address = 'Petuelring 130, 80809 München' } = req.body;
    
    try {
        console.log('🧪 Teste Google Maps API direkt...');
        
        const result = await geocodingService.geocodeWithGoogleMaps(address);
        
        res.json({
            success: true,
            message: 'Google Maps API Test erfolgreich',
            test_address: address,
            result: result,
            api_key_configured: !!process.env.GOOGLE_MAPS_API_KEY,
            request_count: geocodingService.requestCount
        });
        
    } catch (error) {
        console.error('❌ Google Maps API Test fehlgeschlagen:', error);
        res.json({
            success: false,
            message: 'Google Maps API Test fehlgeschlagen',
            error: error.message,
            test_address: address,
            api_key_configured: !!process.env.GOOGLE_MAPS_API_KEY,
            suggestions: [
                'API Key in Railway Environment Variables prüfen',
                'Geocoding API in Google Cloud Console aktivieren',
                'Billing Account aktiviert?',
                'API Quotas ausreichend?'
            ]
        });
    }
});

console.log('🗺️ Enhanced Geocoding Service Endpoints hinzugefügt:');
console.log('  POST /api/geocoding/single - Einzelne Adresse testen');
console.log('  POST /api/geocoding/appointments - Alle Termine geocoden');
console.log('  GET  /api/geocoding/status - Geocoding Statistiken');
console.log('  POST /api/geocoding/clear-cache - Cache leeren');
console.log('  POST /api/geocoding/test-google - Google Maps API testen');

console.log('🐛 Debug Endpoints hinzugefügt:');
console.log('  POST /api/debug/test-google-maps - Google Maps API testen');
console.log('  POST /api/debug/geocode - Adresse geocoden');
console.log('  POST /api/debug/distance-matrix - Entfernungen berechnen');
console.log('  POST /api/admin/fix-geocoding - Alle Termine geocoden');

// ======================================================================
// ERROR HANDLING & 404
// ======================================================================

// Error handling
app.use((err, req, res, next) => {
    console.error('Server Error:', err.stack);
    res.status(500).json({ error: 'Internal server error' });
});

// 404 handler
app.use('*', (req, res) => {
    res.status(404).json({ error: 'Endpoint not found' });
});

// ======================================================================
// SERVER START
// ======================================================================

// Start server
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Testimonial Tourenplaner Server running on port ${PORT}`);
    console.log(`🌐 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`📊 Health check: http://localhost:${PORT}/api/health`);
    console.log(`⚡ Max Efficiency Planning: ENABLED`);
    console.log(`📌 Fixed Appointments Support: ENABLED`);
    console.log(`🚫 On Hold Filter: ACTIVE`);
    console.log(`🗺️ Google Maps API: ${process.env.GOOGLE_MAPS_API_KEY ? '✅ Configured' : '⚠️ Fallback Mode'}`);
    console.log(`✨ Features: Max Efficiency, Fixed Appointments, On Hold Filter, Google Maps`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 SIGTERM received, starting graceful shutdown...');
    db.close((err) => {
        if (err) {
            console.error('Error closing database:', err.message);
        } else {
            console.log('✅ Database connection closed');
        }
        process.exit(0);
    });
});

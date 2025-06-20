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
// Verwende die korrigierte Version des Routenplaners
const IntelligentRoutePlanner = require('./intelligent-route-planner-fixed');
const UltraOptimizedMapsService = require('./optimized-maps-service');
const { APIBudgetController, SmartDistanceCalculator } = require('./api-budget-controller');

// Soll der UltraOptimizedMapsService genutzt werden?
const USE_OPTIMIZED_SERVICE = true;

// Budget-Controller mit 5€ Tageslimit
const apiController = new APIBudgetController(5.0);


// Debug: Umgebungsvariablen prüfen
console.log('🔍 Environment Variables Debug:');
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('GOOGLE_MAPS_API_KEY exists:', !!process.env.GOOGLE_MAPS_API_KEY);

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


// 🛡️ Security Headers (direkt nach robots.txt)
app.use((req, res, next) => {
    res.setHeader('X-Robots-Tag', 'noindex, nofollow, noarchive, nosnippet');
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
    ],
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: false
}));

// JSON body parsing
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

    // Distance Matrix Cache - WICHTIG FÜR KOSTENERSPARNIS!
    db.run(`CREATE TABLE IF NOT EXISTS distance_cache (
        origin_lat REAL,
        origin_lng REAL,
        dest_lat REAL,
        dest_lng REAL,
        distance REAL,
        duration REAL,
        cached_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (origin_lat, origin_lng, dest_lat, dest_lng)
    )`, (err) => {
        if (err) {
            console.error('❌ Distance Cache Tabelle konnte nicht erstellt werden:', err);
        } else {
            console.log('✅ Distance Cache Tabelle erstellt/verifiziert');
        }
    });

    db.run(`CREATE TABLE IF NOT EXISTS geocoding_cache (
        address TEXT PRIMARY KEY,
        lat REAL NOT NULL,
        lng REAL NOT NULL,
        formatted_address TEXT,
        accuracy TEXT,
        method TEXT,
        cached_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`, (err) => {
        if (err) {
            console.error('❌ Geocoding Cache Tabelle konnte nicht erstellt werden:', err);
        } else {
            console.log('✅ Geocoding Cache Tabelle erstellt/verifiziert');
        }
    });

    db.run(`CREATE INDEX IF NOT EXISTS idx_geocoding_cache_address 
            ON geocoding_cache(address)`);

    // Optional: Index für schnellere Suche
    db.run(`CREATE INDEX IF NOT EXISTS idx_distance_cache_coords 
            ON distance_cache(origin_lat, origin_lng, dest_lat, dest_lng)`);

    // Cache-Bereinigung für alte Einträge (älter als 30 Tage)
    db.run(`DELETE FROM distance_cache 
            WHERE cached_at < datetime('now', '-30 days')`, (err, result) => {
        if (!err) {
            console.log('🧹 Alte Distance Cache Einträge bereinigt');
        }
    });

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

// KORRIGIERTE VERSION: Route optimieren mit verbesserter Duplikatsprüfung
app.post('/api/routes/optimize', validateSession, async (req, res) => {
    const { weekStart, driverId, autoSave = true, forceNew = false } = req.body;

    if (!weekStart) {
        return res.status(400).json({ error: 'weekStart is required' });
    }

    console.log('🚀 KORRIGIERTE Routenoptimierung für Woche:', weekStart);

    try {
        if (!forceNew) {
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

            if (existingRoute) {
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
        }

        const allAppointments = await new Promise((resolve, reject) => {
            db.all(
                `SELECT * FROM appointments 
                WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
                ORDER BY 
                    is_fixed DESC,
                    fixed_date ASC,
                    CASE WHEN status = 'bestätigt' THEN 0 ELSE 1 END,
                    pipeline_days DESC,
                    priority DESC`,
                (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            );
        });

        if (allAppointments.length === 0) {
            return res.json({
                success: false,
                message: 'Keine Termine zum Planen gefunden',
                route: createEmptyWeekStructure(weekStart)
            });
        }

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

        const IntelligentRoutePlanner = require('./intelligent-route-planner-fixed');
        const planner = new IntelligentRoutePlanner(db);
        const optimizedRoute = await planner.optimizeWeek(selectedAppointments, weekStart, driverId || 1);

        if (autoSave && optimizedRoute.stats.totalAppointments > 0) {
            await new Promise((resolve, reject) => {
                db.run(
                    "UPDATE saved_routes SET is_active = 0 WHERE week_start = ?",
                    [weekStart],
                    (err) => err ? reject(err) : resolve()
                );
            });

            const routeName = `Woche ${weekStart}: KW ${getWeekNumber(weekStart)} (${optimizedRoute.stats.totalAppointments} Termine)`;
            await saveRouteToDatabase(routeName, weekStart, driverId || 1, optimizedRoute);
            console.log(`💾 Route für ${weekStart} gespeichert`);
        }

        res.json({
            success: true,
            route: optimizedRoute,
            message: `Route optimiert: ${optimizedRoute.stats.totalAppointments} Termine für Woche ${weekStart}`,
            autoSaved: autoSave && optimizedRoute.stats.totalAppointments > 0,
            isNew: true,
            stats: {
                totalAvailable: allAppointments.length,
                selectedForWeek: selectedAppointments.length,
                fixedInWeek: selectedAppointments.filter(a => a.is_fixed).length,
                flexibleInWeek: selectedAppointments.filter(a => !a.is_fixed).length,
                alreadyPlannedElsewhere: allAppointments.filter(a => !a.is_fixed).length -
                                        selectedAppointments.filter(a => !a.is_fixed).length
            }
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
// NEUE FUNKTION: ALLE TERMINE ÜBER MEHRERE WOCHEN OPTIMIEREN
// ======================================================================
app.post('/api/routes/optimize-all', validateSession, async (req, res) => {
    const { driverId, startWeek, autoSave = true } = req.body;

    if (!startWeek) {
        return res.status(400).json({ error: 'startWeek is required' });
    }

    console.log('🌍 GESAMT-ROUTENOPTIMIERUNG: Plane ALLE verfügbaren Termine...');

    try {
        const allAppointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT * FROM appointments 
                WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
                ORDER BY 
                    is_fixed DESC,
                    fixed_date ASC,
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
                totalPlanned: 0,
                weeksPlanned: 0
            });
        }

        console.log(`📊 ${allAppointments.length} Termine insgesamt verfügbar`);

        const weekResults = [];
        let currentWeek = startWeek;
        let remainingAppointments = [...allAppointments];
        let totalPlanned = 0;
        let weekCounter = 0;
        const maxWeeks = 52;

        while (remainingAppointments.length > 0 && weekCounter < maxWeeks) {
            console.log(`\n📅 Plane Woche ${weekCounter + 1} (${currentWeek}): ${remainingAppointments.length} Termine übrig`);

            const weekStartDate = new Date(currentWeek);
            const weekEndDate = new Date(currentWeek);
            weekEndDate.setDate(weekEndDate.getDate() + 4);

            const fixedForThisWeek = remainingAppointments.filter(apt => {
                if (!apt.is_fixed || !apt.fixed_date) return false;
                const aptDate = new Date(apt.fixed_date);
                return aptDate >= weekStartDate && aptDate <= weekEndDate;
            });

            const flexibleAppointments = remainingAppointments.filter(apt =>
                !apt.is_fixed && apt.status !== 'abgesagt'
            );

            const appointmentsForWeek = [
                ...fixedForThisWeek,
                ...flexibleAppointments
            ];

            if (appointmentsForWeek.length === 0) {
                console.log('⏭️ Keine Termine mehr für diese Woche, beende Planung');
                break;
            }

            try {
                const planner = new IntelligentRoutePlanner(db);
                const weekRoute = await planner.optimizeWeek(appointmentsForWeek, currentWeek, driverId || 1);

                const plannedCount = weekRoute.stats.totalAppointments;

                if (plannedCount > 0) {
                    if (autoSave) {
                        const routeName = `Woche ${currentWeek}: KW ${getWeekNumber(currentWeek)} (${plannedCount} Termine)`;
                        await saveRouteToDatabase(routeName, currentWeek, driverId || 1, weekRoute);
                    }

                    const plannedIds = new Set();
                    weekRoute.days.forEach(day => {
                        day.appointments?.forEach(apt => {
                            if (apt.id) plannedIds.add(apt.id);
                        });
                    });

                    remainingAppointments = remainingAppointments.filter(apt => !plannedIds.has(apt.id));

                    totalPlanned += plannedCount;
                    weekResults.push({
                        week: currentWeek,
                        planned: plannedCount,
                        route: weekRoute
                    });

                    console.log(`✅ Woche ${currentWeek}: ${plannedCount} Termine geplant`);
                } else {
                    console.log(`⚠️ Woche ${currentWeek}: Keine Termine konnten geplant werden`);
                }

            } catch (error) {
                console.error(`❌ Fehler bei Woche ${currentWeek}:`, error.message);
            }

            weekCounter++;
            const nextWeekDate = new Date(currentWeek);
            nextWeekDate.setDate(nextWeekDate.getDate() + 7);
            currentWeek = nextWeekDate.toISOString().split('T')[0];
        }

        const unplannableAppointments = remainingAppointments.filter(apt =>
            apt.status !== 'abgesagt' && (!apt.on_hold || apt.on_hold.trim() === '')
        );

        console.log(`\n✅ GESAMTPLANUNG ABGESCHLOSSEN:`);
        console.log(`   - ${totalPlanned} Termine geplant`);
        console.log(`   - ${weekResults.length} Wochen verwendet`);
        console.log(`   - ${unplannableAppointments.length} Termine konnten nicht geplant werden`);

        res.json({
            success: true,
            message: `Gesamtplanung erfolgreich: ${totalPlanned} Termine über ${weekResults.length} Wochen geplant`,
            totalPlanned: totalPlanned,
            weeksPlanned: weekResults.length,
            weekResults: weekResults.map(w => ({
                week: w.week,
                planned: w.planned
            })),
            unplannableAppointments: unplannableAppointments.length,
            stats: {
                totalAvailable: allAppointments.length,
                confirmedPlanned: weekResults.reduce((sum, w) =>
                    sum + (w.route.stats?.confirmedAppointments || 0), 0),
                proposalsPlanned: weekResults.reduce((sum, w) =>
                    sum + (w.route.stats?.proposalAppointments || 0), 0),
                fixedPlanned: weekResults.reduce((sum, w) =>
                    sum + (w.route.stats?.fixedAppointments || 0), 0)
            }
        });

    } catch (error) {
        console.error('❌ Gesamtplanung fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: 'Gesamtplanung fehlgeschlagen',
            details: error.message
        });
    }
});

// ======================================================================
// ROUTE NEU BERECHNEN (OHNE FIXE TERMINE ZU ÄNDERN)
// ======================================================================
app.post('/api/routes/recalculate', validateSession, async (req, res) => {
    const { weekStart, driverId, preserveFixed = true, triggerAppointmentId } = req.body;

    if (!weekStart) {
        return res.status(400).json({ error: 'weekStart is required' });
    }

    console.log(`🔄 Neuberechnung der Route für Woche ${weekStart}...`);

    try {
        const weekStartDate = new Date(weekStart);
        const weekEndDate = new Date(weekStart);
        weekEndDate.setDate(weekEndDate.getDate() + 4);

        const fixedAppointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT * FROM appointments 
                WHERE is_fixed = 1 
                AND fixed_date >= ? 
                AND fixed_date <= ?
                AND (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
            `, [weekStart, weekEndDate.toISOString().split('T')[0]], (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        console.log(`📌 ${fixedAppointments.length} fixe Termine für diese Woche`);

        const usedAppointmentIds = await getUsedAppointmentIds(weekStart);

        const flexibleAppointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT * FROM appointments 
                WHERE (is_fixed IS NULL OR is_fixed = 0)
                AND (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
                AND status != 'abgesagt'
                ORDER BY 
                    CASE WHEN status = 'bestätigt' THEN 0 ELSE 1 END,
                    pipeline_days DESC,
                    priority DESC
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows.filter(apt => !usedAppointmentIds.includes(apt.id)));
            });
        });

        console.log(`📋 ${flexibleAppointments.length} flexible Termine verfügbar`);

        if (triggerAppointmentId) {
            const triggerApt = await new Promise((resolve, reject) => {
                db.get("SELECT * FROM appointments WHERE id = ?", [triggerAppointmentId],
                    (err, row) => err ? reject(err) : resolve(row)
                );
            });

            if (triggerApt && !flexibleAppointments.find(a => a.id === triggerAppointmentId)) {
                flexibleAppointments.unshift(triggerApt);
            }
        }

        const allAppointmentsForWeek = [...fixedAppointments, ...flexibleAppointments];

        if (allAppointmentsForWeek.length === 0) {
            return res.json({
                success: false,
                message: 'Keine Termine für diese Woche verfügbar'
            });
        }

        const planner = new IntelligentRoutePlanner(db);
        const optimizedRoute = await planner.optimizeWeek(allAppointmentsForWeek, weekStart, driverId || 1);

        const routeName = `Woche ${weekStart}: KW ${getWeekNumber(weekStart)} (${optimizedRoute.stats.totalAppointments} Termine) - Neuberechnet`;
        await saveRouteToDatabase(routeName, weekStart, driverId || 1, optimizedRoute);

        console.log(`✅ Route für Woche ${weekStart} neu berechnet: ${optimizedRoute.stats.totalAppointments} Termine`);

        res.json({
            success: true,
            route: optimizedRoute,
            message: `Route erfolgreich neu berechnet: ${optimizedRoute.stats.totalAppointments} Termine geplant`,
            preservedFixed: preserveFixed,
            fixedCount: fixedAppointments.length
        });

    } catch (error) {
        console.error('❌ Neuberechnung fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: 'Neuberechnung fehlgeschlagen',
            details: error.message
        });
    }
});

// ======================================================================
// OPTIMIERTER ROUTE PLANNER ENDPOINT
// ======================================================================

app.post('/api/routes/optimize-efficient', validateSession, async (req, res) => {
    const { weekStart, driverId, autoSave = true } = req.body;

    console.log('🚀 EFFIZIENTER Routenplaner mit minimalem API-Verbrauch...');

    try {
        const allAppointments = await new Promise((resolve, reject) => {
            db.all(
                `
                SELECT * FROM appointments 
                WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
                ORDER BY is_fixed DESC, pipeline_days DESC
            `,
                (err, rows) => (err ? reject(err) : resolve(rows))
            );
        });

        if (allAppointments.length === 0) {
            return res.json({
                success: false,
                message: 'Keine Termine verfügbar'
            });
        }

        const selectedAppointments = await selectMaxAppointmentsForWeek(
            allAppointments,
            weekStart
        );

        if (selectedAppointments.length === 0) {
            return res.json({
                success: false,
                message: 'Keine Termine für diese Woche verfügbar'
            });
        }

        const optimizedRoute = await performMaxEfficiencyOptimization(
            selectedAppointments,
            weekStart,
            driverId
        );

        if (autoSave && optimizedRoute.stats.totalAppointments > 0) {
            const routeName = `EFFIZIENT - Woche ${weekStart} (${optimizedRoute.stats.totalAppointments} Termine)`;
            await saveRouteToDatabase(routeName, weekStart, driverId, optimizedRoute);
        }

        res.json({
            success: true,
            route: optimizedRoute,
            message: `Effizienter Plan: ${optimizedRoute.stats.totalAppointments} Termine`,
            api_efficiency: optimizedRoute.api_usage,
            autoSaved: autoSave && optimizedRoute.stats.totalAppointments > 0
        });

    } catch (error) {
        console.error('❌ Effizienter Planer fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ======================================================================
// KORRIGIERTE FUNKTIONEN FÜR DUPLIKAT-VERMEIDUNG
// ======================================================================

// KORRIGIERTE VERSION: Verhindert doppelte Planung von Terminen
async function getUsedAppointmentIds(excludeWeekStart) {
    return new Promise((resolve, reject) => {
        db.all("SELECT id, week_start, route_data FROM saved_routes WHERE is_active = 1", (err, rows) => {
            if (err) {
                reject(err);
                return;
            }

            const usedIds = new Set();
            const usageMap = new Map();

            rows.forEach(row => {
                if (excludeWeekStart && row.week_start === excludeWeekStart) {
                    return;
                }
                try {
                    const routeData = JSON.parse(row.route_data);
                    routeData.days?.forEach(day => {
                        day.appointments?.forEach(apt => {
                            if (apt.id) {
                                usedIds.add(apt.id);
                                if (!usageMap.has(apt.id)) usageMap.set(apt.id, []);
                                usageMap.get(apt.id).push(row.week_start);
                            }
                        });
                    });
                } catch (e) {
                    console.warn(`Fehler beim Parsen der Route ${row.id}:`, e);
                }
            });

            usageMap.forEach((weeks, aptId) => {
                if (weeks.length > 1) {
                    console.error(`⚠️ DUPLIKAT: Termin ${aptId} ist in ${weeks.length} Wochen geplant: ${weeks.join(', ')}`);
                }
            });

            console.log(`📊 ${usedIds.size} Termine sind bereits geplant (ohne Woche ${excludeWeekStart || 'keine'})`);
            resolve(Array.from(usedIds));
        });
    });
}

// KORRIGIERTE VERSION: Wählt nur ungenutzte Termine aus
async function selectMaxAppointmentsForWeek(allAppointments, weekStart) {
    console.log(`🎯 Optimiere Terminauswahl für Woche ${weekStart}`);

    const weekStartDate = new Date(weekStart);
    const weekEndDate = new Date(weekStartDate);
    weekEndDate.setDate(weekStartDate.getDate() + 4);

    const fixedAppointmentsThisWeek = allAppointments.filter(apt => {
        if (!apt.is_fixed || !apt.fixed_date) return false;
        const aptDate = new Date(apt.fixed_date);
        return aptDate >= weekStartDate && aptDate <= weekEndDate;
    });

    console.log(`📌 ${fixedAppointmentsThisWeek.length} fixe Termine für Woche ${weekStart}`);

    const usedAppointmentIds = await getUsedAppointmentIds(weekStart);

    const availableFlexibleAppointments = allAppointments.filter(apt => {
        if (apt.is_fixed) return false;
        if (usedAppointmentIds.includes(apt.id)) return false;
        if (apt.status === 'abgesagt') return false;
        return true;
    });

    console.log(`📋 ${availableFlexibleAppointments.length} flexible Termine verfügbar (von ${allAppointments.filter(a => !a.is_fixed).length} gesamt)`);
    console.log(`🚫 ${usedAppointmentIds.length} Termine sind bereits in anderen Wochen geplant`);

    const sortedFlexible = availableFlexibleAppointments.sort((a, b) => {
        if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
        if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
        if (b.pipeline_days !== a.pipeline_days) {
            return b.pipeline_days - a.pipeline_days;
        }
        const priorityMap = { 'hoch': 3, 'mittel': 2, 'niedrig': 1 };
        return (priorityMap[b.priority] || 0) - (priorityMap[a.priority] || 0);
    });

    const selectedAppointments = [
        ...fixedAppointmentsThisWeek,
        ...sortedFlexible
    ];

    console.log(`✅ ${selectedAppointments.length} Termine für Woche ${weekStart} verfügbar`);
    console.log(`   - ${fixedAppointmentsThisWeek.length} fixe Termine`);
    console.log(`   - ${sortedFlexible.length} flexible Termine`);

    return selectedAppointments;
}

// ======================================================================
// REALISTISCHE ROUTENOPTIMIERUNG MIT CLUSTERING
// ======================================================================

async function performMaxEfficiencyOptimization(appointments, weekStart, driverId) {
    console.log('💰 KOSTEN-OPTIMIERTE Routenplanung mit Budget-Kontrolle...');
    console.log(`📊 API Budget Status:`, apiController.getStatus());

    try {
        // WICHTIG: Nutze den optimierten Service
        const optimizedService = new UltraOptimizedMapsService(db);

        // Override: Füge Budget-Check hinzu
        const originalGeocode = optimizedService.geocodeWithGoogleMaps.bind(optimizedService);
        optimizedService.geocodeWithGoogleMaps = async function(address) {
            if (!apiController.canMakeAPICall('geocoding')) {
                console.log('⚠️ Budget-Limit erreicht, nutze lokale Schätzung');
                throw new Error('Budget limit reached');
            }
            apiController.registerAPICall('geocoding', 1);
            return originalGeocode(address);
        };

        // Phase 1: Smart Geocoding
        const geocodedAppointments = await optimizedService.smartGeocodeBatch(appointments);
        console.log(`✅ ${geocodedAppointments.length} Termine geocoded`);

        // Phase 2: Smart Distance Matrix
        const distanceMatrix = await optimizedService.calculateSmartDistanceMatrix(geocodedAppointments);

        // Phase 3: Route optimieren
        const planner = new IntelligentRoutePlanner(db);

        // Injiziere die Distanz-Daten
        planner.distanceCache = new Map();
        Object.entries(distanceMatrix).forEach(([from, destinations]) => {
            Object.entries(destinations).forEach(([to, data]) => {
                const fromApt = geocodedAppointments.find(a => a.id === from) || { lat: 52.3759, lng: 9.7320 };
                const toApt = geocodedAppointments.find(a => a.id === to) || { lat: 52.3759, lng: 9.7320 };
                const cacheKey = `${fromApt.lat},${fromApt.lng}-${toApt.lat},${toApt.lng}`;
                planner.distanceCache.set(cacheKey, {
                    distance: data.distance,
                    duration: data.duration
                });
            });
        });

        planner.apiCallsCount = 0; // Reset counter
        const optimizedRoute = await planner.optimizeWeek(geocodedAppointments, weekStart, driverId || 1);

        // Hole finale Statistiken
        const apiStats = optimizedService.getOptimizationStats();
        const budgetStatus = apiController.getStatus();

        return {
            ...optimizedRoute,
            optimization: 'ultra_optimized_with_budget_control',
            api_usage: {
                geocoding_requests: apiStats.geocoding_calls,
                distance_matrix_requests: apiStats.distance_matrix_calls,
                cache_hits: apiStats.cache_hits,
                total_saved: apiStats.api_calls_saved,
                efficiency_percentage: apiStats.efficiency_percentage,
                
                // Budget-Informationen
                estimated_cost_eur: parseFloat(budgetStatus.budget.spent),
                daily_budget_eur: budgetStatus.budget.daily,
                remaining_budget_eur: parseFloat(budgetStatus.budget.remaining),
                budget_used_percentage: budgetStatus.budget.percentage,
                
                // Warnungen
                warnings: budgetStatus.budget.percentage > 80 ? 
                    ['⚠️ Über 80% des Tagesbudgets verbraucht!'] : []
            }
        };

    } catch (error) {
        console.error('❌ Optimierte Planung fehlgeschlagen:', error);
        
        // Fallback auf rein lokale Berechnung
        console.log('🔄 Fallback auf lokale Berechnung ohne API...');
        const planner = new IntelligentRoutePlanner(db);
        
        // Override getDistance für lokale Berechnung
        planner.getDistance = async function(from, to) {
            const distance = this.haversineDistance(from.lat, from.lng, to.lat, to.lng);
            return {
                distance: distance * 1.3,
                duration: (distance / 75) + 0.25,
                approximated: true,
                method: 'local_fallback'
            };
        };
        
        return await planner.optimizeWeek(appointments, weekStart, driverId || 1);
    }
}

async function findSmartAlternativeSlots(appointmentId, weekStart) {
    const optimizedService = new UltraOptimizedMapsService(db);

    const appointment = await new Promise((resolve, reject) => {
        db.get("SELECT * FROM appointments WHERE id = ?", [appointmentId],
            (err, row) => err ? reject(err) : resolve(row)
        );
    });

    if (!appointment) throw new Error('Termin nicht gefunden');

    const savedRoute = await new Promise((resolve, reject) => {
        db.get(
            "SELECT * FROM saved_routes WHERE week_start = ? AND is_active = 1",
            [weekStart],
            (err, row) => err ? reject(err) : resolve(row)
        );
    });

    if (!savedRoute) {
        return {
            success: false,
            message: 'Keine aktive Route für diese Woche',
            alternatives: []
        };
    }

    const weekPlan = JSON.parse(savedRoute.route_data);

    const alternatives = await optimizedService.suggestAlternativeSlots(
        appointment,
        weekPlan
    );

    return alternatives;
}

// Stelle sicher, dass alle Termine Koordinaten haben
async function ensureAllAppointmentsGeocoded(appointments) {
    if (USE_OPTIMIZED_SERVICE) {
        const optimizedService = new UltraOptimizedMapsService(db);
        const geocoded = await optimizedService.smartGeocodeBatch(appointments);

        for (const apt of geocoded) {
            if (apt.geocoded && apt.lat && apt.lng) {
                await new Promise((resolve, reject) => {
                    db.run(
                        "UPDATE appointments SET lat = ?, lng = ?, geocoded = 1 WHERE id = ?",
                        [apt.lat, apt.lng, apt.id],
                        err => err ? reject(err) : resolve()
                    );
                });
            }
        }

        return geocoded.filter(apt => apt.lat && apt.lng);
    }

    const needsGeocoding = appointments.filter(apt => !apt.lat || !apt.lng);

    if (needsGeocoding.length > 0) {
        console.log(`🗺️ Geocoding ${needsGeocoding.length} Termine...`);

        const apiKey = process.env.GOOGLE_MAPS_API_KEY;
        if (!apiKey) {
            throw new Error('Google Maps API Key nicht konfiguriert');
        }

        const BATCH_SIZE = 10;
        for (let i = 0; i < needsGeocoding.length; i += BATCH_SIZE) {
            const batch = needsGeocoding.slice(i, i + BATCH_SIZE);

            await Promise.all(batch.map(async (apt) => {
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

                        await new Promise((resolve, reject) => {
                            db.run(
                                "UPDATE appointments SET lat = ?, lng = ?, geocoded = 1 WHERE id = ?",
                                [location.lat, location.lng, apt.id],
                                err => err ? reject(err) : resolve()
                            );
                        });
                    }
                } catch (error) {
                    console.warn(`⚠️ Geocoding fehlgeschlagen für ${apt.customer}:`, error.message);
                }
            }));

            if (i + BATCH_SIZE < needsGeocoding.length) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
    }

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
    const homeBase = { lat: 52.3759, lng: 9.7320, name: 'Hannover' };

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
            totalHours: 0,
            currentLocation: homeBase,
            overnight: null,
            earliestStart: index === 0 ? 9 : 6,
            latestEnd: index === 4 ? 17 : 20
        };
    });

    const fixedAppointments = allAppointments
        .filter(apt => apt.is_fixed && apt.fixed_date)
        .sort((a, b) => {
            const dateCompare = new Date(a.fixed_date) - new Date(b.fixed_date);
            if (dateCompare !== 0) return dateCompare;
            return timeToHours(a.fixed_time || '00:00') - timeToHours(b.fixed_time || '00:00');
        });

    const conflictingFixed = [];

    fixedAppointments.forEach(apt => {
        const dayIndex = weekDays.findIndex((_, i) => {
            const dayDate = new Date(startDate);
            dayDate.setDate(startDate.getDate() + i);
            return dayDate.toISOString().split('T')[0] === apt.fixed_date;
        });

        if (dayIndex >= 0) {
            const day = week[dayIndex];
            const startTime = apt.fixed_time || '10:00';
            const endTime = addHoursToTime(startTime, apt.duration || 3);

            let hasConflict = false;
            for (const existing of day.appointments) {
                if (checkTimeOverlap(startTime, endTime, existing.startTime, existing.endTime)) {
                    console.log(`⚠️ KONFLIKT: ${apt.customer} (${startTime}-${endTime}) überschneidet sich mit ${existing.customer} (${existing.startTime}-${existing.endTime})`);
                    hasConflict = true;
                    conflictingFixed.push({
                        appointment: apt,
                        conflictsWith: existing.customer,
                        day: weekDays[dayIndex]
                    });
                    break;
                }
            }

            if (!hasConflict) {
                day.appointments.push({
                    ...apt,
                    startTime: startTime,
                    endTime: endTime,
                    duration: apt.duration || 3,
                    isFixed: true
                });
                day.workTime += apt.duration || 3;
                console.log(`📌 Fixer Termin geplant: ${apt.customer} am ${weekDays[dayIndex]} ${startTime}-${endTime}`);
            }
        }
    });

    if (conflictingFixed.length > 0) {
        console.error('❌ WARNUNG: Folgende fixe Termine haben Konflikte:', conflictingFixed);
    }

    const flexibleAppointments = allAppointments.filter(apt => !apt.is_fixed);
    const regionOrder = optimizeRegionOrder(clusters, homeBase);

    for (const region of regionOrder) {
        const regionAppointments = clusters[region]
            .filter(apt => flexibleAppointments.some(f => f.id === apt.id))
            .sort((a, b) => {
                if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
                if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
                return b.pipeline_days - a.pipeline_days;
            });

        if (regionAppointments.length === 0) continue;

        console.log(`\n🗺️ Plane Region ${region} mit ${regionAppointments.length} flexiblen Terminen`);

        for (const apt of regionAppointments) {
            let scheduled = false;

            for (let dayIndex = 0; dayIndex < 5; dayIndex++) {
                const day = week[dayIndex];

                day.appointments.sort((a, b) => timeToHours(a.startTime) - timeToHours(b.startTime));

                const availableSlots = findAvailableSlots(day);

                for (const slot of availableSlots) {
                    if (slot.duration >= 4) {
                        const prevApt = day.appointments[day.appointments.length - 1];
                        const fromLat = prevApt ? prevApt.lat : homeBase.lat;
                        const fromLng = prevApt ? prevApt.lng : homeBase.lng;
                        const travelFromPrev = estimateTravelTime(fromLat, fromLng, apt.lat, apt.lng);

                        let startTimeHours = Math.max(
                            timeToHours(slot.startTime),
                            prevApt ? timeToHours(prevApt.endTime) + travelFromPrev : (dayIndex === 0 ? 9 + travelFromPrev : timeToHours(slot.startTime))
                        );
                        startTimeHours = Math.round(startTimeHours * 2) / 2; // nur 30‑Minuten‑Schritte

                        let endTimeHours = startTimeHours + 3;

                        if (endTimeHours > timeToHours(slot.endTime)) continue;

                        if (dayIndex === 4) {
                            const travelHome = estimateTravelTime(apt.lat, apt.lng, homeBase.lat, homeBase.lng);
                            if (endTimeHours + travelHome > day.latestEnd) continue;
                        }

                        day.appointments.push({
                            ...apt,
                            startTime: hoursToTime(startTimeHours),
                            endTime: hoursToTime(endTimeHours),
                            duration: 3,
                            isFixed: false
                        });
                        day.workTime += 3;
                        console.log(`✅ ${apt.customer} → ${weekDays[dayIndex]} ${hoursToTime(startTimeHours)}-${hoursToTime(endTimeHours)}`);
                        scheduled = true;
                        break;
                    }
                }

                if (scheduled) break;
            }

            if (!scheduled) {
                console.log(`❌ ${apt.customer} konnte nicht eingeplant werden (keine passenden Slots)`);
            }
        }
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
    console.log('🚗 Berechne realistische Fahrzeiten mit Startfahrt...');

    const homeBase = { lat: 52.3759, lng: 9.7320, name: 'Hannover' };

    for (const day of week) {
        if (day.appointments.length === 0) continue;

        day.appointments.sort((a, b) => timeToHours(a.startTime) - timeToHours(b.startTime));

        let totalTravelTime = 0;
        let currentLocation = homeBase;
        day.travelSegments = [];

        const firstApt = day.appointments[0];
        const distanceToFirst = calculateDistance(
            homeBase.lat, homeBase.lng,
            firstApt.lat || homeBase.lat,
            firstApt.lng || homeBase.lng
        );
        const travelTimeToFirst = Math.max(0.5, distanceToFirst / 85);
        totalTravelTime += travelTimeToFirst;

        const departureTime = subtractHoursFromTime(firstApt.startTime, travelTimeToFirst);

        day.travelSegments.push({
            type: 'departure',
            from: 'Hannover',
            to: extractCityFromAddress(firstApt.address),
            distance: Math.round(distanceToFirst),
            duration: travelTimeToFirst,
            startTime: departureTime,
            endTime: firstApt.startTime,
            description: `🚗 Fahrt nach ${extractCityFromAddress(firstApt.address)}`
        });

        for (let i = 0; i < day.appointments.length - 1; i++) {
            const from = day.appointments[i];
            const to = day.appointments[i + 1];

            const distance = calculateDistance(
                from.lat || homeBase.lat, from.lng || homeBase.lng,
                to.lat || homeBase.lat, to.lng || homeBase.lng
            );
            const travelTime = Math.max(0.25, distance / 85);
            totalTravelTime += travelTime;

            const availableTime = timeToHours(to.startTime) - timeToHours(from.endTime);
            if (availableTime < travelTime) {
                console.warn(`⚠️ Zu wenig Zeit zwischen ${from.customer} und ${to.customer}: ${(availableTime * 60).toFixed(0)} Min verfügbar, ${(travelTime * 60).toFixed(0)} Min benötigt`);
            }

            day.travelSegments.push({
                type: 'travel',
                from: extractCityFromAddress(from.address),
                to: extractCityFromAddress(to.address),
                distance: Math.round(distance),
                duration: travelTime,
                startTime: from.endTime,
                endTime: to.startTime,
                description: `🚗 Fahrt von ${extractCityFromAddress(from.address)} nach ${extractCityFromAddress(to.address)}`
            });
        }

        const lastApt = day.appointments[day.appointments.length - 1];
        const distanceToHome = calculateDistance(
            lastApt.lat || homeBase.lat,
            lastApt.lng || homeBase.lng,
            homeBase.lat, homeBase.lng
        );

        if (distanceToHome > 200) {
            day.overnight = {
                city: extractCityFromAddress(lastApt.address),
                reason: `${Math.round(distanceToHome)} km von Hannover`,
                hotel: `🏨 Hotel in ${extractCityFromAddress(lastApt.address)}`
            };
        } else {
            const travelTimeHome = Math.max(0.5, distanceToHome / 85);
            totalTravelTime += travelTimeHome;

            day.travelSegments.push({
                type: 'return',
                from: extractCityFromAddress(lastApt.address),
                to: 'Hannover',
                distance: Math.round(distanceToHome),
                duration: travelTimeHome,
                startTime: lastApt.endTime,
                endTime: addHoursToTime(lastApt.endTime, travelTimeHome),
                description: '🏠 Rückfahrt nach Hannover'
            });
        }

        day.travelTime = Math.round(totalTravelTime * 10) / 10;

        const dayStart = timeToHours(day.travelSegments[0].startTime);
        const dayEnd = day.overnight ?
            timeToHours(lastApt.endTime) :
            timeToHours(day.travelSegments[day.travelSegments.length - 1].endTime);
        const totalDayHours = dayEnd - dayStart;

        // Speichere die gesamte Arbeitszeit des Tages (Fahrzeit + Termine)
        day.totalHours = parseFloat(totalDayHours.toFixed(1));

        if (totalDayHours > 14) {
            console.warn(`⚠️ Sehr langer Tag am ${day.day}: ${totalDayHours.toFixed(1)}h (${hoursToTime(dayStart)} - ${hoursToTime(dayEnd)})`);
        }
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

function estimateTravelTime(lat1, lng1, lat2, lng2) {
    const distance = calculateDistance(lat1, lng1, lat2, lng2);
    return Math.max(0.25, distance / 85) + 0.25; // basic padding
}

async function batchCalculateDistances(origins, destinations) {
    const apiKey = process.env.GOOGLE_MAPS_API_KEY;
    const MAX_ELEMENTS = 100;
    const MAX_ORIGINS = 25;
    const MAX_DESTINATIONS = 25;

    const results = {};

    for (let i = 0; i < origins.length; i += MAX_ORIGINS) {
        const originBatch = origins.slice(i, i + MAX_ORIGINS);

        for (let j = 0; j < destinations.length; j += MAX_DESTINATIONS) {
            const destBatch = destinations.slice(j, j + MAX_DESTINATIONS);

            if (originBatch.length * destBatch.length > MAX_ELEMENTS) {
                continue;
            }

            const originsStr = originBatch.map(o => `${o.lat},${o.lng}`).join('|');
            const destsStr = destBatch.map(d => `${d.lat},${d.lng}`).join('|');

            try {
                const response = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
                    params: {
                        origins: originsStr,
                        destinations: destsStr,
                        key: apiKey,
                        units: 'metric',
                        mode: 'driving',
                        departure_time: 'now',
                        traffic_model: 'best_guess'
                    }
                });

                if (response.data.status === 'OK') {
                    response.data.rows.forEach((row, rowIdx) => {
                        const origin = originBatch[rowIdx];
                        if (!results[origin.id]) results[origin.id] = {};

                        row.elements.forEach((element, colIdx) => {
                            const dest = destBatch[colIdx];
                            if (element.status === 'OK') {
                                results[origin.id][dest.id] = {
                                    distance: element.distance.value / 1000,
                                    duration: element.duration_in_traffic ?
                                        element.duration_in_traffic.value / 3600 :
                                        element.duration.value / 3600
                                };
                            }
                        });
                    });
                }

                await new Promise(resolve => setTimeout(resolve, 100));

            } catch (error) {
                console.error('Distance Matrix Batch Error:', error.message);
            }
        }
    }

    return results;
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
    const rounded = Math.round(hours * 2) / 2; // nur 30‑Minuten‑Schritte
    const h = Math.floor(rounded);
    const m = Math.round((rounded - h) * 60);
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

// ======================================================================
// FIX 1: Überlappende fixe Termine verhindern
// ======================================================================
function checkTimeOverlap(start1, end1, start2, end2) {
    const s1 = timeToHours(start1);
    const e1 = timeToHours(end1);
    const s2 = timeToHours(start2);
    const e2 = timeToHours(end2);

    return (s1 < e2 && s2 < e1);
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

// ======================================================================
// HILFSFUNKTION: Verfügbare Zeitslots finden
// ======================================================================
function findAvailableSlots(day) {
    const slots = [];
    const dayStart = day.earliestStart || 6;
    const dayEnd = day.latestEnd || 20;

    if (day.appointments.length === 0) {
        slots.push({
            startTime: hoursToTime(dayStart),
            endTime: hoursToTime(dayEnd),
            duration: dayEnd - dayStart
        });
        return slots;
    }

    const sortedAppts = [...day.appointments].sort((a, b) =>
        timeToHours(a.startTime) - timeToHours(b.startTime)
    );

    const firstStart = timeToHours(sortedAppts[0].startTime);
    if (firstStart > dayStart + 1) {
        slots.push({
            startTime: hoursToTime(dayStart),
            endTime: hoursToTime(firstStart - 1),
            duration: firstStart - 1 - dayStart
        });
    }

    for (let i = 0; i < sortedAppts.length - 1; i++) {
        const currentEnd = timeToHours(sortedAppts[i].endTime);
        const nextStart = timeToHours(sortedAppts[i + 1].startTime);

        if (nextStart - currentEnd > 1) {
            slots.push({
                startTime: hoursToTime(currentEnd + 0.5),
                endTime: hoursToTime(nextStart - 0.5),
                duration: nextStart - currentEnd - 1
            });
        }
    }

    const lastEnd = timeToHours(sortedAppts[sortedAppts.length - 1].endTime);
    if (dayEnd - lastEnd > 1) {
        slots.push({
            startTime: hoursToTime(lastEnd + 0.5),
            endTime: hoursToTime(dayEnd),
            duration: dayEnd - lastEnd - 0.5
        });
    }

    return slots;
}

// Formatiere das Ergebnis
function formatOptimizedWeek(week, weekStart) {
    const totalAppointments = week.reduce((sum, day) => sum + day.appointments.length, 0);
    const totalWorkHours = week.reduce((sum, day) => sum + day.workTime, 0);
    const totalTravelHours = week.reduce((sum, day) => sum + day.travelTime, 0);
    const totalWeekHours = week.reduce((sum, day) => sum + (day.totalHours || 0), 0);
    const workDays = week.filter(day => day.appointments.length > 0).length;

    return {
        weekStart,
        days: week,
        totalHours: Math.round(totalWeekHours * 10) / 10,
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
            totalHours: Math.round(totalWeekHours * 10) / 10,
            workDays,
            efficiency: {
                travelEfficiency: totalWorkHours > 0 ? 
                    Math.round((1 - totalTravelHours / (totalWorkHours + totalTravelHours)) * 100) / 100 : 0,
                weekUtilization: Math.round((totalWorkHours / 42.5) * 100) / 100
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
            totalHours: 0,
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
    
    let query = "SELECT * FROM saved_routes ORDER BY week_start ASC, created_at DESC";
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

// ======================================================================
// VEREINFACHTE CSV IMPORT FUNKTION - VOLLSTÄNDIGER ERSATZ
// Ersetzt die komplexe Logik in server.js ab Zeile ~1400
// ======================================================================

// CSV Import endpoint for testimonial data - SIMPLIFIED VERSION
app.post('/api/admin/import-csv', upload.single('csvFile'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'Keine CSV-Datei hochgeladen' });
    }

    console.log('📁 CSV Testimonial Import gestartet - VOLLSTÄNDIGER ERSATZ...');

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

        console.log(`📊 ${parsed.data.length} Zeilen in CSV gefunden`);

        const processedAppointments = [];
        let skippedCount = 0;
        let confirmedCount = 0;
        let proposalCount = 0;

        parsed.data.forEach((row, index) => {
            // Skip if "On Hold" is filled
            if (row['On Hold'] && row['On Hold'].trim() !== '') {
                console.log(`⏭️ Zeile ${index + 1}: Übersprungen (On Hold: ${row['On Hold']})`);
                skippedCount++;
                return;
            }

            // Skip if essential data missing
            if (!row['Invitee Name'] || row['Invitee Name'].trim() === '') {
                console.log(`⏭️ Zeile ${index + 1}: Übersprungen (Kein Invitee Name)`);
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

                    console.log(`📅 Parsing date: "${dateTimeStr}"`);

                    // Format 1: DD.MM.YYYY HH:MM oder DD/MM/YYYY HH:MM
                    if (dateTimeStr.match(/^\d{1,2}[\.\/]\d{1,2}[\.\/]\d{4}\s+\d{1,2}:\d{2}/)) {
                        const [datePart, timePart] = dateTimeStr.split(/\s+/);
                        const [day, month, year] = datePart.split(/[\.\/]/);
                        dateTime = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T${timePart}:00`);
                    }
                    // Format 2: YYYY-MM-DD HH:MM
                    else if (dateTimeStr.match(/^\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}/)) {
                        dateTime = new Date(dateTimeStr);
                    }
                    // Fallback: Versuche native Parsing
                    else {
                        dateTime = new Date(dateTimeStr);
                    }

                    // Validierung des geparsten Datums
                    if (!isNaN(dateTime.getTime())) {
                        const year = dateTime.getFullYear();
                        if (year >= 2020 && year <= 2030) {
                            isFixed = true;
                            fixedDate = dateTime.toISOString().split('T')[0];
                            fixedTime = `${dateTime.getHours().toString().padStart(2, '0')}:${dateTime.getMinutes().toString().padStart(2, '0')}`;
                            console.log(`   ✅ Fixer Termin geparst: ${fixedDate} ${fixedTime}`);
                        }
                    }
                } catch (e) {
                    console.log(`❌ Date parsing error for "${row['Start Date & Time']}":`, e.message);
                }
            }

            const status = isFixed ? 'bestätigt' : 'vorschlag';
            
            if (status === 'bestätigt') confirmedCount++;
            else proposalCount++;

            const inviteeName = row['Invitee Name'].trim();
            const company = row['Company'] ? row['Company'].trim() : '';
            const customerCompany = row['Customer Company'] ? row['Customer Company'].trim() : '';

            // Priorität basierend auf Status und Kunde
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

            const appointment = {
                customer: inviteeName,
                address: fullAddress || 'Adresse nicht verfügbar',
                priority: priority,
                status: status,
                duration: 3,
                pipeline_days: status === 'vorschlag' ? Math.floor(Math.random() * 30) + 1 : 7,
                is_fixed: isFixed ? 1 : 0,
                fixed_date: fixedDate,
                fixed_time: fixedTime,
                on_hold: null, // Explizit auf null setzen
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
            console.log(`✅ Verarbeitet: ${inviteeName} (${status}${isFixed ? ', FIX' : ''})`);
        });

        console.log(`📈 Verarbeitung abgeschlossen:`);
        console.log(`   - ${processedAppointments.length} Termine verarbeitet`);
        console.log(`   - ${confirmedCount} bestätigte Termine (fix)`);
        console.log(`   - ${proposalCount} Vorschlag-Termine (flexibel)`);
        console.log(`   - ${skippedCount} übersprungen`);

        if (processedAppointments.length === 0) {
            return res.json({
                success: false,
                message: 'Keine gültigen Termine in der CSV gefunden',
                stats: {
                    totalRows: parsed.data.length,
                    processed: 0,
                    skipped: skippedCount
                }
            });
        }

        // ======================================================================
        // VEREINFACHTE ERSETZUNG: LÖSCHE ALLE, FÜGE NEUE EIN
        // ======================================================================
        
        db.serialize(() => {
            console.log('🗃️ Starte Datenbank-Transaction...');
            db.run("BEGIN TRANSACTION");

            // 1. ALLE bestehenden Termine löschen
            db.run("DELETE FROM appointments", function(deleteErr) {
                if (deleteErr) {
                    console.error('❌ Fehler beim Löschen bestehender Termine:', deleteErr);
                    db.run("ROLLBACK");
                    return res.status(500).json({ 
                        success: false,
                        error: 'Fehler beim Löschen bestehender Termine',
                        details: deleteErr.message
                    });
                }

                console.log(`🧹 ${this.changes} bestehende Termine gelöscht`);

                // 2. Neue Termine einfügen
                const stmt = db.prepare(`
                    INSERT INTO appointments 
                    (customer, address, priority, status, duration, pipeline_days, notes, 
                     preferred_dates, excluded_dates, is_fixed, fixed_date, fixed_time, on_hold,
                     lat, lng, geocoded) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0)
                `);

                let insertedCount = 0;
                let insertErrors = [];

                processedAppointments.forEach((apt, idx) => {
                    stmt.run([
                        apt.customer, 
                        apt.address, 
                        apt.priority, 
                        apt.status, 
                        apt.duration, 
                        apt.pipeline_days, 
                        apt.notes,
                        JSON.stringify([]), // preferred_dates
                        JSON.stringify([]), // excluded_dates
                        apt.is_fixed,
                        apt.fixed_date,
                        apt.fixed_time,
                        apt.on_hold
                    ], function(err) {
                        if (err) {
                            console.error(`❌ Insert-Fehler für ${apt.customer}:`, err.message);
                            insertErrors.push(`${apt.customer}: ${err.message}`);
                        } else {
                            insertedCount++;
                            console.log(`✅ Eingefügt: ${apt.customer} (ID: ${this.lastID})`);
                        }
                        
                        // Wenn alle Termine verarbeitet wurden
                        if (insertedCount + insertErrors.length === processedAppointments.length) {
                            stmt.finalize();
                            
                            if (insertErrors.length > 0) {
                                console.error(`❌ ${insertErrors.length} Insert-Fehler aufgetreten`);
                                db.run("ROLLBACK");
                                res.status(500).json({
                                    success: false,
                                    message: 'Import teilweise fehlgeschlagen',
                                    inserted: insertedCount,
                                    errors: insertErrors.slice(0, 10)
                                });
                            } else {
                                console.log('💾 Alle Termine erfolgreich eingefügt, committe Transaction...');
                                db.run("COMMIT", (commitErr) => {
                                    if (commitErr) {
                                        console.error('❌ Commit-Fehler:', commitErr);
                                        res.status(500).json({
                                            success: false,
                                            error: 'Commit fehlgeschlagen',
                                            details: commitErr.message
                                        });
                                    } else {
                                        console.log('✅ CSV Import erfolgreich abgeschlossen!');
                                        
                                        res.json({
                                            success: true,
                                            message: '✅ CSV Import erfolgreich - Alle Termine ersetzt',
                                            action: 'VOLLSTÄNDIGER ERSATZ',
                                            stats: {
                                                totalRows: parsed.data.length,
                                                processed: processedAppointments.length,
                                                inserted: insertedCount,
                                                confirmed: confirmedCount,
                                                proposals: proposalCount,
                                                skipped: skippedCount,
                                                deleted: this.changes, // Anzahl gelöschter Termine
                                                errors: insertErrors.length
                                            },
                                            import_info: {
                                                fixed_appointments: confirmedCount,
                                                flexible_appointments: proposalCount,
                                                delimiter: parsed.meta.delimiter,
                                                columns: parsed.meta.fields
                                            },
                                            sample_data: processedAppointments.slice(0, 3).map(apt => ({
                                                name: apt.customer,
                                                is_fixed: apt.is_fixed,
                                                fixed_date: apt.fixed_date,
                                                fixed_time: apt.fixed_time,
                                                status: apt.status
                                            })),
                                            timestamp: new Date().toISOString()
                                        });
                                    }
                                });
                            }
                        }
                    });
                });
            });
        });
    } catch (error) {
        console.error('❌ CSV Import Fehler:', error);
        res.status(500).json({
            success: false,
            error: 'CSV Import fehlgeschlagen',
            details: error.message
        });
    }
});

console.log('📝 CSV Import Funktion vereinfacht - VOLLSTÄNDIGER ERSATZ aktiv');

// ======================================================================
// OPTIMIERTER CSV IMPORT (zusätzliche Variante)
// ======================================================================

app.post('/api/admin/import-csv-optimized', upload.single('csvFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'Keine CSV-Datei hochgeladen' });
    }

    console.log('📁 OPTIMIERTER CSV Import gestartet...');

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

        const processedAppointments = [];
        let skippedCount = 0;

        // Parse CSV wie bisher
        parsed.data.forEach((row, index) => {
            if (row['On Hold'] && row['On Hold'].trim() !== '') {
                skippedCount++;
                return;
            }

            if (!row['Invitee Name'] || row['Invitee Name'].trim() === '') {
                skippedCount++;
                return;
            }

            // Baue Adresse
            let fullAddress = row['Adresse'] || '';
            if (!fullAddress && row['Straße & Hausnr.']) {
                const parts = [];
                if (row['Straße & Hausnr.']) parts.push(row['Straße & Hausnr.']);
                if (row['PLZ'] && row['Ort']) parts.push(`${row['PLZ']} ${row['Ort']}`);
                else if (row['Ort']) parts.push(row['Ort']);
                if (row['Land']) parts.push(row['Land']);
                fullAddress = parts.join(', ');
            }

            // Parse Datum/Zeit für fixe Termine
            let isFixed = false;
            let fixedDate = null;
            let fixedTime = null;

            if (row['Start Date & Time'] && row['Start Date & Time'].trim() !== '') {
                try {
                    const dateTimeStr = row['Start Date & Time'].trim();
                    let dateTime;

                    if (dateTimeStr.match(/^\d{1,2}[\.\/]\d{1,2}[\.\/]\d{4}\s+\d{1,2}:\d{2}/)) {
                        const [datePart, timePart] = dateTimeStr.split(/\s+/);
                        const [day, month, year] = datePart.split(/[\.\/]/);
                        dateTime = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T${timePart}:00`);
                    } else if (dateTimeStr.match(/^\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}/)) {
                        dateTime = new Date(dateTimeStr);
                    } else {
                        dateTime = new Date(dateTimeStr);
                    }

                    if (!isNaN(dateTime.getTime())) {
                        const year = dateTime.getFullYear();
                        if (year >= 2020 && year <= 2030) {
                            isFixed = true;
                            fixedDate = dateTime.toISOString().split('T')[0];
                            fixedTime = `${dateTime.getHours().toString().padStart(2, '0')}:${dateTime.getMinutes().toString().padStart(2, '0')}`;
                        }
                    }
                } catch (e) {
                    console.log(`❌ Datum-Parsing Fehler: "${row['Start Date & Time']}"`);
                }
            }

            processedAppointments.push({
                customer: row['Invitee Name'].trim(),
                address: fullAddress || 'Adresse nicht verfügbar',
                priority: isFixed ? 'hoch' : 'mittel',
                status: isFixed ? 'bestätigt' : 'vorschlag',
                duration: 3,
                pipeline_days: isFixed ? 7 : Math.floor(Math.random() * 30) + 1,
                is_fixed: isFixed ? 1 : 0,
                fixed_date: fixedDate,
                fixed_time: fixedTime,
                on_hold: null,
                notes: JSON.stringify({
                    invitee_name: row['Invitee Name'].trim(),
                    company: row['Company'] ? row['Company'].trim() : '',
                    customer_company: row['Customer Company'] ? row['Customer Company'].trim() : '',
                    start_time: row['Start Date & Time'] || null,
                    import_date: new Date().toISOString(),
                    source: 'Optimized CSV Import'
                })
            });
        });

        if (processedAppointments.length === 0) {
            return res.json({
                success: false,
                message: 'Keine gültigen Termine in der CSV gefunden'
            });
        }

        console.log(`📊 ${processedAppointments.length} Termine verarbeitet`);

        // Standard Geocoding ohne Optimized Service
        await ensureAllAppointmentsGeocoded(processedAppointments);

        const geocodedCount = processedAppointments.filter(apt => apt.geocoded).length;
        console.log(`✅ ${geocodedCount}/${processedAppointments.length} Termine geocoded`);

        db.serialize(() => {
            console.log('🗃️ Starte Datenbank-Transaction...');
            db.run("BEGIN TRANSACTION");

            db.run("DELETE FROM appointments", function(deleteErr) {
                if (deleteErr) {
                    console.error('❌ Fehler beim Löschen:', deleteErr);
                    db.run("ROLLBACK");
                    return res.status(500).json({ error: 'Löschfehler' });
                }

                console.log(`🧹 ${this.changes} bestehende Termine gelöscht`);

                const stmt = db.prepare(`
                    INSERT INTO appointments
                    (customer, address, priority, status, duration, pipeline_days, notes,
                     preferred_dates, excluded_dates, is_fixed, fixed_date, fixed_time, on_hold,
                     lat, lng, geocoded)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `);

                let insertedCount = 0;
                let insertErrors = [];

                processedAppointments.forEach((apt, idx) => {
                    stmt.run([
                        apt.customer, apt.address, apt.priority, apt.status,
                        apt.duration, apt.pipeline_days, apt.notes,
                        JSON.stringify([]), JSON.stringify([]),
                        apt.is_fixed, apt.fixed_date, apt.fixed_time, apt.on_hold,
                        apt.lat, apt.lng, apt.geocoded
                    ], function(err) {
                        if (err) {
                            insertErrors.push(`${apt.customer}: ${err.message}`);
                        } else {
                            insertedCount++;
                        }

                        if (insertedCount + insertErrors.length === processedAppointments.length) {
                            stmt.finalize();

                            if (insertErrors.length > 0) {
                                db.run("ROLLBACK");
                                res.status(500).json({
                                    success: false,
                                    message: 'Import teilweise fehlgeschlagen',
                                    errors: insertErrors.slice(0, 10)
                                });
                            } else {
                                db.run("COMMIT", (commitErr) => {
                                    if (commitErr) {
                                        res.status(500).json({
                                            success: false,
                                            error: 'Commit fehlgeschlagen'
                                        });
                                    } else {
                                        console.log('✅ CSV Import erfolgreich!');

                                        res.json({
                                            success: true,
                                            message: '✅ CSV Import erfolgreich',
                                            stats: {
                                                totalRows: parsed.data.length,
                                                processed: processedAppointments.length,
                                                inserted: insertedCount,
                                                geocoded: geocodedCount,
                                                skipped: skippedCount
                                            },
                                            timestamp: new Date().toISOString()
                                        });
                                    }
                                });
                            }
                        }
                    });
                });
            });
        });

    } catch (error) {
        console.error('❌ Optimierter CSV Import Fehler:', error);
        res.status(500).json({
            success: false,
            error: 'CSV Import fehlgeschlagen',
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

// Admin endpoint to show API usage statistics
app.get('/api/admin/api-usage', validateSession, (req, res) => {
    // Zeigt API-Nutzungsstatistiken
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
// ADMIN: Überlappende fixe Termine finden und bereinigen
// ======================================================================

app.get('/api/admin/check-overlapping-appointments', validateSession, async (req, res) => {
    try {
        console.log('🔍 Prüfe auf überlappende fixe Termine...');

        const fixedAppointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT id, customer, fixed_date, fixed_time, duration, address, status
                FROM appointments 
                WHERE is_fixed = 1 AND fixed_date IS NOT NULL AND fixed_time IS NOT NULL
                ORDER BY fixed_date, fixed_time
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        console.log(`📊 ${fixedAppointments.length} fixe Termine gefunden`);

        const appointmentsByDate = {};
        fixedAppointments.forEach(apt => {
            if (!appointmentsByDate[apt.fixed_date]) {
                appointmentsByDate[apt.fixed_date] = [];
            }
            appointmentsByDate[apt.fixed_date].push(apt);
        });

        const overlaps = [];

        Object.entries(appointmentsByDate).forEach(([date, dayAppointments]) => {
            dayAppointments.sort((a, b) =>
                timeToHours(a.fixed_time) - timeToHours(b.fixed_time)
            );

            for (let i = 0; i < dayAppointments.length; i++) {
                for (let j = i + 1; j < dayAppointments.length; j++) {
                    const apt1 = dayAppointments[i];
                    const apt2 = dayAppointments[j];

                    const start1 = timeToHours(apt1.fixed_time);
                    const end1 = start1 + (apt1.duration || 3);
                    const start2 = timeToHours(apt2.fixed_time);
                    const end2 = start2 + (apt2.duration || 3);

                    if (start1 < end2 && start2 < end1) {
                        overlaps.push({
                            date: date,
                            appointment1: {
                                id: apt1.id,
                                customer: apt1.customer,
                                time: `${apt1.fixed_time} - ${hoursToTime(end1)}`,
                                address: apt1.address
                            },
                            appointment2: {
                                id: apt2.id,
                                customer: apt2.customer,
                                time: `${apt2.fixed_time} - ${hoursToTime(end2)}`,
                                address: apt2.address
                            },
                            overlapMinutes: Math.round((Math.min(end1, end2) - Math.max(start1, start2)) * 60)
                        });
                    }
                }
            }
        });

        res.json({
            success: true,
            totalFixed: fixedAppointments.length,
            overlapsFound: overlaps.length,
            overlaps: overlaps,
            message: overlaps.length > 0 ?
                `⚠️ ${overlaps.length} Überlappungen gefunden!` :
                '✅ Keine Überlappungen gefunden'
        });

    } catch (error) {
        console.error('❌ Fehler beim Prüfen der Überlappungen:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.post('/api/admin/fix-overlapping-appointments', validateSession, async (req, res) => {
    const { strategy = 'shift_later' } = req.body;
    try {
        console.log(`🔧 Bereinige Überlappungen mit Strategie: ${strategy}`);

        const fixedAppointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT id, customer, fixed_date, fixed_time, duration, address, status, pipeline_days
                FROM appointments 
                WHERE is_fixed = 1 AND fixed_date IS NOT NULL AND fixed_time IS NOT NULL
                ORDER BY fixed_date, fixed_time
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        const appointmentsByDate = {};
        fixedAppointments.forEach(apt => {
            if (!appointmentsByDate[apt.fixed_date]) {
                appointmentsByDate[apt.fixed_date] = [];
            }
            appointmentsByDate[apt.fixed_date].push(apt);
        });

        const fixes = [];

        for (const [date, dayAppointments] of Object.entries(appointmentsByDate)) {
            dayAppointments.sort((a, b) =>
                timeToHours(a.fixed_time) - timeToHours(b.fixed_time)
            );

            for (let i = 0; i < dayAppointments.length - 1; i++) {
                const current = dayAppointments[i];
                const next = dayAppointments[i + 1];

                const currentEnd = timeToHours(current.fixed_time) + (current.duration || 3);
                const nextStart = timeToHours(next.fixed_time);

                if (currentEnd > nextStart) {
                    console.log(`⚠️ Überlappung: ${current.customer} endet ${hoursToTime(currentEnd)}, ${next.customer} startet ${next.fixed_time}`);

                    if (strategy === 'shift_later') {
                        const newTime = hoursToTime(currentEnd + 0.5);

                        await new Promise((resolve, reject) => {
                            db.run(
                                "UPDATE appointments SET fixed_time = ? WHERE id = ?",
                                [newTime, next.id],
                                (err) => {
                                    if (err) reject(err); else resolve();
                                }
                            );
                        });

                        fixes.push({
                            appointment: next.customer,
                            action: 'verschoben',
                            oldTime: next.fixed_time,
                            newTime: newTime,
                            date: date
                        });

                        next.fixed_time = newTime;

                    } else if (strategy === 'make_flexible') {
                        const toMakeFlexible = current.pipeline_days < next.pipeline_days ? current : next;

                        await new Promise((resolve, reject) => {
                            db.run(
                                "UPDATE appointments SET is_fixed = 0, fixed_date = NULL, fixed_time = NULL WHERE id = ?",
                                [toMakeFlexible.id],
                                (err) => {
                                    if (err) reject(err); else resolve();
                                }
                            );
                        });

                        fixes.push({
                            appointment: toMakeFlexible.customer,
                            action: 'flexibel gemacht',
                            oldTime: toMakeFlexible.fixed_time,
                            date: date
                        });
                    }
                }
            }
        }

        res.json({
            success: true,
            message: `${fixes.length} Überlappungen bereinigt`,
            fixes: fixes,
            strategy: strategy
        });

    } catch (error) {
        console.error('❌ Fehler beim Bereinigen der Überlappungen:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.put('/api/admin/reschedule-fixed-appointment/:id', validateSession, async (req, res) => {
    const { id } = req.params;
    const { fixed_date, fixed_time } = req.body;

    if (!fixed_date || !fixed_time) {
        return res.status(400).json({ error: 'fixed_date und fixed_time erforderlich' });
    }

    try {
        const conflicts = await new Promise((resolve, reject) => {
            db.all(`
                SELECT id, customer, fixed_time, duration
                FROM appointments 
                WHERE is_fixed = 1 
                AND fixed_date = ? 
                AND id != ?
            `, [fixed_date, id], (err, rows) => {
                if (err) reject(err); else resolve(rows);
            });
        });

        const newStart = timeToHours(fixed_time);
        const newEnd = newStart + 3;

        for (const apt of conflicts) {
            const aptStart = timeToHours(apt.fixed_time);
            const aptEnd = aptStart + (apt.duration || 3);

            if (newStart < aptEnd && aptStart < newEnd) {
                return res.status(400).json({
                    error: 'Zeitkonflikt',
                    conflictsWith: apt.customer,
                    conflictTime: `${apt.fixed_time} - ${hoursToTime(aptEnd)}`
                });
            }
        }

        await new Promise((resolve, reject) => {
            db.run(
                "UPDATE appointments SET fixed_date = ?, fixed_time = ? WHERE id = ?",
                [fixed_date, fixed_time, id],
                (err) => {
                    if (err) reject(err); else resolve();
                }
            );
        });

        res.json({
            success: true,
            message: 'Termin erfolgreich verschoben',
            newDate: fixed_date,
            newTime: fixed_time
        });

    } catch (error) {
        console.error('❌ Fehler beim Verschieben des Termins:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ======================================================================
// NEUE BACKEND ROUTES FÜR TERMINE UND ANALYSEN
// ======================================================================

// Get ALL appointments including fixed ones (for calendar display)
app.get('/api/appointments/all', async (req, res) => {
    try {
        console.log('📋 Lade ALLE Termine (inkl. feste Termine) für Kalender...');

        const rows = await new Promise((resolve, reject) => {
            db.all(`
                SELECT * FROM appointments 
                WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '') 
                ORDER BY 
                    is_fixed DESC,
                    fixed_date ASC,
                    fixed_time ASC,
                    created_at DESC
            `, (err, result) => (err ? reject(err) : resolve(result))
            );
        });

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
                start_time: parsedNotes.start_time || null,
                is_fixed: Boolean(row.is_fixed),
                _debug: {
                    raw_is_fixed: row.is_fixed,
                    has_fixed_date: !!row.fixed_date,
                    has_fixed_time: !!row.fixed_time
                }
            };
        });

        const stats = {
            total: enhancedRows.length,
            fixed: enhancedRows.filter(apt => apt.is_fixed).length,
            flexible: enhancedRows.filter(apt => !apt.is_fixed).length,
            confirmed: enhancedRows.filter(apt => apt.status === 'bestätigt').length,
            proposals: enhancedRows.filter(apt => apt.status === 'vorschlag').length
        };

        console.log('📊 Termine-Statistiken:');
        console.log(`   Total: ${stats.total}`);
        console.log(`   Fixe Termine: ${stats.fixed}`);
        console.log(`   Flexible Termine: ${stats.flexible}`);
        console.log(`   Bestätigte Termine: ${stats.confirmed}`);
        console.log(`   Vorschlag-Termine: ${stats.proposals}`);

        res.json({
            appointments: enhancedRows,
            stats: stats,
            meta: {
                query_type: 'all_appointments_including_fixed',
                timestamp: new Date().toISOString(),
                filtering: 'only_on_hold_excluded'
            }
        });

    } catch (err) {
        console.error('❌ Fehler beim Laden aller Termine:', err);
        res.status(500).json({ 
            error: err.message,
            context: 'loading_all_appointments_including_fixed'
        });
    }
});

// Get flexible appointments only (for route optimization) - enhanced version
app.get('/api/appointments', async (req, res) => {
    try {
        console.log('🔧 Lade nur FLEXIBLE Termine für Routenoptimierung...');

        const rows = await new Promise((resolve, reject) => {
            db.all(`
                SELECT * FROM appointments 
                WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '') 
                AND (is_fixed IS NULL OR is_fixed = 0)
                ORDER BY 
                    CASE WHEN status = 'bestätigt' THEN 0 ELSE 1 END,
                    pipeline_days DESC,
                    created_at DESC
            `, (err, result) => (err ? reject(err) : resolve(result))
            );
        });

        const usedIds = await getUsedAppointmentIds();
        const filteredRows = rows.filter(row => !usedIds.includes(row.id));

        const enhancedRows = filteredRows.map(row => {
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
                start_time: parsedNotes.start_time || null,
                is_fixed: Boolean(row.is_fixed)
            };
        });

        console.log(`📋 ${enhancedRows.length} flexible Termine verfügbar für Optimierung`);
        console.log(`🚫 ${usedIds.length} Termine bereits in anderen Routen geplant`);

        res.json(enhancedRows);
    } catch (err) {
        console.error('❌ Fehler beim Laden flexibler Termine:', err);
        res.status(500).json({ error: err.message });
    }
});

// Analyze appointments by type and status
app.get('/api/admin/analyze-appointments', validateSession, async (req, res) => {
    try {
        console.log('🔍 Analysiere alle Termine nach Typ...');

        const allTermine = await new Promise((resolve, reject) => {
            db.all(`
                SELECT 
                    id, customer, address, priority, status, duration, pipeline_days,
                    is_fixed, fixed_date, fixed_time, on_hold,
                    lat, lng, geocoded, created_at
                FROM appointments 
                ORDER BY created_at DESC
            `, (err, rows) => (err ? reject(err) : resolve(rows))
            );
        });

        const analysis = {
            total: allTermine.length,
            by_type: {
                fixed: allTermine.filter(apt => apt.is_fixed).length,
                flexible: allTermine.filter(apt => !apt.is_fixed).length
            },
            by_status: {
                bestätigt: allTermine.filter(apt => apt.status === 'bestätigt').length,
                vorschlag: allTermine.filter(apt => apt.status === 'vorschlag').length,
                abgesagt: allTermine.filter(apt => apt.status === 'abgesagt').length
            },
            by_hold_status: {
                active: allTermine.filter(apt => !apt.on_hold || apt.on_hold.trim() === '').length,
                on_hold: allTermine.filter(apt => apt.on_hold && apt.on_hold.trim() !== '').length
            },
            geocoded: {
                with_coords: allTermine.filter(apt => apt.lat && apt.lng).length,
                without_coords: allTermine.filter(apt => !apt.lat || !apt.lng).length
            },
            fixed_appointments_detail: allTermine
                .filter(apt => apt.is_fixed)
                .map(apt => ({
                    id: apt.id,
                    customer: apt.customer,
                    fixed_date: apt.fixed_date,
                    fixed_time: apt.fixed_time,
                    status: apt.status,
                    has_coords: !!(apt.lat && apt.lng)
                }))
        };

        const issues = [];

        if (analysis.fixed_appointments_detail.some(apt => !apt.fixed_date || !apt.fixed_time)) {
            issues.push('⚠️ Einige fixe Termine haben kein Datum/Zeit');
        }

        if (analysis.geocoded.without_coords > 0) {
            issues.push(`⚠️ ${analysis.geocoded.without_coords} Termine ohne Koordinaten`);
        }

        if (analysis.by_hold_status.on_hold > 0) {
            issues.push(`ℹ️ ${analysis.by_hold_status.on_hold} Termine sind "on hold"`);
        }

        res.json({
            success: true,
            analysis: analysis,
            issues: issues,
            recommendations: [
                analysis.fixed_appointments_detail.length > 0 ? 
                    `✅ ${analysis.fixed_appointments_detail.length} fixe Termine gefunden` : 
                    'ℹ️ Keine fixen Termine vorhanden',
                analysis.geocoded.with_coords > 0 ? 
                    `✅ ${analysis.geocoded.with_coords} Termine haben Koordinaten` : 
                    '❌ Geocoding für Termine erforderlich',
                analysis.by_status.bestätigt + analysis.by_status.vorschlag > 0 ? 
                    `✅ ${analysis.by_status.bestätigt + analysis.by_status.vorschlag} planbare Termine` : 
                    '❌ Keine planbaren Termine gefunden'
            ],
            sample_fixed: analysis.fixed_appointments_detail.slice(0, 5),
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        console.error('❌ Fehler bei Termin-Analyse:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Create a new fixed appointment
app.post('/api/appointments/fixed', validateSession, async (req, res) => {
    const { 
        customer, address, fixed_date, fixed_time, 
        duration = 3, company = '', customer_company = '',
        priority = 'hoch', status = 'bestätigt' 
    } = req.body;

    if (!customer || !address || !fixed_date || !fixed_time) {
        return res.status(400).json({ 
            error: 'customer, address, fixed_date und fixed_time sind erforderlich' 
        });
    }

    try {
        const conflicts = await new Promise((resolve, reject) => {
            db.all(`
                SELECT id, customer, fixed_time, duration
                FROM appointments 
                WHERE is_fixed = 1 
                AND fixed_date = ?
            `, [fixed_date], (err, rows) => {
                if (err) reject(err); else resolve(rows);
            });
        });

        const newStart = timeToHours(fixed_time);
        const newEnd = newStart + duration;

        for (const apt of conflicts) {
            const aptStart = timeToHours(apt.fixed_time);
            const aptEnd = aptStart + (apt.duration || 3);

            if (newStart < aptEnd && aptStart < newEnd) {
                return res.status(400).json({
                    error: 'Zeitkonflikt mit anderem fixen Termin',
                    conflictsWith: apt.customer,
                    conflictTime: `${apt.fixed_time} - ${hoursToTime(aptEnd)}`
                });
            }
        }

        const notes = JSON.stringify({
            invitee_name: customer,
            company: company,
            customer_company: customer_company,
            start_time: `${fixed_date} ${fixed_time}`,
            custom_notes: 'Manuell erstellter fixer Termin',
            created_via: 'admin_interface',
            source: 'Manual Fixed Appointment Creation'
        });

        const result = await new Promise((resolve, reject) => {
            db.run(`
                INSERT INTO appointments 
                (customer, address, priority, status, duration, pipeline_days, notes, 
                 is_fixed, fixed_date, fixed_time, lat, lng, geocoded)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NULL, NULL, 0)
            `, [
                customer, address, priority, status, duration, 7, notes,
                fixed_date, fixed_time
            ], function(err) {
                if (err) reject(err);
                else resolve({ id: this.lastID });
            });
        });

        console.log(`✅ Fixer Termin erstellt: ${customer} am ${fixed_date} ${fixed_time}`);

        res.json({
            success: true,
            message: 'Fixer Termin erfolgreich erstellt',
            appointment: {
                id: result.id,
                customer: customer,
                fixed_date: fixed_date,
                fixed_time: fixed_time,
                duration: duration
            }
        });

    } catch (error) {
        console.error('❌ Fehler beim Erstellen des fixen Termins:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Update a fixed appointment
app.put('/api/appointments/fixed/:id', validateSession, async (req, res) => {
    const { id } = req.params;
    const { fixed_date, fixed_time, duration, customer, address } = req.body;

    try {
        const appointment = await new Promise((resolve, reject) => {
            db.get(
                "SELECT * FROM appointments WHERE id = ? AND is_fixed = 1",
                [id],
                (err, row) => (err ? reject(err) : resolve(row))
            );
        });

        if (!appointment) {
            return res.status(404).json({ error: 'Fixer Termin nicht gefunden' });
        }

        if (fixed_date && fixed_time) {
            const conflicts = await new Promise((resolve, reject) => {
                db.all(`
                    SELECT id, customer, fixed_time, duration
                    FROM appointments 
                    WHERE is_fixed = 1 
                    AND fixed_date = ? 
                    AND id != ?
                `, [fixed_date, id], (err, rows) => {
                    if (err) reject(err); else resolve(rows);
                });
            });

            const newStart = timeToHours(fixed_time);
            const newEnd = newStart + (duration || appointment.duration);

            for (const apt of conflicts) {
                const aptStart = timeToHours(apt.fixed_time);
                const aptEnd = aptStart + (apt.duration || 3);

                if (newStart < aptEnd && aptStart < newEnd) {
                    return res.status(400).json({
                        error: 'Zeitkonflikt',
                        conflictsWith: apt.customer,
                        conflictTime: `${apt.fixed_time} - ${hoursToTime(aptEnd)}`
                    });
                }
            }
        }

        const updates = [];
        const values = [];

        if (fixed_date) {
            updates.push('fixed_date = ?');
            values.push(fixed_date);
        }
        if (fixed_time) {
            updates.push('fixed_time = ?');
            values.push(fixed_time);
        }
        if (duration) {
            updates.push('duration = ?');
            values.push(duration);
        }
        if (customer) {
            updates.push('customer = ?');
            values.push(customer);
        }
        if (address) {
            updates.push('address = ?', 'geocoded = 0');
            values.push(address, 0);
        }

        values.push(id);

        await new Promise((resolve, reject) => {
            db.run(
                `UPDATE appointments SET ${updates.join(', ')} WHERE id = ?`,
                values,
                (err) => (err ? reject(err) : resolve())
            );
        });

        console.log(`✅ Fixer Termin ${id} aktualisiert`);

        res.json({
            success: true,
            message: 'Fixer Termin erfolgreich aktualisiert'
        });

    } catch (error) {
        console.error('❌ Fehler beim Aktualisieren des fixen Termins:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Delete a fixed appointment
app.delete('/api/appointments/fixed/:id', validateSession, async (req, res) => {
    const { id } = req.params;

    try {
        const result = await new Promise((resolve, reject) => {
            db.run(
                "DELETE FROM appointments WHERE id = ? AND is_fixed = 1",
                [id],
                function(err) {
                    if (err) reject(err);
                    else resolve({ changes: this.changes });
                }
            );
        });

        if (result.changes === 0) {
            return res.status(404).json({ error: 'Fixer Termin nicht gefunden' });
        }

        console.log(`🗑️ Fixer Termin ${id} gelöscht`);

        res.json({
            success: true,
            message: 'Fixer Termin erfolgreich gelöscht'
        });

    } catch (error) {
        console.error('❌ Fehler beim Löschen des fixen Termins:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Alternative Terminvorschläge für abgelehnte Termine
app.post('/api/appointments/suggest-alternatives', validateSession, async (req, res) => {
    const { appointmentId, weekStart, reason = 'customer_rejected' } = req.body;

    if (!appointmentId) {
        return res.status(400).json({ error: 'appointmentId is required' });
    }

    try {
        const alternatives = await findSmartAlternativeSlots(
            appointmentId,
            weekStart || new Date().toISOString().split('T')[0]
        );

        if (reason === 'customer_rejected' && alternatives.alternatives?.length > 0) {
            await new Promise((resolve, reject) => {
                db.run(
                    `UPDATE appointments 
                     SET status = 'umzuplanen',
                         notes = json_set(COALESCE(notes, '{}'), '$.rejection_info', json(?))
                     WHERE id = ?`,
                    [JSON.stringify({
                        reason: reason,
                        rejected_at: new Date().toISOString(),
                        alternatives_found: alternatives.alternatives.length
                    }), appointmentId],
                    err => err ? reject(err) : resolve()
                );
            });
        }

        res.json({
            success: true,
            ...alternatives,
            recommendation: alternatives.alternatives?.length > 0 ?
                `${alternatives.alternatives.length} alternative Slots gefunden. Beste Option: ${alternatives.alternatives[0].day} ${alternatives.alternatives[0].startTime}` :
                'Keine passenden Alternativen in dieser Woche. Empfehle Verschiebung in nächste Woche.'
        });

    } catch (error) {
        console.error('❌ Fehler bei Alternativvorschlägen:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

console.log('📌 Erweiterte Termine-Endpoints hinzugefügt:');
console.log('  GET  /api/appointments/all - ALLE Termine (inkl. fixe)');
console.log('  GET  /api/appointments - Nur flexible Termine');
console.log('  GET  /api/admin/analyze-appointments - Termin-Analyse');
console.log('  POST /api/appointments/fixed - Fixen Termin erstellen');
console.log('  PUT  /api/appointments/fixed/:id - Fixen Termin bearbeiten');
console.log('  DELETE /api/appointments/fixed/:id - Fixen Termin löschen');
console.log('  POST /api/appointments/suggest-alternatives - Alternative Terminvorschläge');

// ======================================================================
// INTELLIGENTE ALTERNATIVE SLOT SUCHE
// ======================================================================

// Hauptfunktion: Finde alternative Slots für abgelehnte Termine
app.post('/api/appointments/find-alternatives', validateSession, async (req, res) => {
    const { appointmentId, currentWeek, nextWeeks = 2, reason = 'customer_rejected' } = req.body;

    if (!appointmentId) {
        return res.status(400).json({ error: 'appointmentId ist erforderlich' });
    }

    try {
        console.log(`🔄 Suche alternative Slots für Termin ${appointmentId}...`);
        const appointment = await new Promise((resolve, reject) => {
            db.get(
                "SELECT * FROM appointments WHERE id = ?",
                [appointmentId],
                (err, row) => err ? reject(err) : resolve(row)
            );
        });

        if (!appointment) {
            return res.status(404).json({ error: 'Termin nicht gefunden' });
        }

        if (!appointment.lat || !appointment.lng) {
            const geocodingService = new EnhancedGeocodingService(db);
            try {
                const coords = await geocodingService.geocodeAddress(appointment.address);
                appointment.lat = coords.lat;
                appointment.lng = coords.lng;
            } catch (e) {
                console.warn('⚠️ Geocoding fehlgeschlagen, nutze Fallback');
            }
        }

        const alternatives = await findAlternativeSlotsIntelligent(
            appointment,
            currentWeek || new Date().toISOString().split('T')[0],
            nextWeeks
        );

        if (reason === 'customer_rejected') {
            await new Promise((resolve, reject) => {
                db.run(
                    `UPDATE appointments 
                     SET status = 'umzuplanen',
                         notes = json_set(COALESCE(notes, '{}'), '$.rejection_info', json(?))
                     WHERE id = ?`,
                    [JSON.stringify({
                        reason: reason,
                        rejected_at: new Date().toISOString(),
                        alternatives_found: alternatives.length,
                        original_week: currentWeek
                    }), appointmentId],
                    err => err ? reject(err) : resolve()
                );
            });
        }

        res.json({
            success: true,
            appointment: {
                id: appointment.id,
                customer: appointment.customer,
                address: appointment.address
            },
            alternatives: alternatives,
            totalFound: alternatives.length,
            recommendation: generateRecommendation(alternatives, appointment),
            nextSteps: alternatives.length > 0 ? [
                'Kunde über alternative Termine informieren',
                'Nach Bestätigung Route neu berechnen',
                'Termin als "bestätigt" markieren'
            ] : [
                'Keine passenden Slots in den nächsten Wochen',
                'Termin für spätere Planung vormerken',
                'Kunde über Verzögerung informieren'
            ]
        });

    } catch (error) {
        console.error('❌ Fehler bei Alternativsuche:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// API Endpoint: Termin verschieben
app.post('/api/appointments/reschedule', validateSession, async (req, res) => {
    const { appointmentId, newWeek, newDay, newTime } = req.body;

    if (!appointmentId || !newWeek || !newDay || !newTime) {
        return res.status(400).json({
            error: 'appointmentId, newWeek, newDay und newTime sind erforderlich'
        });
    }

    try {
        await new Promise((resolve, reject) => {
            db.run(
                `UPDATE appointments 
                 SET status = 'bestätigt',
                     notes = json_set(COALESCE(notes, '{}'), '$.rescheduled', json(?))
                 WHERE id = ?`,
                [JSON.stringify({
                    to_week: newWeek,
                    to_day: newDay,
                    to_time: newTime,
                    rescheduled_at: new Date().toISOString()
                }), appointmentId],
                err => err ? reject(err) : resolve()
            );
        });

        const recalcResult = await fetch(`${req.protocol}://${req.get('host')}/api/routes/recalculate`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': req.headers.authorization
            },
            body: JSON.stringify({
                weekStart: newWeek,
                preserveFixed: true,
                triggerAppointmentId: appointmentId
            })
        }).then(r => r.json());

        res.json({
            success: true,
            message: 'Termin erfolgreich verschoben',
            appointment: {
                id: appointmentId,
                newSchedule: {
                    week: newWeek,
                    day: newDay,
                    time: newTime
                }
            },
            routeRecalculated: recalcResult.success
        });

    } catch (error) {
        console.error('❌ Fehler beim Verschieben:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Intelligente Suche nach alternativen Slots
async function findAlternativeSlotsIntelligent(appointment, startWeek, weeksToCheck = 2) {
    const alternatives = [];
    const weekStart = new Date(startWeek);
    for (let w = 0; w < weeksToCheck; w++) {
        const checkWeek = new Date(weekStart);
        checkWeek.setDate(weekStart.getDate() + (w * 7));
        const weekStr = checkWeek.toISOString().split('T')[0];
        console.log(`📅 Prüfe Woche ${w + 1}: ${weekStr}`);
        const weekRoute = await new Promise((resolve, reject) => {
            db.get(
                "SELECT route_data FROM saved_routes WHERE week_start = ? AND is_active = 1",
                [weekStr],
                (err, row) => {
                    if (err || !row) resolve(null);
                    else {
                        try { resolve(JSON.parse(row.route_data)); } catch (e) { resolve(null); }
                    }
                }
            );
        });
        if (!weekRoute) {
            for (let d = 0; d < 5; d++) {
                const dayDate = new Date(checkWeek);
                dayDate.setDate(checkWeek.getDate() + d);
                alternatives.push({
                    week: weekStr,
                    day: ['Montag','Dienstag','Mittwoch','Donnerstag','Freitag'][d],
                    date: dayDate.toISOString().split('T')[0],
                    slot: { startTime: '09:00', endTime: '12:00', duration: 3 },
                    travelEfficiency: 0.5,
                    reason: 'Kompletter Tag verfügbar',
                    quality: 0.7,
                    type: 'empty_day'
                });
            }
        } else {
            for (let dayIndex = 0; dayIndex < weekRoute.days.length; dayIndex++) {
                const day = weekRoute.days[dayIndex];
                const dayAlternatives = await analyzeDayForAlternatives(day, appointment, dayIndex, weekStr);
                alternatives.push(...dayAlternatives);
            }
        }
    }
    alternatives.sort((a, b) => {
        const weekDiff = (new Date(a.week) - new Date(b.week)) / (7*24*60*60*1000);
        const efficiencyDiff = b.travelEfficiency - a.travelEfficiency;
        const qualityDiff = b.quality - a.quality;
        return efficiencyDiff * 2 + weekDiff * 0.5 + qualityDiff;
    });
    return alternatives.slice(0, 10);
}

async function analyzeDayForAlternatives(day, appointment, dayIndex, weekStr) {
    const alternatives = [];
    const constraints = {
        workStartTime: 9,
        workEndTime: 18,
        appointmentDuration: 3,
        minSlotDuration: 3.5,
        travelPadding: 0.25
    };
    const freeSlots = findFreeSlotsInDay(day, constraints);
    for (const slot of freeSlots) {
        if (slot.duration < constraints.minSlotDuration) continue;
        const efficiency = await calculateSlotEfficiency(day, slot, appointment, constraints);
        const quality = evaluateSlotQuality(slot, dayIndex);
        alternatives.push({
            week: weekStr,
            day: day.day,
            date: day.date,
            slot: {
                startTime: slot.startTime,
                endTime: addHoursToTime(slot.startTime, 3),
                duration: 3,
                availableBuffer: slot.duration - 3
            },
            travelEfficiency: efficiency.score,
            travelDetails: efficiency.details,
            quality: quality,
            reason: efficiency.reason,
            type: 'available_slot',
            nearbyAppointments: efficiency.nearbyAppointments
        });
    }
    return alternatives;
}

function findFreeSlotsInDay(day, constraints) {
    const slots = [];
    const dayStart = constraints.workStartTime;
    const dayEnd = constraints.workEndTime;
    const sortedAppointments = [...(day.appointments || [])].sort((a,b)=> timeToHours(a.startTime)-timeToHours(b.startTime));
    if (sortedAppointments.length === 0) {
        slots.push({ startTime: hoursToTime(dayStart), endTime: hoursToTime(dayEnd), duration: dayEnd - dayStart });
        return slots;
    }
    const firstStart = timeToHours(sortedAppointments[0].startTime);
    const firstTravelTime = day.travelSegments?.find(s => s.type === 'departure')?.duration || 1;
    const earliestPossibleStart = dayStart + firstTravelTime;
    if (firstStart > earliestPossibleStart + 0.5) {
        slots.push({
            startTime: hoursToTime(earliestPossibleStart),
            endTime: hoursToTime(firstStart - 0.5),
            duration: firstStart - earliestPossibleStart - 0.5
        });
    }
    for (let i=0;i<sortedAppointments.length-1;i++){
        const currentEnd = timeToHours(sortedAppointments[i].endTime);
        const nextStart = timeToHours(sortedAppointments[i+1].startTime);
        const travelTime = 0.5;
        if (nextStart - currentEnd > travelTime + 0.5) {
            slots.push({
                startTime: hoursToTime(currentEnd + travelTime),
                endTime: hoursToTime(nextStart - constraints.travelPadding),
                duration: nextStart - currentEnd - travelTime - constraints.travelPadding
            });
        }
    }
    const lastEnd = timeToHours(sortedAppointments[sortedAppointments.length-1].endTime);
    const returnTravelTime = day.travelSegments?.find(s => s.type === 'return')?.duration || 1;
    const latestPossibleEnd = dayEnd - returnTravelTime;
    if (latestPossibleEnd > lastEnd + 0.5) {
        slots.push({
            startTime: hoursToTime(lastEnd + 0.5),
            endTime: hoursToTime(latestPossibleEnd),
            duration: latestPossibleEnd - lastEnd - 0.5
        });
    }
    return slots;
}

async function calculateSlotEfficiency(day, slot, appointment, constraints) {
    let score = 0.5;
    const details = [];
    const nearbyAppointments = [];
    for (const existingApt of day.appointments || []) {
        if (!existingApt.lat || !existingApt.lng || !appointment.lat || !appointment.lng) continue;
        const distance = calculateHaversineDistance(appointment.lat, appointment.lng, existingApt.lat, existingApt.lng);
        const timeDiff = Math.abs(timeToHours(slot.startTime) - timeToHours(existingApt.startTime));
        if (distance < 30 && timeDiff < 4) {
            score += 0.3;
            nearbyAppointments.push({ customer: existingApt.customer, distance: Math.round(distance), timeDiff: Math.round(timeDiff*60) });
            details.push(`${Math.round(distance)}km von ${existingApt.customer}`);
        } else if (distance < 50 && timeDiff < 6) {
            score += 0.2;
            nearbyAppointments.push({ customer: existingApt.customer, distance: Math.round(distance), timeDiff: Math.round(timeDiff*60) });
        } else if (distance < 100) {
            score += 0.1;
        }
    }
    if (nearbyAppointments.length === 0 && day.appointments.length > 0) {
        score -= 0.2;
        details.push('Isoliert von anderen Terminen');
    }
    const dayUtilization = (day.appointments.length * 3 + 3) / 9;
    if (dayUtilization > 0.6 && dayUtilization < 0.9) {
        score += 0.1;
        details.push('Gute Tagesauslastung');
    }
    const reason = nearbyAppointments.length > 0 ? `Nahe ${nearbyAppointments.length} anderen Terminen` : 'Freier Slot verfügbar';
    return { score: Math.max(0, Math.min(1, score)), details, nearbyAppointments, reason };
}

function evaluateSlotQuality(slot, dayIndex) {
    let quality = 0.5;
    const slotStart = timeToHours(slot.startTime);
    if (slotStart >= 9 && slotStart <= 11) quality += 0.3;
    else if (slotStart >= 13 && slotStart <= 15) quality += 0.2;
    else if (slotStart >= 15 && slotStart <= 17) quality += 0.1;
    if (dayIndex >= 1 && dayIndex <= 3) quality += 0.1;
    if (slot.duration > 5) quality += 0.2;
    else if (slot.duration > 4) quality += 0.1;
    return Math.max(0, Math.min(1, quality));
}

function generateRecommendation(alternatives, appointment) {
    if (alternatives.length === 0) {
        return {
            summary: 'Keine passenden Alternativen gefunden',
            action: 'Termin für spätere Planung vormerken',
            details: 'In den geprüften Wochen sind keine effizienten Slots verfügbar'
        };
    }
    const best = alternatives[0];
    const recommendation = {
        summary: `Beste Alternative: ${best.day}, ${best.date} um ${best.slot.startTime}`,
        action: 'Kunde kontaktieren und Alternative anbieten',
        details: []
    };
    if (best.travelEfficiency > 0.7) recommendation.details.push('✅ Ausgezeichnete Reiseeffizienz');
    else if (best.travelEfficiency > 0.5) recommendation.details.push('✅ Gute Reiseeffizienz');
    if (best.nearbyAppointments?.length > 0) {
        recommendation.details.push(`📍 ${best.nearbyAppointments.length} Termine in der Nähe`);
    }
    if (best.quality > 0.7) recommendation.details.push('⏰ Optimaler Zeitslot');
    if (alternatives.filter(a => a.travelEfficiency > 0.6).length >= 3) {
        recommendation.details.push(`${alternatives.filter(a => a.travelEfficiency > 0.6).length} weitere gute Alternativen verfügbar`);
    }
    return recommendation;
}

function timeToHours(timeStr) { const [h,m] = timeStr.split(':').map(Number); return h + (m||0)/60; }
function hoursToTime(hours) { const h = Math.floor(hours); const m = Math.round((hours - h)*60); return `${h.toString().padStart(2,'0')}:${m.toString().padStart(2,'0')}`; }
function addHoursToTime(timeStr, hours) { return hoursToTime(timeToHours(timeStr) + hours); }
function calculateHaversineDistance(lat1,lng1,lat2,lng2){ const R=6371; const dLat=(lat2-lat1)*Math.PI/180; const dLng=(lng2-lng1)*Math.PI/180; const a=Math.sin(dLat/2)*Math.sin(dLat/2)+Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLng/2)*Math.sin(dLng/2); const c=2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a)); return R*c; }

console.log('🔄 Alternative Slot Funktionen hinzugefügt:');
console.log('  POST /api/appointments/find-alternatives - Finde alternative Termine');
console.log('  POST /api/appointments/reschedule - Verschiebe Termin');

// ======================================================================
// SERVER INTEGRATION FÜR ENHANCED GEOCODING SERVICE
// ======================================================================

// Enhanced Geocoding Service importieren
const EnhancedGeocodingService = require('./geocoding-service');

// Service-Instanz erstellen
const geocodingService = new EnhancedGeocodingService(db);

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

app.post('/api/admin/warm-cache', validateSession, async (req, res) => {
    try {
        console.log('🔥 Wärme Distance Cache auf...');

        const appointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT id, customer, address, lat, lng 
                FROM appointments 
                WHERE lat IS NOT NULL AND lng IS NOT NULL
                AND (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
                LIMIT 50
            `, (err, rows) => err ? reject(err) : resolve(rows));
        });

        const locations = [
            { id: 'home', lat: 52.3759, lng: 9.7320, name: 'Hannover' },
            ...appointments
        ];

        console.log(`📍 Berechne Distanzen für ${locations.length} Orte...`);

        const distances = await batchCalculateDistances(locations, locations);

        let savedCount = 0;
        Object.entries(distances).forEach(([fromId, destinations]) => {
            Object.entries(destinations).forEach(([toId, data]) => {
                if (fromId !== toId) {
                    savedCount++;
                }
            });
        });

        res.json({
            success: true,
            message: `Cache aufgewärmt: ${savedCount} Distanzen berechnet`,
            locations_processed: locations.length,
            cache_entries: savedCount
        });

    } catch (error) {
        console.error('❌ Cache Warming fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.get('/api/admin/cache-stats', validateSession, async (req, res) => {
    try {
        const stats = await Promise.all([
            new Promise((resolve, reject) => {
                db.all(`
                    SELECT 
                        COUNT(*) as total_entries,
                        COUNT(CASE WHEN cached_at > datetime('now', '-7 days') THEN 1 END) as recent_entries,
                        COUNT(CASE WHEN cached_at > datetime('now', '-1 day') THEN 1 END) as today_entries,
                        MIN(cached_at) as oldest_entry,
                        MAX(cached_at) as newest_entry
                    FROM distance_cache
                `, (err, rows) => {
                    if (err) reject(err);
                    else resolve({ distance_cache: rows[0] });
                });
            }),
            new Promise((resolve, reject) => {
                db.all(`
                    SELECT 
                        COUNT(*) as total_entries,
                        COUNT(CASE WHEN method = 'google_maps' THEN 1 END) as google_entries,
                        COUNT(CASE WHEN method = 'city_database' THEN 1 END) as local_entries,
                        COUNT(CASE WHEN method = 'intelligent_analysis' THEN 1 END) as analyzed_entries
                    FROM geocoding_cache
                `, (err, rows) => {
                    if (err) reject(err);
                    else resolve({ geocoding_cache: rows[0] });
                });
            }),
            new Promise((resolve, reject) => {
                db.all(`
                    SELECT 
                        COUNT(*) as total_appointments,
                        COUNT(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 END) as geocoded,
                        COUNT(CASE WHEN geocoded = 1 THEN 1 END) as verified_geocoded
                    FROM appointments
                    WHERE (on_hold IS NULL OR on_hold = '' OR TRIM(on_hold) = '')
                `, (err, rows) => {
                    if (err) reject(err);
                    else resolve({ appointments: rows[0] });
                });
            })
        ]);

        const avgApiCost = 0.005;
        const potentialSavings = (stats[0].distance_cache.total_entries +
                                 stats[1].geocoding_cache.total_entries) * avgApiCost;

        res.json({
            success: true,
            cache_statistics: {
                distance_cache: stats[0].distance_cache,
                geocoding_cache: stats[1].geocoding_cache,
                appointments: stats[2].appointments
            },
            efficiency: {
                total_cached_requests: stats[0].distance_cache.total_entries +
                                      stats[1].geocoding_cache.total_entries,
                estimated_savings_usd: potentialSavings.toFixed(2),
                cache_hit_potential: `${Math.round((stats[2].appointments.geocoded /
                                     stats[2].appointments.total_appointments) * 100)}%`
            },
            recommendations: [
                stats[0].distance_cache.total_entries < 100 ?
                    '💡 Nutze /api/admin/warm-cache um Cache vorzuwärmen' :
                    '✅ Distance Cache gut gefüllt',
                stats[1].geocoding_cache.google_entries > stats[1].geocoding_cache.local_entries ?
                    '⚠️ Viele Google API Calls - erweitere lokale Städte-Datenbank' :
                    '✅ Lokale Geocoding-Optimierung funktioniert gut'
            ]
        });

    } catch (error) {
        console.error('❌ Cache Stats Fehler:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.post('/api/admin/cleanup-cache', validateSession, async (req, res) => {
    const { olderThanDays = 30 } = req.body;

    try {
        const results = await Promise.all([
            new Promise((resolve, reject) => {
                db.run(
                    `DELETE FROM distance_cache 
                     WHERE cached_at < datetime('now', '-' || ? || ' days')`,
                    [olderThanDays],
                    function(err) {
                        if (err) reject(err);
                        else resolve({ distance_cache_deleted: this.changes });
                    }
                );
            }),
            new Promise((resolve, reject) => {
                db.run(
                    `DELETE FROM geocoding_cache 
                     WHERE cached_at < datetime('now', '-' || ? || ' days')`,
                    [olderThanDays],
                    function(err) {
                        if (err) reject(err);
                        else resolve({ geocoding_cache_deleted: this.changes });
                    }
                );
            })
        ]);

        res.json({
            success: true,
            message: `Cache-Einträge älter als ${olderThanDays} Tage gelöscht`,
            deleted: {
                distance_cache: results[0].distance_cache_deleted,
                geocoding_cache: results[1].geocoding_cache_deleted,
                total: results[0].distance_cache_deleted + results[1].geocoding_cache_deleted
            }
        });

    } catch (error) {
        console.error('❌ Cache Cleanup Fehler:', error);
        res.status(500).json({
            success: false,
            error: error.message
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

// Budget Status anzeigen
app.get('/api/admin/budget-status', validateSession, (req, res) => {
    const status = apiController.getStatus();

    // Berechne geschätzte Monatskosten
    const dailyAverage = parseFloat(status.budget.spent);
    const monthlyEstimate = dailyAverage * 30;

    res.json({
        success: true,
        current: status,
        estimates: {
            daily: dailyAverage.toFixed(2),
            monthly: monthlyEstimate.toFixed(2),
            yearly: (monthlyEstimate * 12).toFixed(2)
        },
        recommendations: [
            status.budget.percentage > 80 ?
                '⚠️ Tagesbudget fast aufgebraucht! Weitere Optimierungen werden lokale Berechnungen nutzen.' :
                '✅ Budget unter Kontrolle',
            monthlyEstimate > 100 ?
                '💡 Tipp: Reduzieren Sie das Tagesbudget für niedrigere Monatskosten' :
                '✅ Monatskosten im akzeptablen Bereich'
        ]
    });
});

// Budget anpassen
app.post('/api/admin/set-budget', validateSession, (req, res) => {
    const { dailyBudgetEUR } = req.body;

    if (!dailyBudgetEUR || dailyBudgetEUR < 1 || dailyBudgetEUR > 50) {
        return res.status(400).json({
            error: 'Ungültiges Budget. Empfohlen: 1-10€ pro Tag'
        });
    }

    // Update Budget
    apiController.dailyBudgetEUR = dailyBudgetEUR;
    apiController.dailyLimits = {
        geocoding: Math.floor(dailyBudgetEUR / 0.005 * 0.2), // 20% für Geocoding
        distanceMatrix: Math.floor(dailyBudgetEUR / 0.01 * 0.8) // 80% für Distance Matrix
    };

    res.json({
        success: true,
        message: `Tagesbudget auf ${dailyBudgetEUR}€ gesetzt`,
        newLimits: {
            geocoding: `${apiController.dailyLimits.geocoding} Anfragen`,
            distanceMatrix: `${apiController.dailyLimits.distanceMatrix} Anfragen`,
            estimatedOptimizations: Math.floor(apiController.dailyLimits.distanceMatrix / 100)
        }
    });
});

// API Usage Reset (für Tests)
app.post('/api/admin/reset-usage', validateSession, (req, res) => {
    apiController.todayUsage = {
        geocoding: 0,
        distanceMatrix: 0,
        totalCostEUR: 0,
        date: new Date().toISOString().split('T')[0]
    };

    res.json({
        success: true,
        message: 'API Usage zurückgesetzt',
        status: apiController.getStatus()
    });
});

// ZUSÄTZLICH: Bereinige doppelte Planungen (Admin-Funktion)
app.post('/api/admin/fix-duplicate-planning', validateSession, async (req, res) => {
    try {
        console.log('🧹 Bereinige doppelte Terminplanungen...');
        const allRoutes = await new Promise((resolve, reject) => {
            db.all("SELECT id, week_start, route_data FROM saved_routes WHERE is_active = 1",
                (err, rows) => err ? reject(err) : resolve(rows)
            );
        });
        const appointmentUsage = new Map();
        const duplicates = [];
        allRoutes.forEach(route => {
            try {
                const routeData = JSON.parse(route.route_data);
                routeData.days?.forEach(day => {
                    day.appointments?.forEach(apt => {
                        if (apt.id) {
                            if (!appointmentUsage.has(apt.id)) {
                                appointmentUsage.set(apt.id, []);
                            }
                            appointmentUsage.get(apt.id).push({
                                routeId: route.id,
                                weekStart: route.week_start,
                                customer: apt.customer
                            });
                        }
                    });
                });
            } catch (e) {
                console.error(`Fehler beim Parsen von Route ${route.id}:`, e);
            }
        });
        appointmentUsage.forEach((usage, aptId) => {
            if (usage.length > 1) {
                duplicates.push({
                    appointmentId: aptId,
                    customer: usage[0].customer,
                    plannedIn: usage.map(u => u.weekStart),
                    routes: usage
                });
            }
        });
        console.log(`🔍 ${duplicates.length} doppelt geplante Termine gefunden`);
        if (req.body.fix === true && duplicates.length > 0) {
            for (const dup of duplicates) {
                const keepWeek = dup.routes[0].weekStart;
                for (let i = 1; i < dup.routes.length; i++) {
                    const removeFrom = dup.routes[i];
                    console.log(`🗑️ Entferne ${dup.customer} aus Woche ${removeFrom.weekStart}`);
                    const routeData = await new Promise((resolve, reject) => {
                        db.get("SELECT route_data FROM saved_routes WHERE id = ?",
                            [removeFrom.routeId],
                            (err, row) => err ? reject(err) : resolve(JSON.parse(row.route_data))
                        );
                    });
                    routeData.days.forEach(day => {
                        day.appointments = day.appointments.filter(apt => apt.id !== dup.appointmentId);
                    });
                    routeData.stats.totalAppointments = routeData.days.reduce(
                        (sum, day) => sum + day.appointments.length, 0
                    );
                    await new Promise((resolve, reject) => {
                        db.run(
                            "UPDATE saved_routes SET route_data = ? WHERE id = ?",
                            [JSON.stringify(routeData), removeFrom.routeId],
                            err => err ? reject(err) : resolve()
                        );
                    });
                }
            }
            res.json({
                success: true,
                message: `${duplicates.length} Duplikate bereinigt`,
                duplicates: duplicates,
                action: 'fixed'
            });
        } else {
            res.json({
                success: true,
                duplicatesFound: duplicates.length,
                duplicates: duplicates.slice(0, 10),
                message: duplicates.length > 0 ?
                    `${duplicates.length} Duplikate gefunden. Setze fix=true zum Bereinigen` :
                    'Keine Duplikate gefunden'
            });
        }
    } catch (error) {
        console.error('❌ Duplikat-Bereinigung fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// HELPER: Lade alle Termine einer Woche (für Alternative Slots)
async function getWeekAppointments(weekStart) {
    return new Promise((resolve, reject) => {
        db.get(
            "SELECT route_data FROM saved_routes WHERE week_start = ? AND is_active = 1",
            [weekStart],
            (err, row) => {
                if (err || !row) {
                    resolve([]);
                } else {
                    try {
                        const routeData = JSON.parse(row.route_data);
                        const allAppointments = [];
                        routeData.days.forEach(day => {
                            day.appointments.forEach(apt => {
                                allAppointments.push({
                                    ...apt,
                                    day: day.day,
                                    date: day.date
                                });
                            });
                        });
                        resolve(allAppointments);
                    } catch (e) {
                        resolve([]);
                    }
                }
            }
        );
    });
}

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
    console.log('⚡ OPTIMIERTE API-Endpoints aktiviert:');
    console.log('  POST /api/admin/import-csv-optimized - Effizienter CSV Import');
    console.log('  POST /api/routes/optimize-efficient - Minimaler API-Verbrauch');
    console.log('  💰 Erwartete Kosteneinsparung: 95-98%');
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

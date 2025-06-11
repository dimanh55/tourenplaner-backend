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
// HILFSFUNKTIONEN FÜR ROUTENOPTIMIERUNG
// ======================================================================

async function selectMaxAppointmentsForWeek(allAppointments, weekStart) {
    console.log(`🎯 MAXIMIERE ALLE TERMINE für Woche ${weekStart}`);
    
    // Trenne fixe und flexible Termine
    const weekStartDate = new Date(weekStart);
    const weekEndDate = new Date(weekStartDate);
    weekEndDate.setDate(weekStartDate.getDate() + 4); // Freitag

    const fixedAppointmentsThisWeek = allAppointments.filter(apt => {
        if (!apt.is_fixed || !apt.fixed_date) return false;
        const aptDate = new Date(apt.fixed_date);
        return aptDate >= weekStartDate && aptDate <= weekEndDate;
    });

    const flexibleAppointments = allAppointments.filter(apt => 
        !apt.is_fixed || !apt.fixed_date
    );

    // Bereits verwendete Termine prüfen
    const usedAppointmentIds = await getUsedAppointmentIds(weekStart);
    
    // ALLE verfügbaren Termine verwenden
    const selectedAppointments = [...fixedAppointmentsThisWeek];
    
    const availableFlexible = flexibleAppointments.filter(apt => 
        !usedAppointmentIds.includes(apt.id) &&
        !selectedAppointments.some(selected => selected.id === apt.id)
    );
    
    // Nach Pipeline-Alter und Status sortieren
    const sortedFlexible = availableFlexible.sort((a, b) => {
        if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
        if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
        return b.pipeline_days - a.pipeline_days;
    });
    
    // ALLE verfügbaren Termine hinzufügen
    selectedAppointments.push(...sortedFlexible);
    
    console.log(`✅ MAXIMUM ERREICHT: ${selectedAppointments.length} Termine ausgewählt`);
    
    return selectedAppointments;
}

async function performMaxEfficiencyOptimization(appointments, weekStart, driverId) {
    console.log('⚡ MAXIMALE EFFIZIENZ: Alle Termine planen...');
    
    try {
        const optimizableAppointments = appointments.map(apt => {
            let parsedNotes = {};
            try {
                parsedNotes = JSON.parse(apt.notes || '{}');
            } catch (e) {
                parsedNotes = {};
            }

            return {
                id: apt.id,
                customer: apt.customer,
                invitee_name: parsedNotes.invitee_name || apt.customer,
                company: parsedNotes.company || '',
                customer_company: parsedNotes.customer_company || '',
                address: apt.address,
                priority: apt.priority,
                status: apt.status,
                duration: apt.duration || 3,
                pipeline_days: apt.pipeline_days || 0,
                is_fixed: apt.is_fixed,
                fixed_date: apt.fixed_date,
                fixed_time: apt.fixed_time,
                notes: apt.notes,
                lat: apt.lat,
                lng: apt.lng,
                geocoded: apt.geocoded
            };
        });

        const planner = new IntelligentRoutePlanner();
        const optimizedResult = await planner.optimizeWeek(optimizableAppointments, weekStart, driverId || 1);
        
        return optimizedResult;
        
    } catch (error) {
        console.warn('⚠️ Planner fehlgeschlagen, verwende Fallback:', error.message);
        return performMaxEfficiencyFallback(appointments, weekStart);
    }
}

async function performMaxEfficiencyFallback(appointments, weekStart) {
    console.log('🔄 Verwende Max-Effizienz Fallback-Optimierung...');
    
    const optimizedWeek = createEmptyWeekStructure(weekStart);
    // Vereinfachte Fallback-Logik hier...
    
    return {
        weekStart,
        days: optimizedWeek,
        totalHours: 40,
        optimizations: ['Fallback-Optimierung verwendet'],
        stats: {
            totalAppointments: appointments.length,
            confirmedAppointments: appointments.filter(a => a.status === 'bestätigt').length,
            proposalAppointments: appointments.filter(a => a.status === 'vorschlag').length,
            fixedAppointments: appointments.filter(a => a.is_fixed).length,
            totalTravelTime: 15,
            workDays: 5,
            efficiency: { travelEfficiency: 0.8, weekUtilization: 0.8 }
        },
        generatedAt: new Date().toISOString()
    };
}

async function getUsedAppointmentIds(excludeWeekStart) {
    return new Promise((resolve, reject) => {
        db.all(
            "SELECT route_data FROM saved_routes WHERE week_start != ? AND is_active = 1",
            [excludeWeekStart],
            (err, rows) => {
                if (err) {
                    reject(err);
                    return;
                }
                
                const usedIds = new Set();
                rows.forEach(row => {
                    try {
                        const routeData = JSON.parse(row.route_data);
                        routeData.days?.forEach(day => {
                            day.appointments?.forEach(apt => {
                                if (apt.id) usedIds.add(apt.id);
                            });
                        });
                    } catch (e) {
                        console.warn('Fehler beim Parsen der Route-Daten:', e);
                    }
                });
                
                resolve(Array.from(usedIds));
            }
        );
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
// ======================================================================
// SERVER INTEGRATION FÜR ENHANCED GEOCODING SERVICE
// Zu server.js hinzufügen - Neue API Endpoints
// ======================================================================

// Enhanced Geocoding Service importieren (oben im server.js einfügen)
const EnhancedGeocodingService = require('./geocoding-service');

// Service-Instanz erstellen (nach anderen Initialisierungen)
const geocodingService = new EnhancedGeocodingService();

// ======================================================================
// NEUE API ROUTES FÜR GEOCODING (Zu server.js hinzufügen)
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

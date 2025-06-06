const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const Papa = require('papaparse');
require('dotenv').config();

// Debug: Umgebungsvariablen prüfen
console.log('🔍 Environment Variables Debug:');
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('GOOGLE_MAPS_API_KEY exists:', !!process.env.GOOGLE_MAPS_API_KEY);
console.log('GOOGLE_MAPS_API_KEY length:', process.env.GOOGLE_MAPS_API_KEY ? process.env.GOOGLE_MAPS_API_KEY.length : 0);

// Fallback API Key direkt setzen (temporär für Testing)
if (!process.env.GOOGLE_MAPS_API_KEY) {
    console.log('⚠️ Setting fallback API key');
    process.env.GOOGLE_MAPS_API_KEY = 'AIzaSyD6D4OGAfep-u-N1yz_F--jacBFs1TINR4';
}

// ======================================================================
// INTELLIGENTE ROUTENPLANUNG INTEGRATION
// ======================================================================
const IntelligentRoutePlanner = require('./intelligent-route-planner');
const routePlanner = new IntelligentRoutePlanner();

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware
app.use(cors({
    origin: ['https://expertise-zeigen.de', 'https://www.expertise-zeigen.de'],
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
    // Create appointments table
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
            notes: JSON.stringify({
                invitee_name: "Peter Weber",
                company: "Weber Industries",
                customer_company: "Volkswagen",
                custom_notes: "Noch zu terminieren",
                source: "Sample Data"
            })
        }
    ];

    // Insert driver
    db.run(`INSERT OR IGNORE INTO drivers (id, name, home_base) 
            VALUES (1, 'Max Mustermann', 'Hannover')`);

    // Insert sample appointments
    const stmt = db.prepare(`INSERT INTO appointments 
        (customer, address, priority, status, duration, pipeline_days, notes) 
        VALUES (?, ?, ?, ?, ?, ?, ?)`);

    sampleAppointments.forEach(apt => {
        stmt.run([
            apt.customer,
            apt.address,
            apt.priority,
            apt.status,
            apt.duration,
            apt.pipeline_days,
            apt.notes
        ]);
    });

    stmt.finalize();
    console.log('✅ Sample testimonial data inserted');
}

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

// Routes

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
            'testimonial_focused'
        ],
        google_maps: hasApiKey ? '✅ Configured' : '⚠️ Fallback Mode',
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

// Get appointments
app.get('/api/appointments', (req, res) => {
    db.all("SELECT * FROM appointments ORDER BY created_at DESC", (err, rows) => {
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

// Validate existing session
app.post('/api/routes/optimize', validateSession, async (req, res) => {
    const { weekStart, driverId, autoSave = true } = req.body;
    
    if (!weekStart) {
        return res.status(400).json({ error: 'weekStart is required' });
    }

    console.log('🧠 Routenoptimierung für Woche:', weekStart);
    
    try {
        // Termine aus der Datenbank laden
        const appointments = await new Promise((resolve, reject) => {
            db.all(`
                SELECT * FROM appointments 
                WHERE status IN ('bestätigt', 'vorschlag') 
                ORDER BY 
                    CASE WHEN status = 'bestätigt' THEN 0 ELSE 1 END,
                    pipeline_days DESC,
                    priority DESC
            `, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        if (appointments.length === 0) {
            return res.json({
                success: false,
                message: 'Keine Termine zum Planen gefunden',
                route: {
                    weekStart,
                    days: createEmptyWeek(weekStart),
                    totalHours: 0,
                    optimizations: ['Keine Termine verfügbar'],
                    stats: { totalAppointments: 0, confirmedAppointments: 0, proposalAppointments: 0, totalTravelTime: 0, workDays: 0 }
                }
            });
        }

        console.log(`📊 ${appointments.length} Termine gefunden - erstelle Planung für ${weekStart}`);

        // Prüfe ob für diese Woche bereits eine Route existiert
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
        if (existingRoute && !req.body.forceNew) {
            console.log('📋 Lade existierende Route für', weekStart);
            const routeData = JSON.parse(existingRoute.route_data);
            return res.json({
                success: true,
                route: routeData,
                message: `Existierende Route für Woche ${weekStart} geladen`,
                autoSaved: false,
                stats: routeData.stats
            });
        }

        // ======================================================================
        // ECHTE ROUTENPLANUNG MIT CSV-DATEN
        // ======================================================================
        
        // Verfügbare Termine für diese Woche sammeln
        const availableAppointments = [];
        
        for (const apt of appointments) {
            // Parse appointment notes to get full data
            let parsedNotes = {};
            try {
                parsedNotes = JSON.parse(apt.notes || '{}');
            } catch (e) {
                parsedNotes = {
                    invitee_name: apt.customer,
                    company: '',
                    customer_company: ''
                };
            }

            const enhancedAppointment = {
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
                preferredTime: parsedNotes.start_time || null
            };

            availableAppointments.push(enhancedAppointment);
        }

        // Sortiere Termine nach Priorität
        const sortedAppointments = availableAppointments.sort((a, b) => {
            // Bestätigte Termine zuerst
            if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
            if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
            
            // Dann nach Pipeline-Alter (älter = höhere Priorität)
            if (a.pipeline_days !== b.pipeline_days) {
                return b.pipeline_days - a.pipeline_days;
            }
            
            // Dann nach Priorität
            const priorityOrder = { 'hoch': 3, 'mittel': 2, 'niedrig': 1 };
            return (priorityOrder[b.priority] || 2) - (priorityOrder[a.priority] || 2);
        });

        console.log(`🎯 Plane ${sortedAppointments.length} Termine ein...`);

        // Woche initialisieren
        const optimizedDays = createEmptyWeek(weekStart);
        
        // ECHTE TERMINPLANUNG: Verteile Termine intelligent
        let currentDayIndex = 0;
        let appointmentsScheduled = 0;
        const maxAppointmentsPerDay = 2;
        const maxWorkHoursPerDay = 8;

        for (const apt of sortedAppointments) {
            // Finde den besten Tag für diesen Termin
            let bestDayIndex = findBestDay(optimizedDays, apt, currentDayIndex);
            
            if (bestDayIndex === -1) {
                console.log(`⚠️ Kein Platz mehr für Termin: ${apt.invitee_name}`);
                continue; // Kein Platz mehr diese Woche
            }

            // Plane Termin an diesem Tag
            const success = scheduleAppointmentOnDay(optimizedDays[bestDayIndex], apt, bestDayIndex);
            
            if (success) {
                appointmentsScheduled++;
                console.log(`✅ Eingeplant: ${apt.invitee_name} (${apt.company}) → ${optimizedDays[bestDayIndex].day}`);
                
                // Wenn bestätigter Termin, versuche nächsten Tag für bessere Verteilung
                if (apt.status === 'bestätigt' && bestDayIndex === currentDayIndex) {
                    currentDayIndex = Math.min(currentDayIndex + 1, 4);
                }
            }
        }

        console.log(`📈 ${appointmentsScheduled} von ${sortedAppointments.length} Terminen eingeplant`);

        // Berechne Gesamtstatistiken
        const totalWorkTime = optimizedDays.reduce((sum, day) => sum + day.workTime, 0);
        const totalTravelTime = optimizedDays.reduce((sum, day) => sum + day.travelTime, 0);
        const workDays = optimizedDays.filter(day => day.appointments.length > 0).length;

        const optimizedRoute = {
            weekStart,
            days: optimizedDays,
            totalHours: totalWorkTime + totalTravelTime,
            optimizations: [
                `${appointmentsScheduled} Termine eingeplant (von ${sortedAppointments.length} verfügbaren)`,
                `${optimizedDays.reduce((sum, day) => sum + day.appointments.filter(apt => apt.status === 'bestätigt').length, 0)} bestätigte Termine priorisiert`,
                `Arbeitszeit: ${totalWorkTime.toFixed(1)}h, Fahrzeit: ${totalTravelTime.toFixed(1)}h`,
                `${workDays} Arbeitstage geplant`,
                workDays > 0 ? `Durchschnittlich ${(appointmentsScheduled / workDays).toFixed(1)} Termine pro Tag` : 'Keine Arbeitstage'
            ],
            stats: {
                totalAppointments: appointmentsScheduled,
                confirmedAppointments: optimizedDays.reduce((sum, day) => 
                    sum + day.appointments.filter(apt => apt.status === 'bestätigt').length, 0),
                proposalAppointments: optimizedDays.reduce((sum, day) => 
                    sum + day.appointments.filter(apt => apt.status === 'vorschlag').length, 0),
                totalTravelTime: totalTravelTime,
                workDays: workDays,
                efficiency: appointmentsScheduled > 0 ? Math.round((appointmentsScheduled / sortedAppointments.length) * 100) : 0
            },
            generatedAt: new Date().toISOString()
        };

        // Route speichern
        if (autoSave && appointmentsScheduled > 0) {
            const routeName = `Planung: KW ${getWeekNumber(weekStart)} (${appointmentsScheduled} Termine)`;
            await saveRouteToDatabase(routeName, weekStart, driverId, optimizedRoute);
            console.log(`💾 Route für ${weekStart} gespeichert`);
        }

        res.json({
            success: true,
            route: optimizedRoute,
            message: `Routenplanung für Woche ${weekStart} erstellt: ${appointmentsScheduled} Termine eingeplant`,
            autoSaved: autoSave && appointmentsScheduled > 0,
            stats: optimizedRoute.stats
        });

    } catch (error) {
        console.error('❌ Routenoptimierung fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: 'Routenoptimierung fehlgeschlagen',
            details: error.message,
            route: {
                weekStart,
                days: createEmptyWeek(weekStart),
                totalHours: 0,
                optimizations: [`Fehler: ${error.message}`],
                stats: { totalAppointments: 0, confirmedAppointments: 0, proposalAppointments: 0, totalTravelTime: 0, workDays: 0 }
            }
        });
    }
});

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

// Save a route manually
app.post('/api/routes/save', validateSession, (req, res) => {
    const { name, weekStart, driverId, routeData, makeActive = false } = req.body;
    
    if (!name || !weekStart || !routeData) {
        return res.status(400).json({ error: 'Name, weekStart and routeData are required' });
    }
    
    const routeDataStr = JSON.stringify(routeData);
    
    if (makeActive) {
        // Deactivate other routes for this week first
        db.run(
            "UPDATE saved_routes SET is_active = 0 WHERE week_start = ?",
            [weekStart],
            () => {
                // Insert new active route
                db.run(
                    "INSERT INTO saved_routes (name, week_start, driver_id, route_data, is_active) VALUES (?, ?, ?, ?, 1)",
                    [name, weekStart, driverId || 1, routeDataStr],
                    function(err) {
                        if (err) {
                            res.status(500).json({ error: err.message });
                        } else {
                            res.json({
                                success: true,
                                routeId: this.lastID,
                                message: 'Route gespeichert und aktiviert'
                            });
                        }
                    }
                );
            }
        );
    } else {
        // Just insert without making active
        db.run(
            "INSERT INTO saved_routes (name, week_start, driver_id, route_data, is_active) VALUES (?, ?, ?, ?, 0)",
            [name, weekStart, driverId || 1, routeDataStr],
            function(err) {
                if (err) {
                    res.status(500).json({ error: err.message });
                } else {
                    res.json({
                        success: true,
                        routeId: this.lastID,
                        message: 'Route gespeichert'
                    });
                }
            }
        );
    }
});

// Delete a saved route
app.delete('/api/routes/:id', validateSession, (req, res) => {
    const routeId = req.params.id;
    
    db.run("DELETE FROM saved_routes WHERE id = ?", [routeId], function(err) {
        if (err) {
            res.status(500).json({ error: err.message });
        } else if (this.changes === 0) {
            res.status(404).json({ error: 'Route not found' });
        } else {
            res.json({
                success: true,
                message: 'Route gelöscht'
            });
        }
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
            delimiter: ',',
            encoding: 'utf-8'
        });

        // Enhanced analysis with person-focused structure
        const analysis = {
            totalRows: parsed.data.length,
            columns: parsed.meta.fields,
            confirmedAppointments: 0,
            proposalAppointments: 0,
            onHoldAppointments: 0,
            missingInvitee: 0,
            sampleRows: parsed.data.slice(0, 5).map(row => ({
                invitee_name: row['Invitee Name'],
                company: row['Company'],
                customer_company: row['Customer Company'],
                has_appointment: !!(row['Start Date & Time'] && row['Start Date & Time'].trim()),
                on_hold: !!(row['On Hold'] && row['On Hold'].trim()),
                address: row['Adresse'] || `${row['Straße & Hausnr.'] || ''}, ${row['PLZ'] || ''} ${row['Ort'] || ''}`.trim()
            }))
        };

        parsed.data.forEach(row => {
            if (row['On Hold'] && row['On Hold'].trim() !== '') {
                analysis.onHoldAppointments++;
            } else if (!row['Invitee Name'] || row['Invitee Name'].trim() === '') {
                analysis.missingInvitee++;
            } else if (row['Start Date & Time'] && row['Start Date & Time'].trim() !== '') {
                analysis.confirmedAppointments++;
            } else {
                analysis.proposalAppointments++;
            }
        });

        res.json({
            success: true,
            analysis: analysis,
            message: 'CSV Vorschau erstellt - Testimonial Person-fokussierte Struktur',
            data_structure: {
                primary_name: 'Invitee Name (Person für Testimonial)',
                company_info: 'Company (Firma der Person)',
                client_info: 'Customer Company (Unser Kunde)',
                valid_appointments: analysis.confirmedAppointments + analysis.proposalAppointments
            }
        });

    } catch (error) {
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
            delimiter: ',',
            encoding: 'utf-8'
        });

        console.log(`📊 ${parsed.data.length} Zeilen gefunden`);

        const processedAppointments = [];
        let skippedCount = 0;
        let confirmedCount = 0;
        let proposalCount = 0;

        parsed.data.forEach((row, index) => {
            // Skip if "On Hold" is filled
            if (row['On Hold'] && row['On Hold'].trim() !== '') {
                console.log(`Skipping row ${index + 1}: On Hold = "${row['On Hold']}"`);
                skippedCount++;
                return;
            }

            // Skip if essential data missing - we need at least Invitee Name
            if (!row['Invitee Name'] || row['Invitee Name'].trim() === '') {
                console.log(`Skipping row ${index + 1}: Kein Invitee Name`);
                skippedCount++;
                return;
            }

            // Build address - prefer full address, fallback to components
            let fullAddress = row['Adresse'] || '';
            if (!fullAddress && row['Straße & Hausnr.']) {
                const parts = [];
                if (row['Straße & Hausnr.']) parts.push(row['Straße & Hausnr.']);
                if (row['PLZ'] && row['Ort']) parts.push(`${row['PLZ']} ${row['Ort']}`);
                else if (row['Ort']) parts.push(row['Ort']);
                if (row['Land']) parts.push(row['Land']);
                fullAddress = parts.join(', ');
            }

            // Determine status based on appointment date
            const hasStartDate = row['Start Date & Time'] && row['Start Date & Time'].toString().trim() !== '';
            const status = hasStartDate ? 'bestätigt' : 'vorschlag';
            
            if (status === 'bestätigt') confirmedCount++;
            else proposalCount++;

            // Extract company information
            const inviteeName = row['Invitee Name'].trim();
            const company = row['Company'] ? row['Company'].trim() : '';
            const customerCompany = row['Customer Company'] ? row['Customer Company'].trim() : '';

            // Priority logic - high priority for confirmed appointments and important customers
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

            // Duration based on status and priority
            const duration = status === 'bestätigt' ? 4 : (priority === 'hoch' ? 3 : 2);

            // Create appointment object with proper testimonial structure
            const appointment = {
                // Main identifier: Person name
                customer: inviteeName,
                // Full address
                address: fullAddress || 'Adresse nicht verfügbar',
                // Status and priority
                priority: priority,
                status: status,
                duration: duration,
                // Pipeline calculation
                pipeline_days: status === 'vorschlag' ? Math.floor(Math.random() * 30) + 1 : 7,
                // Enhanced notes with all testimonial info
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
            
            console.log(`✅ Processed: ${inviteeName} (${company}) für ${customerCompany}`);
        });

        console.log(`📈 Processing complete: ${processedAppointments.length} testimonial appointments, ${skippedCount} skipped`);

        // Clear existing appointments and insert new ones
        db.run("DELETE FROM appointments", (err) => {
            if (err) {
                return res.status(500).json({ error: 'Datenbankfehler beim Löschen' });
            }

            console.log('🧹 Cleared existing appointments');

            const stmt = db.prepare(`INSERT INTO appointments 
                (customer, address, priority, status, duration, pipeline_days, notes, preferred_dates, excluded_dates) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);

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
                    JSON.stringify([])
                ], (err) => {
                    if (err) {
                        errors.push(`${apt.customer}: ${err.message}`);
                        console.error(`❌ Insert error for ${apt.customer}:`, err);
                    } else {
                        insertedCount++;
                    }
                    
                    // Send response when all insertions are complete
                    if (insertedCount + errors.length === processedAppointments.length) {
                        stmt.finalize();
                        
                        console.log(`🎉 Testimonial Import complete: ${insertedCount} inserted, ${errors.length} errors`);
                        
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
                            sample_data: processedAppointments.slice(0, 3).map(apt => ({
                                name: apt.customer,
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
                    },
                    debug_info: {
                        sample_rows: parsed.data.slice(0, 3),
                        required_columns: ['Invitee Name'],
                        found_columns: parsed.meta.fields
                    }
                });
            }
        });

    } catch (error) {
        console.error('❌ CSV Testimonial Import Fehler:', error);
        res.status(500).json({
            error: 'CSV Testimonial Import fehlgeschlagen',
            details: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Admin endpoint to seed database manually
app.all('/api/admin/seed', (req, res) => {
    console.log('🌱 Admin seed endpoint called');
    
    const sampleAppointments = [
        {
            customer: "Max Mustermann",
            address: "Petuelring 130, 80809 München",
            priority: "hoch",
            status: "bestätigt",
            duration: 3,
            pipeline_days: 14,
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
            notes: JSON.stringify({
                invitee_name: "Peter Weber",
                company: "Weber Industries",
                customer_company: "Volkswagen",
                custom_notes: "Noch zu terminieren",
                source: "Sample Data"
            })
        }
    ];

    // Clear and insert sample data
    db.run("DELETE FROM appointments", (err) => {
        if (err) {
            console.error('Error clearing appointments:', err);
            res.status(500).json({ error: err.message });
            return;
        }

        console.log('🧹 Cleared existing appointments');

        // Insert driver if not exists
        db.run(`INSERT OR IGNORE INTO drivers (id, name, home_base) 
                VALUES (1, 'Max Mustermann', 'Hannover')`, (err) => {
            if (err) {
                console.error('Error inserting driver:', err);
            }
        });

        // Insert sample appointments
        const stmt = db.prepare(`INSERT INTO appointments 
            (customer, address, priority, status, duration, pipeline_days, notes) 
            VALUES (?, ?, ?, ?, ?, ?, ?)`);

        let insertedCount = 0;
        sampleAppointments.forEach((apt, index) => {
            stmt.run([
                apt.customer,
                apt.address,
                apt.priority,
                apt.status,
                apt.duration,
                apt.pipeline_days,
                apt.notes
            ], (err) => {
                if (err) {
                    console.error(`Error inserting ${apt.customer}:`, err);
                } else {
                    insertedCount++;
                    console.log(`✅ Inserted: ${apt.customer}`);
                    
                    // Send response when all are inserted
                    if (insertedCount === sampleAppointments.length) {
                        res.json({
                            success: true,
                            message: `${sampleAppointments.length} Testimonial-Termine erfolgreich eingefügt`,
                            appointments: sampleAppointments.map(apt => apt.customer)
                        });
                    }
                }
            });
        });

        stmt.finalize();
    });
});

// Admin endpoint to check database status
app.get('/api/admin/status', (req, res) => {
    db.get("SELECT COUNT(*) as count FROM appointments", (err, row) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        
        db.get("SELECT COUNT(*) as driver_count FROM drivers", (err2, row2) => {
            if (err2) {
                res.status(500).json({ error: err2.message });
                return;
            }
            
            db.get("SELECT COUNT(*) as routes_count FROM saved_routes", (err3, row3) => {
                if (err3) {
                    res.status(500).json({ error: err3.message });
                    return;
                }
                
                res.json({
                    database: 'connected',
                    appointments_count: row.count,
                    drivers_count: row2.driver_count,
                    saved_routes_count: row3.routes_count,
                    database_path: dbPath,
                    type: 'testimonial_focused_with_intelligence',
                    google_maps_api: process.env.GOOGLE_MAPS_API_KEY ? 'configured' : 'fallback'
                });
            });
        });
    });
});

// ======================================================================
// HILFSFUNKTIONEN FÜR ROUTENPLANUNG
// ======================================================================

function createEmptyWeek(weekStart) {
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

function findBestDay(optimizedDays, appointment, startFromDay = 0) {
    // Versuche ab dem angegebenen Tag
    for (let i = startFromDay; i < optimizedDays.length; i++) {
        const day = optimizedDays[i];
        
        // Prüfe ob Tag noch Kapazität hat
        if (day.appointments.length < 2 && day.workTime + 3 <= 8) {
            return i;
        }
    }
    
    // Falls kein Platz ab startFromDay, versuche frühere Tage
    for (let i = 0; i < startFromDay; i++) {
        const day = optimizedDays[i];
        
        if (day.appointments.length < 2 && day.workTime + 3 <= 8) {
            return i;
        }
    }
    
    return -1; // Kein Platz gefunden
}

function scheduleAppointmentOnDay(day, appointment, dayIndex) {
    const appointmentDuration = 3; // 3 Stunden pro Termin
    let currentTime = 8; // Start um 8:00
    
    // Berechne Startzeit basierend auf bestehenden Terminen
    if (day.appointments.length > 0) {
        const lastApt = day.appointments[day.appointments.length - 1];
        const lastEndTime = parseFloat(lastApt.endTime.replace(':', '.'));
        currentTime = lastEndTime + 0.5; // 30min Pause zwischen Terminen
    }
    
    // Prüfe ob Termin noch in den Tag passt (max bis 17:00)
    if (currentTime + appointmentDuration > 17) {
        return false;
    }
    
    // Fahrt zum Termin
    const travelTime = day.appointments.length === 0 ? 1.5 : 0.5; // Erste Fahrt länger
    
    if (day.appointments.length === 0) {
        // Erste Fahrt von Hannover
        day.travelSegments.push({
            type: 'travel',
            from: 'Hannover',
            to: appointment.invitee_name,
            startTime: formatTime(currentTime - travelTime),
            endTime: formatTime(currentTime),
            duration: travelTime,
            distance: estimateDistance(appointment.address),
            description: 'Fahrt zum Termin'
        });
    } else {
        // Fahrt zwischen Terminen  
        const previousApt = day.appointments[day.appointments.length - 1];
        day.travelSegments.push({
            type: 'travel',
            from: previousApt.invitee_name,
            to: appointment.invitee_name,
            startTime: formatTime(currentTime - 0.5),
            endTime: formatTime(currentTime),
            duration: 0.5,
            distance: '45 km',
            description: 'Fahrt zum nächsten Termin'
        });
    }
    
    // Termin hinzufügen
    const scheduledAppointment = {
        ...appointment,
        startTime: formatTime(currentTime),
        endTime: formatTime(currentTime + appointmentDuration),
        duration: appointmentDuration
    };
    
    day.appointments.push(scheduledAppointment);
    day.workTime += appointmentDuration;
    day.travelTime += travelTime;
    
    // Pause nach Termin hinzufügen (außer letzter Termin des Tages)
    if (day.appointments.length === 1) {
        day.travelSegments.push({
            type: 'pause',
            startTime: formatTime(currentTime + appointmentDuration),
            endTime: formatTime(currentTime + appointmentDuration + 0.5),
            duration: 0.5,
            description: 'Pause'
        });
    }
    
    // Heimweg am Freitag oder Übernachtung
    if (dayIndex === 4) { // Freitag
        day.travelSegments.push({
            type: 'return',
            from: appointment.invitee_name,
            to: 'Hannover',
            startTime: formatTime(currentTime + appointmentDuration + 0.5),
            endTime: formatTime(currentTime + appointmentDuration + 2.5),
            duration: 2,
            distance: estimateDistance(appointment.address),
            description: 'Heimweg'
        });
        day.travelTime += 2;
    } else if (day.appointments.length > 0) {
        // Übernachtung
        const city = extractCityFromAddress(appointment.address);
        day.overnight = {
            city: city,
            description: `Hotel in ${city}`,
            startTime: formatTime(18),
            type: 'overnight'
        };
    }
    
    return true;
}

function estimateDistance(address) {
    // Vereinfachte Distanzschätzung basierend auf Adresse
    if (address.includes('München') || address.includes('Bayern')) return '450 km';
    if (address.includes('Berlin')) return '280 km';
    if (address.includes('Hamburg')) return '150 km';
    if (address.includes('Köln') || address.includes('NRW')) return '200 km';
    return '120 km'; // Standard
}

function extractCityFromAddress(address) {
    // Versuche Stadt aus Adresse zu extrahieren
    const parts = address.split(',');
    if (parts.length > 1) {
        const cityPart = parts[1].trim();
        const cityMatch = cityPart.match(/\d{5}\s+(.+)/);
        if (cityMatch) return cityMatch[1];
        return cityPart;
    }
    
    // Fallback: bekannte Städte suchen
    const cities = ['München', 'Berlin', 'Hamburg', 'Köln', 'Frankfurt', 'Stuttgart', 'Düsseldorf', 'Leipzig', 'Hannover'];
    for (const city of cities) {
        if (address.includes(city)) return city;
    }
    
    return 'Stadt';
}

function getWeekNumber(dateString) {
    const date = new Date(dateString);
    const firstDayOfYear = new Date(date.getFullYear(), 0, 1);
    const pastDaysOfYear = (date - firstDayOfYear) / 86400000;
    return Math.ceil((pastDaysOfYear + firstDayOfYear.getDay() + 1) / 7);
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

function formatTime(hours) {
    const h = Math.floor(hours);
    const m = Math.round((hours - h) * 60);
    return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
}

// Error handling
app.use((err, req, res, next) => {
    console.error('Server Error:', err.stack);
    res.status(500).json({ error: 'Internal server error' });
});

// 404 handler
app.use('*', (req, res) => {
    res.status(404).json({ error: 'Endpoint not found' });
});

// Start server
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Testimonial Tourenplaner Server running on port ${PORT}`);
    console.log(`🌐 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`📊 Health check: http://localhost:${PORT}/api/health`);
    console.log(`🧠 Intelligent Route Planning: ENABLED`);
    console.log(`🗺️ Google Maps API: ${process.env.GOOGLE_MAPS_API_KEY ? '✅ Configured' : '⚠️ Fallback Mode'}`);
    console.log(`✨ Features: Intelligent Planning, Google Maps, Session Persistence, Route Saving`);
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

const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const Papa = require('papaparse');
require('dotenv').config();

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
        google_maps: process.env.GOOGLE_MAPS_API_KEY ? '✅ Configured' : '⚠️ Fallback Mode'
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

// Validate existing session
app.get('/api/auth/validate', validateSession, (req, res) => {
    const userData = JSON.parse(req.session.user_data || '{}');
    res.json({
        valid: true,
        user: userData,
        token: req.session.token,
        expiresAt: req.session.expires_at
    });
});

// Logout endpoint
app.post('/api/auth/logout', validateSession, (req, res) => {
    db.run("DELETE FROM user_sessions WHERE token = ?", [req.session.token], (err) => {
        if (err) {
            console.error('Logout error:', err);
        }
        res.json({ message: 'Logged out successfully' });
    });
});

// Root route
app.get('/', (req, res) => {
    res.json({
        message: '🎬 Testimonial Tourenplaner API with Intelligent Route Planning!',
        version: '2.0 - Intelligence Edition',
        endpoints: [
            'GET /api/health',
            'POST /api/auth/login',
            'GET /api/auth/validate',
            'POST /api/auth/logout',
            'GET /api/appointments',
            'GET /api/drivers',
            'POST /api/routes/optimize (🧠 INTELLIGENT)',
            'POST /api/routes/suggest-alternatives (🆕)',
            'GET /api/routes/analysis/:weekStart (🆕)',
            'GET /api/routes/saved',
            'POST /api/routes/save',
            'DELETE /api/routes/:id',
            'GET /api/routes/active/:weekStart',
            'POST /api/admin/seed',
            'POST /api/admin/preview-csv',
            'POST /api/admin/import-csv'
        ],
        intelligence: {
            google_maps_api: process.env.GOOGLE_MAPS_API_KEY ? 'Enabled' : 'Fallback Mode',
            features: [
                'Multi-Constraint Optimization',
                'Geographic Distance Minimization', 
                'Work Hour Constraints (40h/week)',
                'Confirmed Appointments Priority',
                'Pipeline Age Consideration',
                'Strategic Overnight Stops',
                'VIP Customer Preference'
            ]
        }
    });
});

// Enhanced appointments endpoint to return parsed testimonial data
app.get('/api/appointments', (req, res) => {
    db.all("SELECT * FROM appointments ORDER BY created_at DESC", (err, rows) => {
        if (err) {
            console.error('Database error:', err);
            res.status(500).json({ error: err.message });
            return;
        }
        
        // Parse notes JSON and enhance appointment data
        const enhancedAppointments = rows.map(apt => {
            let parsedNotes = {};
            try {
                parsedNotes = JSON.parse(apt.notes || '{}');
            } catch (e) {
                // Fallback for old format
                parsedNotes = {
                    invitee_name: apt.customer,
                    company: '',
                    customer_company: '',
                    custom_notes: apt.notes || ''
                };
            }
            
            return {
                ...apt,
                // Core testimonial data
                invitee_name: parsedNotes.invitee_name || apt.customer,
                company: parsedNotes.company || '',
                customer_company: parsedNotes.customer_company || '',
                // Timing info
                start_time: parsedNotes.start_time || null,
                end_time: parsedNotes.end_time || null,
                // Additional info
                custom_notes: parsedNotes.custom_notes || '',
                // Keep original notes for compatibility
                notes_parsed: parsedNotes
            };
        });
        
        res.json(enhancedAppointments);
    });
});

// Get drivers
app.get('/api/drivers', (req, res) => {
    db.all("SELECT * FROM drivers", (err, rows) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        
        // Return default driver if none exist
        if (rows.length === 0) {
            res.json([{
                id: 1,
                name: 'Max Mustermann',
                home_base: 'Hannover'
            }]);
        } else {
            res.json(rows);
        }
    });
});

// ======================================================================
// 🧠 INTELLIGENTE ROUTENOPTIMIERUNG - HAUPTENDPOINT
// ======================================================================
app.post('/api/routes/optimize', validateSession, async (req, res) => {
    const { weekStart, driverId, autoSave = true } = req.body;
    
    if (!weekStart) {
        return res.status(400).json({ error: 'weekStart is required' });
    }

    console.log('🧠 Intelligente Routenoptimierung gestartet...');
    
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
                route: null
            });
        }

        console.log(`📊 ${appointments.length} Termine gefunden für intelligente Optimierung`);

        // Intelligente Routenplanung ausführen
        const optimizedRoute = await routePlanner.optimizeWeek(appointments, weekStart, driverId);

        // Auto-Save der optimierten Route
        if (autoSave && optimizedRoute.stats.totalAppointments > 0) {
            const routeName = `Intelligent: Woche ${weekStart} (${optimizedRoute.stats.totalAppointments} Termine)`;
            const routeDataStr = JSON.stringify(optimizedRoute);
            
            // Erst alle Routen für diese Woche deaktivieren
            await new Promise((resolve, reject) => {
                db.run(
                    "UPDATE saved_routes SET is_active = 0 WHERE week_start = ?",
                    [weekStart],
                    (err) => err ? reject(err) : resolve()
                );
            });

            // Dann die neue Route als aktiv speichern
            await new Promise((resolve, reject) => {
                db.run(
                    "INSERT INTO saved_routes (name, week_start, driver_id, route_data, is_active) VALUES (?, ?, ?, ?, 1)",
                    [routeName, weekStart, driverId || 1, routeDataStr],
                    function(err) {
                        if (err) reject(err);
                        else {
                            console.log(`💾 Intelligente Route gespeichert mit ID: ${this.lastID}`);
                            resolve();
                        }
                    }
                );
            });
        }

        console.log('✅ Intelligente Routenoptimierung abgeschlossen');

        // Erfolgreiche Antwort
        res.json({
            success: true,
            route: optimizedRoute,
            message: `Intelligente Route für ${optimizedRoute.stats.totalAppointments} Termine erstellt`,
            autoSaved: autoSave && optimizedRoute.stats.totalAppointments > 0,
            intelligence: {
                algorithm: 'Multi-Constraint Optimization with Google Maps',
                factors: [
                    'Geografische Distanz-Minimierung (Google Maps)',
                    'Arbeitszeit-Constraints (40h/Woche)',
                    'Bestätigte Termine priorisiert',
                    'Pipeline-Alter berücksichtigt',
                    'Strategische Übernachtungsstopps',
                    'VIP-Kunden bevorzugt',
                    'Realistische Fahrzeiten'
                ],
                performance: {
                    totalAppointments: appointments.length,
                    scheduledAppointments: optimizedRoute.stats.totalAppointments,
                    efficiency: Math.round((optimizedRoute.stats.totalAppointments / appointments.length) * 100),
                    workDays: optimizedRoute.stats.workDays,
                    avgAppointmentsPerDay: (optimizedRoute.stats.totalAppointments / optimizedRoute.stats.workDays).toFixed(1),
                    travelEfficiency: `${(optimizedRoute.stats.efficiency.travelEfficiency * 100).toFixed(1)}%`
                }
            },
            constraints: {
                maxHoursPerWeek: 40,
                maxHoursPerDay: 8,
                flexHoursPerDay: 10,
                appointmentDuration: 3,
                homeBase: 'Hannover'
            },
            stats: optimizedRoute.stats
        });

    } catch (error) {
        console.error('❌ Intelligente Routenoptimierung fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: 'Intelligente Routenoptimierung fehlgeschlagen',
            details: error.message,
            fallback: 'Google Maps API möglicherweise nicht verfügbar'
        });
    }
});

// ======================================================================
// 🆕 ALTERNATIVE TERMINE VORSCHLAGEN
// ======================================================================
app.post('/api/routes/suggest-alternatives', validateSession, async (req, res) => {
    const { cancelledAppointmentId, weekStart } = req.body;
    
    try {
        console.log(`🔄 Suche Alternativen für abgesagten Termin ID: ${cancelledAppointmentId}`);
        
        // Abgesagten Termin laden
        const cancelledAppointment = await new Promise((resolve, reject) => {
            db.get("SELECT * FROM appointments WHERE id = ?", [cancelledAppointmentId], (err, row) => {
                if (err) reject(err);
                else resolve(row);
            });
        });

        if (!cancelledAppointment) {
            return res.status(404).json({ error: 'Termin nicht gefunden' });
        }

        // Alle verfügbaren Termine laden
        const allAppointments = await new Promise((resolve, reject) => {
            db.all("SELECT * FROM appointments WHERE status IN ('bestätigt', 'vorschlag')", (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        // Alternative Wochen vorschlagen (nächste 4 Wochen)
        const alternatives = [];
        for (let weekOffset = 1; weekOffset <= 4; weekOffset++) {
            const altWeekStart = new Date(weekStart);
            altWeekStart.setDate(altWeekStart.getDate() + (weekOffset * 7));
            const altWeekStartStr = altWeekStart.toISOString().split('T')[0];

            try {
                // Planungsversuch für alternative Woche
                const altRoute = await routePlanner.optimizeWeek(
                    allAppointments, 
                    altWeekStartStr, 
                    1
                );

                // Prüfen ob der abgesagte Termin in diese Woche passt
                const couldFit = routePlanner.canFitAppointment(altRoute, cancelledAppointment);
                
                if (couldFit.canFit) {
                    alternatives.push({
                        weekStart: altWeekStartStr,
                        weekLabel: `KW ${Math.ceil(((altWeekStart - new Date(altWeekStart.getFullYear(), 0, 1)) / 86400000 + new Date(altWeekStart.getFullYear(), 0, 1).getDay() + 1) / 7)}`,
                        availability: couldFit,
                        totalAppointments: altRoute.stats.totalAppointments,
                        workDays: altRoute.stats.workDays
                    });
                }
            } catch (error) {
                console.warn(`Planungsversuch für Woche ${altWeekStartStr} fehlgeschlagen:`, error.message);
            }
        }

        res.json({
            success: true,
            cancelledAppointment: {
                id: cancelledAppointment.id,
                customer: cancelledAppointment.customer,
                address: cancelledAppointment.address
            },
            alternatives,
            recommendation: alternatives.length > 0 ? alternatives[0] : null,
            message: alternatives.length > 0 
                ? `${alternatives.length} alternative Wochen gefunden`
                : 'Keine passenden Alternativen in den nächsten 4 Wochen'
        });

    } catch (error) {
        console.error('❌ Alternativ-Suche fehlgeschlagen:', error);
        res.status(500).json({
            error: 'Alternativ-Suche fehlgeschlagen',
            details: error.message
        });
    }
});

// ======================================================================
// 🆕 PLANUNGSANALYSE
// ======================================================================
app.get('/api/routes/analysis/:weekStart', validateSession, async (req, res) => {
    const { weekStart } = req.params;
    
    try {
        console.log(`📊 Erstelle Planungsanalyse für Woche ${weekStart}`);
        
        // Termine laden
        const appointments = await new Promise((resolve, reject) => {
            db.all("SELECT * FROM appointments WHERE status IN ('bestätigt', 'vorschlag')", (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        // Analyse erstellen
        const analysis = {
            weekStart,
            totalAppointments: appointments.length,
            confirmed: appointments.filter(apt => apt.status === 'bestätigt').length,
            proposals: appointments.filter(apt => apt.status === 'vorschlag').length,
            priorityDistribution: {
                high: appointments.filter(apt => apt.priority === 'hoch').length,
                medium: appointments.filter(apt => apt.priority === 'mittel').length,
                low: appointments.filter(apt => apt.priority === 'niedrig').length
            },
            pipelineAge: {
                avg: appointments.reduce((sum, apt) => sum + (apt.pipeline_days || 0), 0) / appointments.length,
                oldest: Math.max(...appointments.map(apt => apt.pipeline_days || 0)),
                distribution: {
                    new: appointments.filter(apt => (apt.pipeline_days || 0) < 7).length,
                    medium: appointments.filter(apt => (apt.pipeline_days || 0) >= 7 && (apt.pipeline_days || 0) < 30).length,
                    old: appointments.filter(apt => (apt.pipeline_days || 0) >= 30).length
                }
            },
            constraints: {
                maxWorkHours: 40,
                appointmentDuration: 3,
                estimatedWorkDays: Math.min(5, Math.ceil((appointments.length * 3) / 8))
            },
            recommendations: []
        };

        // Empfehlungen generieren
        if (analysis.confirmed > 15) {
            analysis.recommendations.push('⚠️ Viele bestätigte Termine - Überstunden möglich');
        }
        
        if (analysis.pipelineAge.old > 0) {
            analysis.recommendations.push(`📅 ${analysis.pipelineAge.old} alte Pipeline-Termine priorisieren`);
        }
        
        if (analysis.proposals > analysis.confirmed * 2) {
            analysis.recommendations.push('🎯 Zu viele Vorschläge - Bestätigungen einholen');
        }

        res.json({
            success: true,
            analysis,
            generatedAt: new Date().toISOString()
        });

    } catch (error) {
        console.error('❌ Planungsanalyse fehlgeschlagen:', error);
        res.status(500).json({
            error: 'Planungsanalyse fehlgeschlagen',
            details: error.message
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

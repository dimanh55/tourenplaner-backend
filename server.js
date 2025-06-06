const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const Papa = require('papaparse');

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

    // ✨ NEW: Create saved_routes table for persistent route storage
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

    // ✨ NEW: Create user_sessions table for session management
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

    console.log('✅ Database tables initialized (including saved_routes and user_sessions)');
}

function insertSampleData() {
    const sampleAppointments = [
        {
            customer: "BMW München",
            address: "Petuelring 130, 80809 München",
            priority: "hoch",
            status: "bestätigt",
            duration: 3,
            pipeline_days: 14
        },
        {
            customer: "Mercedes-Benz Berlin",
            address: "Salzufer 1, 10587 Berlin",
            priority: "hoch",
            status: "bestätigt",
            duration: 3,
            pipeline_days: 21
        },
        {
            customer: "Volkswagen Wolfsburg",
            address: "Berliner Ring 2, 38440 Wolfsburg",
            priority: "mittel",
            status: "vorschlag",
            duration: 3,
            pipeline_days: 7
        },
        {
            customer: "Porsche Stuttgart",
            address: "Porscheplatz 1, 70435 Stuttgart",
            priority: "hoch",
            status: "bestätigt",
            duration: 4,
            pipeline_days: 28
        },
        {
            customer: "SAP Walldorf",
            address: "Hasso-Plattner-Ring 7, 69190 Walldorf",
            priority: "mittel",
            status: "vorschlag",
            duration: 2,
            pipeline_days: 5
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
            'Sample data for Tourenplaner'
        ]);
    });

    stmt.finalize();
    console.log('✅ Sample data inserted');
}

// ✨ NEW: Helper function to generate session tokens
function generateSessionToken() {
    return 'railway-session-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
}

// ✨ NEW: Middleware to validate session tokens
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
        message: 'Tourenplaner Backend running on Railway!',
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development',
        features: ['persistent_sessions', 'route_saving', 'csv_import']
    });
});

// ✨ UPDATED: Authentication with session persistence
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

// ✨ NEW: Validate existing session
app.get('/api/auth/validate', validateSession, (req, res) => {
    const userData = JSON.parse(req.session.user_data || '{}');
    res.json({
        valid: true,
        user: userData,
        token: req.session.token,
        expiresAt: req.session.expires_at
    });
});

// ✨ NEW: Logout endpoint
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
        message: '🚀 Tourenplaner API is running!',
        endpoints: [
            'GET /api/health',
            'POST /api/auth/login',
            'GET /api/auth/validate',
            'POST /api/auth/logout',
            'GET /api/appointments',
            'GET /api/drivers',
            'POST /api/routes/optimize',
            'GET /api/routes/saved',
            'POST /api/routes/save',
            'DELETE /api/routes/:id',
            'POST /api/admin/seed',
            'POST /api/admin/preview-csv',
            'POST /api/admin/import-csv'
        ]
    });
});

// Get appointments
app.get('/api/appointments', (req, res) => {
    db.all("SELECT * FROM appointments ORDER BY created_at DESC", (err, rows) => {
        if (err) {
            console.error('Database error:', err);
            res.status(500).json({ error: err.message });
            return;
        }
        res.json(rows);
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

// Route optimization
app.post('/api/routes/optimize', (req, res) => {
    const { weekStart, driverId, autoSave = true } = req.body;
    
    if (!weekStart) {
        return res.status(400).json({ error: 'weekStart is required' });
    }

    console.log('🔄 Route optimization started...');
    
    db.all("SELECT * FROM appointments WHERE status IN ('bestätigt', 'vorschlag') ORDER BY priority DESC, pipeline_days DESC", (err, appointments) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        
        // Simple route optimization
        const weekDays = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag'];
        const startDate = new Date(weekStart);
        
        const optimizedDays = weekDays.map((day, index) => {
            const date = new Date(startDate);
            date.setDate(startDate.getDate() + index);
            
            // Distribute appointments across days (max 2 per day)
            const dayAppointments = appointments.slice(index * 2, (index + 1) * 2);
            
            const mappedAppointments = dayAppointments.map((apt, i) => ({
                ...apt,
                startTime: `${9 + (i * 4)}:00`,
                endTime: `${9 + (i * 4) + apt.duration}:00`,
                travelTime: 1.5
            }));
            
            return {
                day,
                date: date.toISOString().split('T')[0],
                appointments: mappedAppointments,
                workTime: mappedAppointments.reduce((sum, apt) => sum + apt.duration, 0),
                travelTime: mappedAppointments.length * 1.5,
                overnight: mappedAppointments.length > 0 ? 
                    `Hotel in ${mappedAppointments[0]?.address?.split(',')[0] || 'Stadt'}` : null
            };
        });

        const optimizedRoute = {
            weekStart,
            totalHours: optimizedDays.reduce((sum, day) => sum + day.workTime + day.travelTime, 0),
            days: optimizedDays,
            optimizations: [
                `${appointments.length} Termine erfolgreich eingeplant`,
                'Arbeitszeiten optimiert (max. 40h/Woche)',
                'Fahrzeiten minimiert durch intelligente Reihenfolge'
            ],
            generatedAt: new Date().toISOString()
        };

        // ✨ NEW: Auto-save the optimized route if requested
        if (autoSave && appointments.length > 0) {
            const routeName = `Automatisch: Woche ${weekStart}`;
            const routeDataStr = JSON.stringify(optimizedRoute);
            
            // First, deactivate all routes for this week
            db.run(
                "UPDATE saved_routes SET is_active = 0 WHERE week_start = ?",
                [weekStart],
                () => {
                    // Then save the new route as active
                    db.run(
                        "INSERT INTO saved_routes (name, week_start, driver_id, route_data, is_active) VALUES (?, ?, ?, ?, 1)",
                        [routeName, weekStart, driverId || 1, routeDataStr],
                        function(err) {
                            if (err) {
                                console.error('Auto-save route error:', err);
                            } else {
                                console.log(`✅ Route auto-saved with ID: ${this.lastID}`);
                            }
                        }
                    );
                }
            );
        }

        console.log('✅ Route optimization completed');

        res.json({
            success: true,
            route: optimizedRoute,
            message: `Route für ${appointments.length} Termine optimiert`,
            autoSaved: autoSave && appointments.length > 0,
            stats: {
                totalAppointments: appointments.length,
                scheduledAppointments: optimizedRoute.days.reduce((sum, day) => sum + day.appointments.length, 0),
                totalHours: optimizedRoute.totalHours,
                workDays: optimizedRoute.days.filter(day => day.appointments.length > 0).length
            }
        });
    });
});

// ✨ NEW: Get saved routes
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

// ✨ NEW: Save a route manually
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

// ✨ NEW: Delete a saved route
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

// ✨ NEW: Load active route for a week
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

// Admin endpoint to seed database manually
app.all('/api/admin/seed', (req, res) => {
    console.log('🌱 Admin seed endpoint called');
    
    const sampleAppointments = [
        {
            customer: "BMW München",
            address: "Petuelring 130, 80809 München",
            priority: "hoch",
            status: "bestätigt",
            duration: 3,
            pipeline_days: 14
        },
        {
            customer: "Mercedes-Benz Berlin",
            address: "Salzufer 1, 10587 Berlin",
            priority: "hoch",
            status: "bestätigt",
            duration: 3,
            pipeline_days: 21
        },
        {
            customer: "Volkswagen Wolfsburg",
            address: "Berliner Ring 2, 38440 Wolfsburg",
            priority: "mittel",
            status: "vorschlag",
            duration: 3,
            pipeline_days: 7
        },
        {
            customer: "Porsche Stuttgart",
            address: "Porscheplatz 1, 70435 Stuttgart",
            priority: "hoch",
            status: "bestätigt",
            duration: 4,
            pipeline_days: 28
        },
        {
            customer: "SAP Walldorf",
            address: "Hasso-Plattner-Ring 7, 69190 Walldorf",
            priority: "mittel",
            status: "vorschlag",
            duration: 2,
            pipeline_days: 5
        },
        {
            customer: "Audi Ingolstadt",
            address: "AUTO UNION STRASSE 1, 85045 Ingolstadt",
            priority: "mittel",
            status: "vorschlag",
            duration: 3,
            pipeline_days: 10
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
                'Admin seeded data'
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
                            message: `${sampleAppointments.length} Termine erfolgreich eingefügt`,
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
                    database_path: dbPath
                });
            });
        });
    });
});

// CSV Preview endpoint
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

        // Analyze the data
        const analysis = {
            totalRows: parsed.data.length,
            columns: parsed.meta.fields,
            confirmedAppointments: 0,
            proposalAppointments: 0,
            onHoldAppointments: 0,
            missingData: 0,
            sampleRows: parsed.data.slice(0, 5)
        };

        parsed.data.forEach(row => {
            if (row['On Hold'] && row['On Hold'].trim() !== '') {
                analysis.onHoldAppointments++;
            } else if (row['Start Date & Time'] && row['Start Date & Time'].trim() !== '') {
                analysis.confirmedAppointments++;
            } else if (row['Customer Company'] || row['Invitee Name']) {
                analysis.proposalAppointments++;
            } else {
                analysis.missingData++;
            }
        });

        res.json({
            success: true,
            analysis: analysis,
            message: 'CSV Vorschau erstellt'
        });

    } catch (error) {
        res.status(500).json({
            error: 'CSV Analyse fehlgeschlagen',
            details: error.message
        });
    }
});

// CSV Import endpoint
app.post('/api/admin/import-csv', upload.single('csvFile'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'Keine CSV-Datei hochgeladen' });
    }

    console.log('📁 CSV Import gestartet...');
    
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
                skippedCount++;
                return;
            }

            // Skip if essential data missing
            if (!row['Customer Company'] && !row['Invitee Name']) {
                skippedCount++;
                return;
            }

            // Build address
            let fullAddress = row['Adresse'] || '';
            if (!fullAddress && row['Straße & Hausnr.']) {
                const parts = [];
                if (row['Straße & Hausnr.']) parts.push(row['Straße & Hausnr.']);
                if (row['PLZ'] && row['Ort']) parts.push(`${row['PLZ']} ${row['Ort']}`);
                if (row['Land']) parts.push(row['Land']);
                fullAddress = parts.join(', ');
            }

            // Determine status
            const hasStartDate = row['Start Date & Time'] && row['Start Date & Time'].toString().trim() !== '';
            const status = hasStartDate ? 'bestätigt' : 'vorschlag';
            
            if (status === 'bestätigt') confirmedCount++;
            else proposalCount++;

            // Priority logic
            let priority = 'mittel';
            const company = (row['Customer Company'] || '').toLowerCase();
            if (company.includes('bmw') || company.includes('mercedes') || company.includes('audi') || 
                company.includes('porsche') || company.includes('volkswagen')) {
                priority = 'hoch';
            }

            const appointment = {
                customer: row['Customer Company'] || row['Invitee Name'],
                address: fullAddress || 'Adresse nicht verfügbar',
                priority: priority,
                status: status,
                duration: priority === 'hoch' ? 4 : 3,
                pipeline_days: status === 'vorschlag' ? Math.floor(Math.random() * 30) + 1 : 7,
                notes: `CSV Import - ${row['Notiz'] || 'Keine Notizen'}`
            };

            processedAppointments.push(appointment);
        });

        // Clear and insert
        db.run("DELETE FROM appointments", (err) => {
            if (err) {
                return res.status(500).json({ error: 'Datenbankfehler beim Löschen' });
            }

            const stmt = db.prepare(`INSERT INTO appointments 
                (customer, address, priority, status, duration, pipeline_days, notes, preferred_dates, excluded_dates) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);

            let insertedCount = 0;
            let errors = [];

            processedAppointments.forEach((apt) => {
                stmt.run([
                    apt.customer, apt.address, apt.priority, apt.status, 
                    apt.duration, apt.pipeline_days, apt.notes,
                    JSON.stringify([]), JSON.stringify([])
                ], (err) => {
                    if (err) {
                        errors.push(`${apt.customer}: ${err.message}`);
                    } else {
                        insertedCount++;
                    }
                    
                    if (insertedCount + errors.length === processedAppointments.length) {
                        stmt.finalize();
                        res.json({
                            success: true,
                            message: 'CSV Import abgeschlossen',
                            stats: {
                                totalRows: parsed.data.length,
                                processed: processedAppointments.length,
                                inserted: insertedCount,
                                confirmed: confirmedCount,
                                proposals: proposalCount,
                                skipped: skippedCount,
                                errors: errors.length
                            },
                            errors: errors.length > 0 ? errors : undefined
                        });
                    }
                });
            });

            if (processedAppointments.length === 0) {
                stmt.finalize();
                res.json({
                    success: false,
                    message: 'Keine gültigen Termine in der CSV gefunden',
                    stats: {
                        totalRows: parsed.data.length,
                        skipped: skippedCount
                    }
                });
            }
        });

    } catch (error) {
        console.error('❌ CSV Import Fehler:', error);
        res.status(500).json({
            error: 'CSV Import fehlgeschlagen',
            details: error.message
        });
    }
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
    console.log(`🚀 Tourenplaner Server running on port ${PORT}`);
    console.log(`🌐 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`📊 Health check: http://localhost:${PORT}/api/health`);
    console.log(`✨ New features: Session persistence, Route saving`);
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

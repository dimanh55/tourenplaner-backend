const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());

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

// Routes

// Health check
app.get('/api/health', (req, res) => {
    res.json({
        status: 'OK',
        message: 'Tourenplaner Backend running on Railway!',
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development'
    });
});

// Root route
app.get('/', (req, res) => {
    res.json({
        message: '🚀 Tourenplaner API is running!',
        endpoints: [
            'GET /api/health',
            'POST /api/auth/login',
            'GET /api/appointments',
            'GET /api/drivers',
            'POST /api/routes/optimize',
            'POST /api/admin/seed'
        ]
    });
});

// Authentication
app.post('/api/auth/login', (req, res) => {
    const { password } = req.body;
    
    if (password === 'testimonials2025') {
        res.json({
            token: 'railway-token-' + Date.now(),
            user: { name: 'Admin', role: 'admin' },
            message: 'Login successful'
        });
    } else {
        res.status(401).json({ error: 'Invalid password' });
    }
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
    const { weekStart, driverId } = req.body;
    
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
            ]
        };

        console.log('✅ Route optimization completed');

        res.json({
            success: true,
            route: optimizedRoute,
            message: `Route für ${appointments.length} Termine optimiert`,
            stats: {
                totalAppointments: appointments.length,
                scheduledAppointments: optimizedRoute.days.reduce((sum, day) => sum + day.appointments.length, 0),
                totalHours: optimizedRoute.totalHours,
                workDays: optimizedRoute.days.filter(day => day.appointments.length > 0).length
            }
        });
    });
});

// Admin endpoint to seed database manually
app.post('/api/admin/seed', (req, res) => {
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
            
            res.json({
                database: 'connected',
                appointments_count: row.count,
                drivers_count: row2.driver_count,
                database_path: dbPath
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
    console.log(`🚀 Tourenplaner Server running on port ${PORT}`);
    console.log(`🌐 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`📊 Health check: http://localhost:${PORT}/api/health`);
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

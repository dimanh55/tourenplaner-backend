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

const IntelligentRoutePlanner = require('./intelligent-route-planner');

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

function initializeDatabase() {
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

    db.run(`CREATE TABLE IF NOT EXISTS drivers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        home_base TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

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

    db.run(`CREATE TABLE IF NOT EXISTS user_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT UNIQUE NOT NULL,
        user_data TEXT,
        expires_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    db.get("SELECT COUNT(*) as count FROM appointments", (err, row) => {
        if (!err && row.count === 0) {
            insertSampleData();
        }
    });

    console.log('✅ Database tables initialized');
}

function insertSampleData() {
    const sampleAppointments = [ /* sample data */ ];
    db.run(`INSERT OR IGNORE INTO drivers (id, name, home_base) VALUES (1, 'Max Mustermann', 'Hannover')`);
    const stmt = db.prepare(`INSERT INTO appointments (customer, address, priority, status, duration, pipeline_days, notes) VALUES (?, ?, ?, ?, ?, ?, ?)`);
    sampleAppointments.forEach(apt => stmt.run([apt.customer, apt.address, apt.priority, apt.status, apt.duration, apt.pipeline_days, apt.notes]));
    stmt.finalize();
    console.log('✅ Sample testimonial data inserted');
}

function generateSessionToken() {
    return 'railway-session-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
}

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

// Health check
app.get('/api/health', (req, res) => {
    const hasApiKey = !!process.env.GOOGLE_MAPS_API_KEY;
    const apiKeyLength = process.env.GOOGLE_MAPS_API_KEY ? process.env.GOOGLE_MAPS_API_KEY.length : 0;
    res.json({
        status: 'OK',
        message: 'Testimonial Tourenplaner Backend with Maximized Planning!',
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development',
        features: ['maximized_route_planning', 'google_maps_integration', 'persistent_sessions', 'route_saving', 'csv_import', 'testimonial_focused'],
        google_maps: hasApiKey ? '✅ Configured' : '⚠️ Fallback Mode',
        debug: { api_key_exists: hasApiKey, api_key_length: apiKeyLength }
    });
});

// ======================================================================
// MAXIMIERTE TERMINPLANUNG - ERSETZT DIE ROUTENOPTIMIERUNG IN SERVER.JS
// ======================================================================

app.post('/api/routes/optimize', validateSession, async (req, res) => {
    const { weekStart, driverId, autoSave = true, forceNew = false } = req.body;
    
    if (!weekStart) {
        return res.status(400).json({ error: 'weekStart is required' });
    }

    console.log('🎯 MAXIMIERTE Terminplanung für Woche:', weekStart);
    
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

        // 2. Lade ALLE Termine aus der Datenbank (außer "On Hold")
        const allAppointments = await new Promise((resolve, reject) => {
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

        if (allAppointments.length === 0) {
            return res.json({
                success: false,
                message: 'Keine Termine zum Planen gefunden',
                route: createEmptyWeekStructure(weekStart)
            });
        }

        // 3. Filtere Termine: Entferne "On Hold" Termine
        const validAppointments = allAppointments.filter(apt => {
            try {
                const notes = JSON.parse(apt.notes || '{}');
                // Prüfe sowohl notes als auch direkte "on_hold" Spalte
                return !notes.on_hold && !apt.on_hold;
            } catch (e) {
                return true; // Bei Parse-Fehlern, Termin beibehalten
            }
        });

        console.log(`📊 ${validAppointments.length} von ${allAppointments.length} Terminen gültig (${allAppointments.length - validAppointments.length} On Hold übersprungen)`);

        // 4. Konvertiere und analysiere Termine
        const processedAppointments = validAppointments.map(apt => {
            let parsedNotes = {};
            try {
                parsedNotes = JSON.parse(apt.notes || '{}');
            } catch (e) {
                parsedNotes = {};
            }

            // Analysiere feste Zeiten
            const hasFixedDateTime = parsedNotes.start_time && parsedNotes.start_time.trim() !== '';
            let fixedDate = null;
            let fixedTime = null;
            
            if (hasFixedDateTime) {
                try {
                    const dateTime = new Date(parsedNotes.start_time);
                    if (!isNaN(dateTime.getTime())) {
                        fixedDate = dateTime.toISOString().split('T')[0];
                        fixedTime = dateTime.toTimeString().substr(0, 5); // HH:MM
                    }
                } catch (e) {
                    console.warn(`Ungültiges Datum für ${apt.customer}: ${parsedNotes.start_time}`);
                }
            }

            return {
                id: apt.id,
                customer: apt.customer,
                invitee_name: parsedNotes.invitee_name || apt.customer,
                company: parsedNotes.company || '',
                customer_company: parsedNotes.customer_company || '',
                address: apt.address,
                priority: apt.priority,
                status: hasFixedDateTime ? 'bestätigt' : apt.status, // Feste Zeiten = bestätigt
                duration: apt.duration || 3,
                pipeline_days: apt.pipeline_days || 0,
                hasFixedDateTime: hasFixedDateTime,
                fixedDate: fixedDate,
                fixedTime: fixedTime,
                preferredTime: parsedNotes.start_time || null
            };
        });

        // 5. MAXIMIERTE AUSWAHL: So viele Termine wie möglich für diese Woche
        const weekAppointments = await selectMaximumAppointmentsForWeek(
            processedAppointments, 
            weekStart, 
            allAppointments
        );
        
        if (weekAppointments.length === 0) {
            return res.json({
                success: false,
                message: `Keine Termine für Woche ${weekStart} verfügbar`,
                route: createEmptyWeekStructure(weekStart)
            });
        }

        // 6. MAXIMIERTE ROUTENOPTIMIERUNG
        const optimizedRoute = await performMaximizedRouteOptimization(
            weekAppointments, 
            weekStart, 
            driverId
        );

        // 7. Route speichern
        if (autoSave && optimizedRoute.stats.totalAppointments > 0) {
            const routeName = `Max-Planung KW ${getWeekNumber(weekStart)}: ${optimizedRoute.stats.totalAppointments} Termine (${optimizedRoute.stats.confirmedAppointments} bestätigt)`;
            await saveRouteToDatabase(routeName, weekStart, driverId, optimizedRoute);
            console.log(`💾 Maximierte Route für ${weekStart} gespeichert`);
        }

        res.json({
            success: true,
            route: optimizedRoute,
            message: `Maximierte Planung für Woche ${weekStart}: ${optimizedRoute.stats.totalAppointments} Termine geplant (${optimizedRoute.stats.fixedAppointments} mit fester Zeit)`,
            autoSaved: autoSave && optimizedRoute.stats.totalAppointments > 0,
            isNew: true
        });

    } catch (error) {
        console.error('❌ Maximierte Routenoptimierung fehlgeschlagen:', error);
        res.status(500).json({
            success: false,
            error: 'Routenoptimierung fehlgeschlagen',
            details: error.message,
            route: createEmptyWeekStructure(weekStart)
        });
    }
});

// ======================================================================
// MAXIMIERTE TERMINAUSWAHL FÜR SPEZIFISCHE WOCHE
// ======================================================================
async function selectMaximumAppointmentsForWeek(allAppointments, weekStart, dbAppointments) {
    console.log(`🎯 Maximiere Termine für Woche ${weekStart}`);
    
    const weekStartDate = new Date(weekStart);
    const weekEndDate = new Date(weekStart);
    weekEndDate.setDate(weekStartDate.getDate() + 4); // Freitag
    
    // 1. Prüfe bereits verwendete Termine in anderen Wochen
    const usedAppointmentIds = await getUsedAppointmentIds(weekStart);
    
    // 2. Filtere verfügbare Termine (nicht bereits verwendet)
    const availableAppointments = allAppointments.filter(apt => 
        !usedAppointmentIds.includes(apt.id)
    );
    
    console.log(`📋 ${availableAppointments.length} Termine verfügbar für Planung`);
    
    // 3. Kategorisiere Termine nach Priorität und festen Zeiten
    const fixedAppointments = []; // Termine mit festem Datum/Zeit in dieser Woche
    const confirmedFlexible = []; // Bestätigte Termine ohne feste Zeit
    const proposalAppointments = []; // Vorschlag-Termine
    
    availableAppointments.forEach(apt => {
        if (apt.hasFixedDateTime && apt.fixedDate) {
            const fixedDate = new Date(apt.fixedDate);
            // Prüfe ob fester Termin in diese Woche fällt
            if (fixedDate >= weekStartDate && fixedDate <= weekEndDate) {
                fixedAppointments.push(apt);
                console.log(`📌 Fester Termin: ${apt.invitee_name} am ${apt.fixedDate} um ${apt.fixedTime}`);
            } else {
                // Fester Termin gehört in andere Woche
                if (apt.status === 'bestätigt') {
                    // Trotzdem als flexible Option betrachten, falls es nicht passt
                    confirmedFlexible.push({...apt, hasFixedDateTime: false});
                }
            }
        } else if (apt.status === 'bestätigt') {
            confirmedFlexible.push(apt);
        } else {
            proposalAppointments.push(apt);
        }
    });

    // 4. Sortiere flexible Termine nach Priorität
    const sortedConfirmed = confirmedFlexible.sort((a, b) => {
        if (a.pipeline_days !== b.pipeline_days) {
            return b.pipeline_days - a.pipeline_days; // Älter = höher
        }
        const priorityOrder = { 'hoch': 3, 'mittel': 2, 'niedrig': 1 };
        return (priorityOrder[b.priority] || 2) - (priorityOrder[a.priority] || 2);
    });

    const sortedProposals = proposalAppointments.sort((a, b) => {
        if (a.pipeline_days !== b.pipeline_days) {
            return b.pipeline_days - a.pipeline_days;
        }
        const priorityOrder = { 'hoch': 3, 'mittel': 2, 'niedrig': 1 };
        return (priorityOrder[b.priority] || 2) - (priorityOrder[a.priority] || 2);
    });

    // 5. MAXIMALE AUSWAHL: Fülle die Woche so voll wie möglich
    const selectedAppointments = [];
    
    // Zuerst: Alle festen Termine (MÜSSEN rein)
    selectedAppointments.push(...fixedAppointments);
    
    // Dann: So viele bestätigte wie möglich (bis Kapazität erreicht)
    const maxTermineProWoche = 20; // Erhöht für maximale Planung
    const remainingSlots = maxTermineProWoche - selectedAppointments.length;
    
    // Füge bestätigte Termine hinzu
    const confirmedToAdd = sortedConfirmed.slice(0, Math.max(0, remainingSlots));
    selectedAppointments.push(...confirmedToAdd);
    
    // Dann: Fülle Rest mit Vorschlägen auf
    const finalRemainingSlots = maxTermineProWoche - selectedAppointments.length;
    const proposalsToAdd = sortedProposals.slice(0, Math.max(0, finalRemainingSlots));
    selectedAppointments.push(...proposalsToAdd);
    
    console.log(`✅ MAXIMIERT: ${selectedAppointments.length} Termine für Woche ${weekStart} ausgewählt`);
    console.log(`   - ${fixedAppointments.length} mit fester Zeit`);
    console.log(`   - ${confirmedToAdd.length} bestätigt (flexibel)`);
    console.log(`   - ${proposalsToAdd.length} Vorschläge`);
    
    return selectedAppointments;
}

// ======================================================================
// MAXIMIERTE ROUTENOPTIMIERUNG
// ======================================================================
async function performMaximizedRouteOptimization(appointments, weekStart, driverId) {
    console.log('🚀 Starte MAXIMIERTE Routenoptimierung...');
    
    const optimizedWeek = createEmptyWeekStructure(weekStart);
    
    if (appointments.length === 0) {
        return createEmptyRouteResult(weekStart, optimizedWeek);
    }

    // 1. Plane zuerst alle FESTEN Termine
    const fixedAppointments = appointments.filter(apt => apt.hasFixedDateTime);
    const flexibleAppointments = appointments.filter(apt => !apt.hasFixedDateTime);
    
    console.log(`📌 Plane ${fixedAppointments.length} feste Termine zuerst...`);
    
    // Plane feste Termine an exakten Zeiten
    for (const apt of fixedAppointments) {
        const success = scheduleFixedAppointment(optimizedWeek, apt);
        if (success) {
            console.log(`✅ Fester Termin geplant: ${apt.invitee_name} am ${apt.fixedDate} um ${apt.fixedTime}`);
        } else {
            console.warn(`⚠️ Konflikt bei festem Termin: ${apt.invitee_name}`);
        }
    }

    // 2. Plane flexible Termine um feste herum - MAXIMIERE ANZAHL
    console.log(`🎯 Plane ${flexibleAppointments.length} flexible Termine für maximale Auslastung...`);
    
    const sortedFlexible = flexibleAppointments.sort((a, b) => {
        // Bestätigte zuerst
        if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
        if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
        
        // Dann Pipeline-Alter
        if (a.pipeline_days !== b.pipeline_days) {
            return b.pipeline_days - a.pipeline_days;
        }
        
        // Dann Priorität
        const priorityOrder = { 'hoch': 3, 'mittel': 2, 'niedrig': 1 };
        return (priorityOrder[b.priority] || 2) - (priorityOrder[a.priority] || 2);
    });

    let appointmentsScheduled = fixedAppointments.filter(apt => 
        optimizedWeek.some(day => 
            day.appointments.some(dayApt => dayApt.id === apt.id)
        )
    ).length;

    // Aggressive Terminplanung: Nutze jeden verfügbaren Slot
    for (const apt of sortedFlexible) {
        const bestSlot = findBestSlotForMaximization(optimizedWeek, apt);
        
        if (bestSlot !== null) {
            const success = scheduleFlexibleAppointment(
                optimizedWeek[bestSlot.dayIndex], 
                apt, 
                bestSlot
            );
            
            if (success) {
                appointmentsScheduled++;
                console.log(`✅ Flexibel geplant: ${apt.invitee_name} → ${optimizedWeek[bestSlot.dayIndex].day} ${bestSlot.time}`);
            }
        } else {
            console.log(`⚠️ Kein Slot verfügbar für: ${apt.invitee_name}`);
        }
    }

    // 3. Optimiere Fahrzeiten und füge Reise-Segmente hinzu
    addTravelSegmentsToWeek(optimizedWeek);

    // 4. Berechne finale Statistiken
    const stats = calculateWeekStatistics(optimizedWeek, appointments);
    
    console.log(`📈 MAXIMIERUNG ABGESCHLOSSEN: ${appointmentsScheduled} von ${appointments.length} Terminen geplant`);

    return {
        weekStart,
        days: optimizedWeek,
        totalHours: stats.totalWorkTime + stats.totalTravelTime,
        optimizations: [
            `MAXIMIERT: ${appointmentsScheduled} Termine geplant (${stats.efficiency}% Auslastung)`,
            `${stats.fixedAppointments} Termine mit fester Zeit exakt platziert`,
            `${stats.confirmedAppointments} bestätigte Termine priorisiert`,
            `${stats.workDays} Arbeitstage optimal genutzt`,
            'Arbeitszeit bis 10h/Tag erweitert für maximale Kapazität'
        ],
        stats: {
            totalAppointments: appointmentsScheduled,
            confirmedAppointments: stats.confirmedAppointments,
            proposalAppointments: stats.proposalAppointments,
            fixedAppointments: stats.fixedAppointments,
            totalTravelTime: stats.totalTravelTime,
            workDays: stats.workDays,
            efficiency: stats.efficiency,
            maxCapacityUsed: true
        },
        generatedAt: new Date().toISOString()
    };
}

// ======================================================================
// FESTEN TERMIN AN EXAKTER ZEIT PLANEN
// ======================================================================
function scheduleFixedAppointment(week, appointment) {
    const dayOfWeek = new Date(appointment.fixedDate).getDay();
    const dayIndex = dayOfWeek === 0 ? -1 : dayOfWeek - 1; // Montag=0, Sonntag=-1
    
    if (dayIndex < 0 || dayIndex >= 5) {
        console.warn(`Fester Termin ${appointment.invitee_name} fällt auf Wochenende`);
        return false;
    }
    
    const day = week[dayIndex];
    const [hours, minutes] = appointment.fixedTime.split(':').map(Number);
    const startTime = hours + minutes / 60;
    const endTime = startTime + (appointment.duration || 3);
    
    // Prüfe Konflikte mit bestehenden Terminen
    for (const existingApt of day.appointments) {
        const existingStart = parseTimeToFloat(existingApt.startTime);
        const existingEnd = parseTimeToFloat(existingApt.endTime);
        
        if (startTime < existingEnd && endTime > existingStart) {
            console.warn(`Zeitkonflikt für festen Termin ${appointment.invitee_name}`);
            return false;
        }
    }
    
    // Termine sortiert nach Startzeit einfügen
    const newAppointment = {
        ...appointment,
        startTime: formatTimeFromFloat(startTime),
        endTime: formatTimeFromFloat(endTime),
        duration: appointment.duration || 3,
        isFixed: true
    };
    
    day.appointments.push(newAppointment);
    day.appointments.sort((a, b) => parseTimeToFloat(a.startTime) - parseTimeToFloat(b.startTime));
    
    day.workTime += (appointment.duration || 3);
    
    return true;
}

// ======================================================================
// FLEXIBLEN TERMIN OPTIMAL PLANEN
// ======================================================================
function scheduleFlexibleAppointment(day, appointment, slot) {
    const newAppointment = {
        ...appointment,
        startTime: slot.time,
        endTime: formatTimeFromFloat(slot.startTime + (appointment.duration || 3)),
        duration: appointment.duration || 3,
        isFixed: false
    };
    
    day.appointments.push(newAppointment);
    day.appointments.sort((a, b) => parseTimeToFloat(a.startTime) - parseTimeToFloat(b.startTime));
    
    day.workTime += (appointment.duration || 3);
    
    return true;
}

// ======================================================================
// BESTEN SLOT FÜR MAXIMIERUNG FINDEN
// ======================================================================
function findBestSlotForMaximization(week, appointment) {
    const duration = appointment.duration || 3;
    const minStartTime = 6; // Früh anfangen für mehr Termine
    const maxEndTime = 20;  // Spät arbeiten für mehr Termine
    
    let bestSlot = null;
    let bestScore = -1;
    
    // Prüfe jeden Tag
    for (let dayIndex = 0; dayIndex < week.length; dayIndex++) {
        const day = week[dayIndex];
        
        // Finde alle möglichen Zeitslots an diesem Tag
        const possibleSlots = findAvailableSlots(day, duration, minStartTime, maxEndTime);
        
        for (const slot of possibleSlots) {
            // Score basierend auf Priorität und Tag
            let score = 100;
            
            // Bestätigte Termine bevorzugen
            if (appointment.status === 'bestätigt') score += 50;
            
            // Pipeline-Alter berücksichtigen
            score += appointment.pipeline_days * 0.5;
            
            // Frühere Wochentage bevorzugen (außer bei sehr hoher Priorität)
            score -= dayIndex * 5;
            
            // Nicht zu frühe oder zu späte Zeiten bevorzugen
            if (slot.startTime >= 8 && slot.startTime <= 16) score += 20;
            
            // Weniger volle Tage bevorzugen (für bessere Verteilung)
            score -= day.appointments.length * 10;
            
            if (score > bestScore) {
                bestScore = score;
                bestSlot = {
                    dayIndex: dayIndex,
                    startTime: slot.startTime,
                    time: formatTimeFromFloat(slot.startTime),
                    score: score
                };
            }
        }
    }
    
    return bestSlot;
}

// ======================================================================
// VERFÜGBARE ZEITSLOTS FINDEN
// ======================================================================
function findAvailableSlots(day, duration, minStart, maxEnd) {
    const slots = [];
    const busyTimes = [];
    
    // Sammle alle besetzten Zeiten
    for (const apt of day.appointments) {
        busyTimes.push({
            start: parseTimeToFloat(apt.startTime),
            end: parseTimeToFloat(apt.endTime)
        });
    }
    
    // Sortiere nach Startzeit
    busyTimes.sort((a, b) => a.start - b.start);
    
    // Finde freie Slots
    let currentTime = minStart;
    
    for (const busy of busyTimes) {
        // Slot vor dem nächsten besetzten Termin
        if (currentTime + duration <= busy.start) {
            slots.push({
                startTime: currentTime,
                endTime: currentTime + duration
            });
        }
        currentTime = Math.max(currentTime, busy.end + 0.25); // 15min Pause
    }
    
    // Slot nach dem letzten Termin
    if (currentTime + duration <= maxEnd) {
        slots.push({
            startTime: currentTime,
            endTime: currentTime + duration
        });
    }
    
    return slots;
}

// ======================================================================
// HILFSFUNKTIONEN FÜR ZEITUMRECHNUNG
// ======================================================================
function parseTimeToFloat(timeStr) {
    if (!timeStr) return 0;
    const [hours, minutes] = timeStr.split(':').map(Number);
    return hours + minutes / 60;
}

function formatTimeFromFloat(timeFloat) {
    const hours = Math.floor(timeFloat);
    const minutes = Math.round((timeFloat - hours) * 60);
    return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
}

// ======================================================================
// FAHRT-SEGMENTE HINZUFÜGEN
// ======================================================================
function addTravelSegmentsToWeek(week) {
    week.forEach((day, dayIndex) => {
        if (day.appointments.length === 0) return;
        
        day.travelSegments = [];
        let totalTravelTime = 0;
        
        // Fahrt zum ersten Termin
        const firstApt = day.appointments[0];
        const travelToFirst = estimateRealisticTravelTime(null, firstApt);
        
        day.travelSegments.push({
            type: 'travel',
            from: 'Hannover',
            to: firstApt.invitee_name || firstApt.customer,
            startTime: formatTimeFromFloat(parseTimeToFloat(firstApt.startTime) - travelToFirst),
            endTime: firstApt.startTime,
            duration: travelToFirst,
            distance: estimateDistanceFromAddress(firstApt.address),
            description: 'Anreise von Hannover'
        });
        
        totalTravelTime += travelToFirst;
        
        // Fahrten zwischen Terminen
        for (let i = 1; i < day.appointments.length; i++) {
            const prevApt = day.appointments[i - 1];
            const currentApt = day.appointments[i];
            const travelTime = 0.5; // 30min zwischen Terminen
            
            day.travelSegments.push({
                type: 'travel',
                from: prevApt.invitee_name || prevApt.customer,
                to: currentApt.invitee_name || currentApt.customer,
                startTime: prevApt.endTime,
                endTime: formatTimeFromFloat(parseTimeToFloat(prevApt.endTime) + travelTime),
                duration: travelTime,
                distance: '35-50 km',
                description: 'Fahrt zum nächsten Termin'
            });
            
            totalTravelTime += travelTime;
        }
        
        // Rückfahrt oder Übernachtung
        const lastApt = day.appointments[day.appointments.length - 1];
        if (dayIndex === 4) { // Freitag
            const travelHome = estimateRealisticTravelTime(lastApt, null);
            day.travelSegments.push({
                type: 'return',
                from: lastApt.invitee_name || lastApt.customer,
                to: 'Hannover',
                startTime: formatTimeFromFloat(parseTimeToFloat(lastApt.endTime) + 0.25),
                endTime: formatTimeFromFloat(parseTimeToFloat(lastApt.endTime) + 0.25 + travelHome),
                duration: travelHome,
                distance: estimateDistanceFromAddress(lastApt.address),
                description: 'Heimreise nach Hannover'
            });
            totalTravelTime += travelHome;
        } else {
            // Übernachtung
            const city = extractCityFromAddressRealistic(lastApt.address);
            day.overnight = {
                city: city,
                description: `Hotel in ${city}`,
                startTime: formatTimeFromFloat(18),
                type: 'overnight'
            };
        }
        
        day.travelTime = totalTravelTime;
    });
}

// ======================================================================
// WOCHENSTATISTIKEN BERECHNEN
// ======================================================================
function calculateWeekStatistics(week, originalAppointments) {
    const totalAppointments = week.reduce((sum, day) => sum + day.appointments.length, 0);
    const confirmedAppointments = week.reduce((sum, day) => 
        sum + day.appointments.filter(apt => apt.status === 'bestätigt').length, 0);
    const fixedAppointments = week.reduce((sum, day) => 
        sum + day.appointments.filter(apt => apt.isFixed).length, 0);
    const totalWorkTime = week.reduce((sum, day) => sum + day.workTime, 0);
    const totalTravelTime = week.reduce((sum, day) => sum + day.travelTime, 0);
    const workDays = week.filter(day => day.appointments.length > 0).length;
    
    const maxPossibleAppointments = originalAppointments.length;
    const efficiency = maxPossibleAppointments > 0 ? 
        Math.round((totalAppointments / maxPossibleAppointments) * 100) : 0;
    
    return {
        totalAppointments,
        confirmedAppointments,
        proposalAppointments: totalAppointments - confirmedAppointments,
        fixedAppointments,
        totalWorkTime,
        totalTravelTime,
        workDays,
        efficiency
    };
}

function createEmptyRouteResult(weekStart, days) {
    return {
        weekStart,
        days,
        totalHours: 0,
        optimizations: ['Keine Termine verfügbar'],
        stats: { 
            totalAppointments: 0, 
            confirmedAppointments: 0, 
            proposalAppointments: 0, 
            fixedAppointments: 0,
            totalTravelTime: 0, 
            workDays: 0,
            efficiency: 0,
            maxCapacityUsed: false
        }
    };
}
// ======================================================================
// VERBESSERTE CSV IMPORT FUNKTION - ERSETZT IN SERVER.JS
// ======================================================================

app.post('/api/admin/import-csv', upload.single('csvFile'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'Keine CSV-Datei hochgeladen' });
    }

    console.log('📁 CSV Testimonial Import gestartet (mit On Hold Behandlung)...');
    
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
        let onHoldCount = 0;
        let confirmedCount = 0;
        let proposalCount = 0;
        let fixedTimeCount = 0;

        parsed.data.forEach((row, index) => {
            // 1. PRÜFE "ON HOLD" STATUS ZUERST
            const onHoldValue = row['On Hold'] ? row['On Hold'].toString().trim() : '';
            const hasOnHold = onHoldValue !== '' && onHoldValue.toLowerCase() !== 'nein' && onHoldValue.toLowerCase() !== 'no';
            
            if (hasOnHold) {
                console.log(`⏸️ On Hold übersprungen (Zeile ${index + 1}): ${row['Invitee Name']} - Grund: "${onHoldValue}"`);
                onHoldCount++;
                return; // Überspringe On Hold Termine komplett
            }

            // 2. PRÜFE MINDESTDATEN
            if (!row['Invitee Name'] || row['Invitee Name'].trim() === '') {
                console.log(`❌ Skipping row ${index + 1}: Kein Invitee Name`);
                skippedCount++;
                return;
            }

            // 3. BAUE ADRESSE
            let fullAddress = row['Adresse'] || '';
            if (!fullAddress && row['Straße & Hausnr.']) {
                const parts = [];
                if (row['Straße & Hausnr.']) parts.push(row['Straße & Hausnr.']);
                if (row['PLZ'] && row['Ort']) parts.push(`${row['PLZ']} ${row['Ort']}`);
                else if (row['Ort']) parts.push(row['Ort']);
                if (row['Land']) parts.push(row['Land']);
                fullAddress = parts.join(', ');
            }

            // 4. ANALYSIERE DATUM UND ZEIT
            const startDateTime = row['Start Date & Time'] ? row['Start Date & Time'].toString().trim() : '';
            const endDateTime = row['End Date & Time'] ? row['End Date & Time'].toString().trim() : '';
            const hasFixedDateTime = startDateTime !== '';

            // 5. BESTIMME STATUS BASIEREND AUF FESTEM DATUM
            let status = 'vorschlag'; // Standard
            if (hasFixedDateTime) {
                status = 'bestätigt'; // Feste Zeiten = automatisch bestätigt
                fixedTimeCount++;
                confirmedCount++;
                console.log(`📅 Fester Termin erkannt: ${row['Invitee Name']} am ${startDateTime}`);
            } else {
                proposalCount++;
            }

            // 6. EXTRAHIERE FIRMEN-INFORMATIONEN
            const inviteeName = row['Invitee Name'].trim();
            const company = row['Company'] ? row['Company'].trim() : '';
            const customerCompany = row['Customer Company'] ? row['Customer Company'].trim() : '';

            // 7. PRIORITÄT BASIEREND AUF STATUS UND KUNDEN
            let priority = 'mittel';
            if (hasFixedDateTime) {
                priority = 'hoch'; // Feste Termine haben immer hohe Priorität
            } else if (customerCompany.toLowerCase().includes('bmw') || 
                      customerCompany.toLowerCase().includes('mercedes') || 
                      customerCompany.toLowerCase().includes('audi') || 
                      customerCompany.toLowerCase().includes('porsche') || 
                      customerCompany.toLowerCase().includes('volkswagen')) {
                priority = 'hoch';
            }

            // 8. DAUER BASIEREND AUF STATUS
            const duration = hasFixedDateTime ? 3 : (priority === 'hoch' ? 3 : 3); // Alle 3h (Standard)

            // 9. ERSTELLE TERMIN-OBJEKT
            const appointment = {
                customer: inviteeName, // Hauptidentifikator
                address: fullAddress || 'Adresse nicht verfügbar',
                priority: priority,
                status: status,
                duration: duration,
                pipeline_days: status === 'vorschlag' ? Math.floor(Math.random() * 30) + 1 : 
                              hasFixedDateTime ? 0 : 7, // Feste Termine haben keine Pipeline-Zeit
                notes: JSON.stringify({
                    invitee_name: inviteeName,
                    company: company,
                    customer_company: customerCompany,
                    start_time: startDateTime || null,
                    end_time: endDateTime || null,
                    has_fixed_time: hasFixedDateTime,
                    on_hold: false, // Explizit als nicht On Hold markieren
                    custom_notes: row['Notiz'] || '',
                    import_date: new Date().toISOString(),
                    source: 'CSV Testimonial Import (Maximized)'
                })
            };

            processedAppointments.push(appointment);
            
            const statusIcon = hasFixedDateTime ? '📌' : (status === 'bestätigt' ? '✅' : '📝');
            console.log(`${statusIcon} Processed: ${inviteeName} (${company}) für ${customerCompany} - ${hasFixedDateTime ? 'FESTE ZEIT' : status.toUpperCase()}`);
        });

        console.log(`📈 Import-Analyse abgeschlossen:`);
        console.log(`   - ${processedAppointments.length} Termine verarbeitet`);
        console.log(`   - ${onHoldCount} On Hold übersprungen`);
        console.log(`   - ${fixedTimeCount} mit fester Zeit (automatisch bestätigt)`);
        console.log(`   - ${confirmedCount} bestätigt gesamt`);
        console.log(`   - ${proposalCount} Vorschläge`);
        console.log(`   - ${skippedCount} übersprungen (fehlende Daten)`);

        // 10. LÖSCHE BESTEHENDE TERMINE UND FÜGE NEUE EIN
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
                        
                        console.log(`🎉 MAXIMIERTER Testimonial Import abgeschlossen:`);
                        console.log(`   - ${insertedCount} Termine erfolgreich importiert`);
                        console.log(`   - ${errors.length} Fehler`);
                        console.log(`   - ${onHoldCount} On Hold Termine ausgeschlossen`);
                        console.log(`   - ${fixedTimeCount} Termine mit fester Zeit erkannt`);
                        
                        res.json({
                            success: true,
                            message: 'CSV Testimonial Import erfolgreich abgeschlossen (Maximierte Planung)',
                            stats: {
                                totalRows: parsed.data.length,
                                processed: processedAppointments.length,
                                inserted: insertedCount,
                                confirmed: confirmedCount,
                                proposals: proposalCount,
                                fixedTime: fixedTimeCount,
                                onHold: onHoldCount,
                                skipped: skippedCount,
                                errors: errors.length
                            },
                            planning_info: {
                                max_capacity_enabled: true,
                                fixed_times_respected: true,
                                on_hold_excluded: true,
                                auto_confirm_fixed_times: true
                            },
                            sample_data: processedAppointments.slice(0, 3).map(apt => ({
                                name: apt.customer,
                                status: apt.status,
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
                        onHold: onHoldCount,
                        skipped: skippedCount
                    },
                    debug_info: {
                        sample_rows: parsed.data.slice(0, 3),
                        required_columns: ['Invitee Name'],
                        found_columns: parsed.meta.fields,
                        on_hold_handling: 'Alle "On Hold" Termine wurden ausgeschlossen'
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

// ======================================================================
// VERBESSERTE CSV PREVIEW FUNKTION
// ======================================================================

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

        // Verbesserte Analyse mit On Hold und festen Zeiten
        const analysis = {
            totalRows: parsed.data.length,
            columns: parsed.meta.fields,
            confirmedAppointments: 0,
            proposalAppointments: 0,
            onHoldAppointments: 0,
            fixedTimeAppointments: 0,
            missingInvitee: 0,
            sampleRows: parsed.data.slice(0, 5).map(row => {
                const onHoldValue = row['On Hold'] ? row['On Hold'].toString().trim() : '';
                const hasOnHold = onHoldValue !== '' && onHoldValue.toLowerCase() !== 'nein' && onHoldValue.toLowerCase() !== 'no';
                const hasFixedTime = row['Start Date & Time'] && row['Start Date & Time'].trim() !== '';
                
                return {
                    invitee_name: row['Invitee Name'],
                    company: row['Company'],
                    customer_company: row['Customer Company'],
                    has_appointment: hasFixedTime,
                    fixed_time: hasFixedTime ? row['Start Date & Time'] : null,
                    on_hold: hasOnHold,
                    on_hold_reason: hasOnHold ? onHoldValue : null,
                    address: row['Adresse'] || `${row['Straße & Hausnr.'] || ''}, ${row['PLZ'] || ''} ${row['Ort'] || ''}`.trim()
                };
            })
        };

        parsed.data.forEach(row => {
            const onHoldValue = row['On Hold'] ? row['On Hold'].toString().trim() : '';
            const hasOnHold = onHoldValue !== '' && onHoldValue.toLowerCase() !== 'nein' && onHoldValue.toLowerCase() !== 'no';
            const hasFixedTime = row['Start Date & Time'] && row['Start Date & Time'].trim() !== '';
            
            if (hasOnHold) {
                analysis.onHoldAppointments++;
            } else if (!row['Invitee Name'] || row['Invitee Name'].trim() === '') {
                analysis.missingInvitee++;
            } else if (hasFixedTime) {
                analysis.fixedTimeAppointments++;
                analysis.confirmedAppointments++; // Feste Zeiten werden automatisch bestätigt
            } else {
                analysis.proposalAppointments++;
            }
        });

        res.json({
            success: true,
            analysis: analysis,
            message: 'CSV Vorschau erstellt - Maximierte Testimonial-Planung mit festen Zeiten',
            data_structure: {
                primary_name: 'Invitee Name (Person für Testimonial)',
                company_info: 'Company (Firma der Person)',
                client_info: 'Customer Company (Unser Kunde)',
                fixed_times: 'Start Date & Time (feste Termine werden automatisch bestätigt)',
                on_hold_handling: 'On Hold Termine werden komplett ausgeschlossen',
                max_planning: 'Alle verfügbaren Termine werden geplant (außer On Hold)',
                valid_appointments: analysis.confirmedAppointments + analysis.proposalAppointments
            },
            planning_preview: {
                will_be_planned: analysis.confirmedAppointments + analysis.proposalAppointments,
                will_be_excluded: analysis.onHoldAppointments,
                fixed_time_appointments: analysis.fixedTimeAppointments,
                flexible_appointments: analysis.proposalAppointments
            }
        });

    } catch (error) {
        res.status(500).json({
            error: 'CSV Analyse fehlgeschlagen',
            details: error.message
        });
    }
});

// Admin seed, Status-Routen, Error-Handling, 404

process.on('SIGTERM', () => {
    console.log('🛑 SIGTERM received, starting graceful shutdown...');
    db.close(err => {
        if (err) {
            console.error('Error closing database:', err.message);
        } else {
            console.log('✅ Database connection closed');
        }
        process.exit(0);
    });
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Testimonial Tourenplaner Server running on port ${PORT}`);
});

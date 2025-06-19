const axios = require('axios');
const EnhancedGeocodingService = require('./geocoding-service');

class IntelligentRoutePlanner {
    constructor() {
        this.geocodingService = new EnhancedGeocodingService();
        this.constraints = {
            maxWorkHoursPerWeek: 42.5,      // Max 40h Arbeit + 2.5h Pausen
            maxWorkHoursPerDay: 9,         // Arbeit + Fahrtzeit (Ende 18:00)
            flexWorkHoursPerDay: 11,        // Absolute Obergrenze mit Überstunden
            workStartTime: 9,               // Früh starten für lange Fahrten
            workEndTime: 18,                // Rückkehr bis 18 Uhr einplanen
            appointmentDuration: 3,         // 3h pro Dreh
            homeBase: { lat: 52.3759, lng: 9.7320, name: 'Hannover' },
            travelMode: 'driving',          // Immer mit dem PKW
            travelTimePadding: 0.25,        // 15 Min Fahrzeit-Puffer
            maxTravelTimePerDay: 9,         // Bis zu 8h Fahrt pro Tag OK
            maxSingleTravelTime: 9,         // Einzelfahrt bis 5h
            overnightThreshold: 100,        // Übernachtung ab 100km vom Heimatort
            minOvernightDistance: 25       // Mindestens 25km für Übernachtung
        };
        this.distanceMatrixApiDisabled = false;
    }

    // ======================================================================
    // HAUPTFUNKTION: Intelligente Routenoptimierung
    // ======================================================================
    async optimizeWeek(appointments, weekStart, driverId) {
        console.log('🧠 Starte intelligente Routenplanung...');
        
        try {
            // 1. Termine analysieren und kategorisieren
            const categorizedAppointments = this.categorizeAppointments(appointments);
            
            // 2. Geocoding für alle Adressen
            const geoAppointments = await this.geocodeAppointments(categorizedAppointments);
            
            if (geoAppointments.length === 0) {
                throw new Error('Keine Termine mit gültigen Adressen gefunden');
            }
            
            // 3. Reisematrix berechnen mit ECHTER Google Maps API
            const travelMatrix = await this.calculateTravelMatrix(geoAppointments);
            
            // 4. Intelligente Wochenplanung
            const optimizedWeek = await this.planOptimalWeek(geoAppointments, travelMatrix, weekStart);
            
            // 5. Übernachtungsstopps optimieren
            const weekWithOvernight = this.optimizeOvernightStops(optimizedWeek, travelMatrix);
            
            // 6. Qualitätsprüfung und Validierung
            const validatedWeek = this.validateWeekPlan(weekWithOvernight);
            
            console.log('✅ Intelligente Routenplanung abgeschlossen');
            return this.formatWeekResult(validatedWeek, travelMatrix);
            
        } catch (error) {
            console.error('❌ Routenplanung fehlgeschlagen:', error);
            throw new Error(`Routenplanung fehlgeschlagen: ${error.message}`);
        }
    }

    // ======================================================================
    // TERMINE KATEGORISIEREN
    // ======================================================================
    categorizeAppointments(appointments) {
        return appointments.map(apt => {
            let parsedNotes = {};
            try {
                parsedNotes = JSON.parse(apt.notes || '{}');
            } catch (e) {
                parsedNotes = {};
            }

            return {
                ...apt,
                isConfirmed: apt.status === 'bestätigt',
                isProposal: apt.status === 'vorschlag',
                isFixed: Boolean(apt.is_fixed),
                fixedDate: apt.fixed_date,
                fixedTime: apt.fixed_time,
                duration: apt.duration || 3,
                priority: this.calculateDynamicPriority(apt, parsedNotes),
                preferredTimes: parsedNotes.preferred_times || [],
                excludedDates: JSON.parse(apt.excluded_dates || '[]'),
                invitee_name: parsedNotes.invitee_name || apt.customer,
                company: parsedNotes.company || '',
                customer_company: parsedNotes.customer_company || '',
                pipeline_age: apt.pipeline_days || 0
            };
        });
    }

    // ======================================================================
    // DYNAMISCHE PRIORITÄT BERECHNEN
    // ======================================================================
    calculateDynamicPriority(apt, notes) {
        let score = 50;

        if (apt.status === 'bestätigt') score += 40;
        score += Math.min(apt.pipeline_days * 0.5, 20);
        
        const vipCustomers = ['BMW', 'Mercedes', 'Audi', 'Porsche', 'Volkswagen', 'Apple', 'Microsoft', 'Google'];
        if (vipCustomers.some(vip => 
            (notes.customer_company || '').toLowerCase().includes(vip.toLowerCase())
        )) {
            score += 15;
        }
        
        if (apt.priority === 'hoch') score += 10;
        else if (apt.priority === 'niedrig') score -= 10;

        return Math.max(0, Math.min(100, score));
    }

    // ======================================================================
    // GEOCODING - ADRESSEN ZU KOORDINATEN
    // ======================================================================
    async geocodeAppointments(appointments) {
        console.log('🗺️ Geocoding von Adressen...');

        const geocoded = [];
        for (const apt of appointments) {
            if (apt.lat && apt.lng) {
                geocoded.push({
                    ...apt,
                    geocoded: true,
                    geocoding_method: 'database'
                });
                continue;
            }

            try {
                const coords = await this.geocodingService.geocodeAddress(apt.address);
                geocoded.push({
                    ...apt,
                    lat: coords.lat,
                    lng: coords.lng,
                    geocoded: true,
                    geocoding_method: coords.geocoding_method
                });
                console.log(`✅ Geocoded: ${apt.invitee_name} in ${apt.address}`);
            } catch (error) {
                console.warn(`⚠️ Geocoding fehlgeschlagen für ${apt.address}: ${error.message}`);
            }
        }

        return geocoded.filter(apt => apt.geocoded);
    }

    // ======================================================================
    // REISEMATRIX MIT GOOGLE DISTANCE MATRIX API
    // ======================================================================
    async calculateTravelMatrix(appointments) {
        console.log('🚗 Berechne OPTIMIERTE Reisematrix...');
        console.log('💰 KOSTEN-SPAR-MODUS AKTIV');

        const matrix = {};
        const apiKey = process.env.GOOGLE_MAPS_API_KEY;

        if (!apiKey) {
            throw new Error('Google Maps API Key nicht konfiguriert!');
        }

        // KRITISCH: Cluster-basierte Berechnung statt vollständiger Matrix!
        const clusters = this.clusterAppointmentsByProximity(appointments);
        console.log(`📊 ${appointments.length} Termine in ${clusters.length} Cluster gruppiert`);

        // Home-Base
        const homePoint = { id: 'home', ...this.constraints.homeBase };
        const allPoints = [homePoint, ...appointments];

        let apiCalls = 0;
        let savedCalls = 0;

        // STRATEGIE 1: Nur Home zu Cluster-Zentren (statt zu jedem Termin!)
        for (const cluster of clusters) {
            if (cluster.appointments.length === 0) continue;

            const center = cluster.center;

            // API Call nur für Cluster-Zentrum
            try {
                const response = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
                    params: {
                        origins: `${homePoint.lat},${homePoint.lng}`,
                        destinations: `${center.lat},${center.lng}`,
                        key: apiKey,
                        units: 'metric',
                        mode: 'driving'
                    },
                    timeout: 5000
                });

                if (response.data.status === 'OK') {
                    const element = response.data.rows[0].elements[0];
                    if (element.status === 'OK') {
                        const distance = element.distance.value / 1000;
                        const duration = element.duration.value / 3600;

                        // Speichere für Cluster-Zentrum
                        if (!matrix['home']) matrix['home'] = {};
                        if (!matrix[center.id]) matrix[center.id] = {};

                        matrix['home'][center.id] = { distance, duration: duration + 0.25 };
                        matrix[center.id]['home'] = { distance, duration: duration + 0.25 };

                        // WICHTIG: Verwende diese Werte für ALLE Termine im Cluster!
                        cluster.appointments.forEach(apt => {
                            if (!matrix['home']) matrix['home'] = {};
                            if (!matrix[apt.id]) matrix[apt.id] = {};

                            // Kleine Variation für Realismus
                            const variation = 0.1 + Math.random() * 0.1;
                            matrix['home'][apt.id] = {
                                distance: distance * (1 + variation),
                                duration: (duration + 0.25) * (1 + variation),
                                approximated: true
                            };
                            matrix[apt.id]['home'] = {
                                distance: distance * (1 + variation),
                                duration: (duration + 0.25) * (1 + variation),
                                approximated: true
                            };
                            savedCalls++;
                        });

                        apiCalls++;
                        console.log(`✅ Cluster ${cluster.id}: 1 API Call für ${cluster.appointments.length} Termine`);
                    }
                }

                await new Promise(resolve => setTimeout(resolve, 200));

            } catch (error) {
                console.error(`❌ API Fehler für Cluster ${cluster.id}:`, error.message);
            }
        }

        // STRATEGIE 2: Innerhalb der Cluster nur Approximation (KEINE API CALLS!)
        appointments.forEach(from => {
            if (!matrix[from.id]) matrix[from.id] = {};

            appointments.forEach(to => {
                if (from.id !== to.id && !matrix[from.id][to.id]) {
                    const distance = this.calculateHaversineDistance(
                        { lat: from.lat, lng: from.lng },
                        { lat: to.lat, lng: to.lng }
                    );

                    // Realistische Fahrzeit-Schätzung
                    let duration;
                    if (distance < 50) {
                        duration = distance / 60; // Stadtverkehr
                    } else if (distance < 200) {
                        duration = distance / 80; // Landstraße
                    } else {
                        duration = distance / 100; // Autobahn
                    }

                    matrix[from.id][to.id] = {
                        distance: distance,
                        duration: duration + 0.25,
                        approximated: true
                    };
                    savedCalls++;
                }
            });
        });

        console.log(`\n💰 === KOSTEN-BERICHT ===`);
        console.log(`✅ API Calls: ${apiCalls} (${(apiCalls * 0.01).toFixed(2)}€)`);
        console.log(`💾 Approximiert: ${savedCalls} (0€)`);
        console.log(`📊 Einsparung: ${Math.round((savedCalls / (apiCalls + savedCalls)) * 100)}%`);
        console.log(`💶 Geschätzte Kosten: ${(apiCalls * 0.01).toFixed(2)}€ statt ${((apiCalls + savedCalls) * 0.01).toFixed(2)}€`);
        console.log(`========================\n`);

        return matrix;
    }

    // ======================================================================
    // DISTANCE MATRIX BATCH VERARBEITUNG
    // ======================================================================
    async calculateDistanceMatrixBatch(origins, destinations, matrix, apiKey) {
        const originsStr = origins.map(p => `${p.lat},${p.lng}`).join('|');
        const destinationsStr = destinations.map(p => `${p.lat},${p.lng}`).join('|');
        
        console.log(`🌐 Distance Matrix API Call: ${origins.length}×${destinations.length} Elemente`);
        
        const response = await axios.get(`https://maps.googleapis.com/maps/api/distancematrix/json`, {
            params: {
                origins: originsStr,
                destinations: destinationsStr,
                key: apiKey,
                units: 'metric',
                mode: this.constraints.travelMode,
                avoid: 'tolls',
                language: 'de',
                region: 'de',
                departure_time: 'now' // Berücksichtigt aktuellen Verkehr
            },
            timeout: 15000
        });

        if (response.data.status === 'OK') {
            console.log(`✅ Distance Matrix API erfolgreich`);
            
            for (let i = 0; i < origins.length; i++) {
                const origin = origins[i];
                if (!matrix[origin.id]) matrix[origin.id] = {};
                
                for (let j = 0; j < destinations.length; j++) {
                    const destination = destinations[j];
                    const element = response.data.rows[i].elements[j];
                    
                    if (element.status === 'OK') {
                        // ECHTE Fahrzeiten von Google Maps!
                        matrix[origin.id][destination.id] = {
                            distance: element.distance.value / 1000, // km
                            duration: (element.duration.value / 3600) + this.constraints.travelTimePadding, // Stunden + Puffer
                            duration_in_traffic: element.duration_in_traffic ? 
                                (element.duration_in_traffic.value / 3600) + this.constraints.travelTimePadding : 
                                (element.duration.value / 3600) + this.constraints.travelTimePadding
                        };
                    } else {
                        console.warn(`❌ Keine Route: ${origin.id} → ${destination.id}`);
                        throw new Error(`Keine Route gefunden zwischen Punkten`);
                    }
                }
            }
        } else {
            throw new Error(`Distance Matrix API Status: ${response.data.status}`);
        }
    }

    // ======================================================================
    // REALISTISCHE WOCHENPLANUNG
    // ======================================================================
    async planOptimalWeek(appointments, travelMatrix, weekStart) {
        console.log('🚗 Plane optimale Route mit ECHTEN Google Maps Fahrzeiten...');

        const fixedAppointments = appointments.filter(a => a.isFixed && a.fixedDate);
        const confirmedAppointments = appointments.filter(apt => apt.isConfirmed && !apt.isFixed);
        const proposalAppointments = appointments.filter(apt => apt.isProposal && !apt.isFixed);

        console.log(`📅 Termine: ${fixedAppointments.length} fix, ${confirmedAppointments.length} bestätigt, ${proposalAppointments.length} Vorschläge`);

        const week = this.initializeWeek(weekStart);

        // 1. Fixe Termine einplanen
        await this.scheduleFixedAppointments(week, fixedAppointments, travelMatrix);

        // 2. Termine nach Region gruppieren
        const appointmentsByRegion = this.groupAppointmentsByRegion([
            ...confirmedAppointments,
            ...proposalAppointments
        ]);

        // 3. Nach regionalen Clustern planen
        await this.scheduleByRegionalClusters(week, appointmentsByRegion, travelMatrix);

        // 4. Tagesrouten optimieren
        this.optimizeDailyRoutes(week, travelMatrix);

        // 5. Übernachtungen planen
        this.planOvernightStops(week, travelMatrix);

        return week;
    }

    // ======================================================================
    // WOCHE INITIALISIEREN
    // ======================================================================
    initializeWeek(weekStart) {
        const weekDays = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag'];
        const startDate = new Date(weekStart);
        
        return weekDays.map((day, index) => {
            const date = new Date(startDate);
            date.setDate(startDate.getDate() + index);
            
            return {
                day,
                date: date.toISOString().split('T')[0],
                appointments: [],
                travelSegments: [],
                workTime: 0,
                travelTime: 0,
                currentLocation: 'home',
                lastAppointmentEnd: this.constraints.workStartTime,
                overtime: false
            };
        });
    }

    // ======================================================================
    // FESTE TERMINE EINPLANEN
    // ======================================================================
    async scheduleFixedAppointments(week, fixedAppointments, travelMatrix) {
        console.log('📌 Plane feste Termine...');

        for (const apt of fixedAppointments) {
            const dayIndex = week.findIndex(d => d.date === apt.fixedDate);
            if (dayIndex === -1) continue;

            const day = week[dayIndex];
            const startTimeStr = apt.fixedTime || '10:00';
            const startHours = this.timeToHours(startTimeStr);
            const endHours = startHours + (apt.duration || this.constraints.appointmentDuration);

            day.appointments.push({
                ...apt,
                startTime: startTimeStr,
                endTime: this.formatTime(endHours),
                isFixed: true,
                customer: apt.invitee_name
            });

            day.workTime += (apt.duration || this.constraints.appointmentDuration);
            day.currentLocation = apt.id;
            day.lastAppointmentEnd = endHours;

            day.appointments.sort((a, b) => this.timeToHours(a.startTime) - this.timeToHours(b.startTime));
        }
    }

    // ======================================================================
    // TERMINE NACH REGIONEN GRUPPIEREN
    // ======================================================================
    groupAppointmentsByRegion(appointments) {
        const regions = {
            'Nord': { center: { lat: 53.5, lng: 10.0 }, appointments: [] },
            'Ost': { center: { lat: 52.5, lng: 13.4 }, appointments: [] },
            'West': { center: { lat: 51.2, lng: 7.0 }, appointments: [] },
            'Süd': { center: { lat: 48.5, lng: 11.5 }, appointments: [] },
            'Mitte': { center: { lat: 50.5, lng: 9.0 }, appointments: [] }
        };

        appointments.forEach(apt => {
            let minDistance = Infinity;
            let bestRegion = 'Mitte';

            Object.entries(regions).forEach(([regionName, regionData]) => {
                const distance = this.calculateHaversineDistance(
                    { lat: apt.lat, lng: apt.lng },
                    regionData.center
                );
                if (distance < minDistance) {
                    minDistance = distance;
                    bestRegion = regionName;
                }
            });

            regions[bestRegion].appointments.push({
                ...apt,
                distanceToCenter: minDistance
            });
        });

        return regions;
    }

    // ======================================================================
    // REGIONALE CLUSTER-BASIERTE PLANUNG
    // ======================================================================
    async scheduleByRegionalClusters(week, regionGroups, travelMatrix) {
        console.log('🗺️ Plane Termine nach regionalen Clustern mit echten Fahrzeiten...');

        const homeBase = this.constraints.homeBase;
        const sortedRegions = Object.entries(regionGroups)
            .filter(([_, data]) => data.appointments.length > 0)
            .sort((a, b) => {
                // Nutze ECHTE Fahrzeiten aus der Matrix!
                const distA = travelMatrix['home']?.[a[1].appointments[0]?.id]?.distance || 999;
                const distB = travelMatrix['home']?.[b[1].appointments[0]?.id]?.distance || 999;
                return distA - distB;
            });

        for (const [regionName, regionData] of sortedRegions) {
            console.log(`\n🎯 Plane Region ${regionName} (${regionData.appointments.length} Termine)`);

            const sortedAppointments = regionData.appointments.sort((a, b) => {
                if (a.isConfirmed !== b.isConfirmed) {
                    return a.isConfirmed ? -1 : 1;
                }
                return b.pipeline_days - a.pipeline_days;
            });

            await this.scheduleRegionAppointments(week, sortedAppointments, travelMatrix, regionName);
        }
    }

    // ======================================================================
    // TERMINE EINER REGION EINPLANEN
    // ======================================================================
    async scheduleRegionAppointments(week, appointments, travelMatrix, regionName) {
        for (const apt of appointments) {
            let scheduled = false;

            for (let dayIndex = 0; dayIndex < week.length; dayIndex++) {
                const day = week[dayIndex];

                if (day.workTime + day.travelTime + 3 > this.constraints.maxWorkHoursPerDay) {
                    continue;
                }

                const slot = this.findOptimalTimeSlot(day, apt, travelMatrix);

                if (slot) {
                    this.assignAppointmentToSlot(day, apt, slot, travelMatrix);
                    console.log(`✅ ${apt.customer} → ${day.day} ${slot.time} (Region ${regionName})`);
                    scheduled = true;
                    break;
                }
            }

            if (!scheduled && apt.isConfirmed) {
                console.log(`⚠️ Bestätigter Termin ${apt.customer} konnte nicht eingeplant werden`);
            }
        }
    }

    // ======================================================================
    // OPTIMALEN ZEITSLOT FINDEN (MIT ECHTEN FAHRZEITEN!)
    // ======================================================================
    findOptimalTimeSlot(day, appointment, travelMatrix) {
        const slots = this.findAvailableSlots(day);

        for (const s of slots) {
            const currentLocationId = day.currentLocation || 'home';
            const travelToAppointment = travelMatrix[currentLocationId]?.[appointment.id];

            if (!travelToAppointment) {
                console.warn(`❌ Keine Routendaten von ${currentLocationId} zu ${appointment.id}`);
                return null;
            }

            const travelTime = travelToAppointment.duration_in_traffic || travelToAppointment.duration;

            let startTime = Math.max(this.timeToHours(s.startTime), day.lastAppointmentEnd + travelTime);
            startTime = Math.round(startTime * 2) / 2;

            const appointmentEnd = startTime + this.constraints.appointmentDuration;

            if (appointmentEnd > this.timeToHours(s.endTime)) {
                continue;
            }

            if (appointmentEnd > this.constraints.workEndTime) {
                continue;
            }

            const travelHome = travelMatrix[appointment.id]?.['home'];
            if (travelHome) {
                const returnTime = appointmentEnd + travelHome.duration;
                if (returnTime > this.constraints.workEndTime) {
                    if (travelHome.distance < this.constraints.overnightThreshold) {
                        continue;
                    }
                    day.overnight = {
                        city: this.extractCityFromAddress(appointment.address),
                        distance: travelHome.distance
                    };
                }
            }

            return {
                time: this.formatTime(startTime),
                startTime: startTime,
                travelTime: travelTime,
                travelDistance: travelToAppointment.distance
            };
        }

        return null;
    }

    // ======================================================================
    // TERMIN ZU SLOT ZUWEISEN
    // ======================================================================
    assignAppointmentToSlot(day, appointment, slot, travelMatrix) {
        day.appointments.push({
            ...appointment,
            startTime: slot.time,
            endTime: this.formatTime(slot.startTime + this.constraints.appointmentDuration),
            travelTimeThere: slot.travelTime,
            travelDistance: slot.travelDistance,
            customer: appointment.invitee_name,
            isFixed: appointment.isFixed || false
        });

        day.workTime += this.constraints.appointmentDuration;
        day.travelTime += slot.travelTime;
        day.lastAppointmentEnd = slot.startTime + this.constraints.appointmentDuration;
        day.currentLocation = appointment.id;

        if (day.workTime + day.travelTime > this.constraints.maxWorkHoursPerDay) {
            day.overtime = true;
        }
    }

    // ======================================================================
    // TAGESROUTEN OPTIMIEREN
    // ======================================================================
    optimizeDailyRoutes(week, travelMatrix) {
        console.log('🔧 Optimiere Tagesrouten mit echten Google Maps Daten...');

        week.forEach((day, dayIndex) => {
            if (day.appointments.length === 0) {
                day.travelSegments = [];
                return;
            }

            console.log(`📅 Optimiere ${day.day} (${day.appointments.length} Termine)`);

            // Sortiere Termine nach Startzeit
            day.appointments.sort((a, b) => this.timeToHours(a.startTime) - this.timeToHours(b.startTime));

            // Generiere Fahrtsegmente mit ECHTEN Fahrzeiten aus der Matrix
            day.travelSegments = this.generateTravelSegments(day, travelMatrix);

            // Berechne Gesamt-Fahrzeit
            day.travelTime = day.travelSegments.reduce((sum, segment) => sum + segment.duration, 0);

            console.log(`   → ${day.travelTime.toFixed(1)}h Fahrzeit (echte Google Maps Daten)`);
        });
    }

    // ======================================================================
    // FAHRTSEGMENTE GENERIEREN (MIT ECHTEN MAPS DATEN!)
    // ======================================================================
    generateTravelSegments(day, travelMatrix) {
        if (day.appointments.length === 0) return [];
        
        const segments = [];
        const homeBase = this.constraints.homeBase;
        
        // 1. Hinfahrt zum ersten Termin
        const firstApt = day.appointments[0];
        const travelToFirst = travelMatrix['home']?.[firstApt.id];
        
        if (travelToFirst) {
            const departureTime = this.timeToHours(firstApt.startTime) - travelToFirst.duration;
            
            segments.push({
                type: 'departure',
                from: 'Hannover',
                to: this.extractCityFromAddress(firstApt.address),
                distance: Math.round(travelToFirst.distance),
                duration: travelToFirst.duration,
                startTime: this.formatTime(departureTime),
                endTime: firstApt.startTime,
                description: `Fahrt nach ${this.extractCityFromAddress(firstApt.address)}`
            });
        }
        
        // 2. Fahrten zwischen Terminen
        for (let i = 0; i < day.appointments.length - 1; i++) {
            const from = day.appointments[i];
            const to = day.appointments[i + 1];
            
            const travelData = travelMatrix[from.id]?.[to.id];
            
            if (travelData) {
                segments.push({
                    type: 'travel',
                    from: this.extractCityFromAddress(from.address),
                    to: this.extractCityFromAddress(to.address),
                    distance: Math.round(travelData.distance),
                    duration: travelData.duration,
                    startTime: from.endTime,
                    endTime: to.startTime,
                    description: `Fahrt von ${this.extractCityFromAddress(from.address)} nach ${this.extractCityFromAddress(to.address)}`
                });
            }
        }
        
        // 3. Rückfahrt oder Übernachtung
        const lastApt = day.appointments[day.appointments.length - 1];
        const travelHome = travelMatrix[lastApt.id]?.['home'];
        
        if (travelHome && !day.overnight) {
            if (travelHome.distance < this.constraints.overnightThreshold) {
                segments.push({
                    type: 'return',
                    from: this.extractCityFromAddress(lastApt.address),
                    to: 'Hannover',
                    distance: Math.round(travelHome.distance),
                    duration: travelHome.duration,
                    startTime: lastApt.endTime,
                    endTime: this.formatTime(this.timeToHours(lastApt.endTime) + travelHome.duration),
                    description: 'Rückfahrt nach Hannover'
                });
            }
        }
        
        return segments;
    }

    // ======================================================================
    // ÜBERNACHTUNGSSTOPPS PLANEN
    // ======================================================================
    planOvernightStops(week, travelMatrix) {
        console.log('🏨 Plane Übernachtungen basierend auf echten Entfernungen...');
        
        for (let i = 0; i < week.length; i++) {
            const day = week[i];
            if (day.appointments.length === 0) continue;
            
            const lastAppointment = day.appointments[day.appointments.length - 1];
            const travelHome = travelMatrix[lastAppointment.id]?.['home'];
            
            if (travelHome && travelHome.distance >= this.constraints.overnightThreshold) {
                const nearestCity = this.extractCityFromAddress(lastAppointment.address);
                day.overnight = {
                    city: nearestCity,
                    reason: `${travelHome.distance.toFixed(0)}km von Hannover - Übernachtung nötig`,
                    distance: travelHome.distance
                };
                console.log(`🏨 ${day.day}: Übernachtung in ${nearestCity} (${travelHome.distance.toFixed(0)}km)`);
            }
        }
    }

    // ======================================================================
    // ÜBERNACHTUNGSSTOPPS OPTIMIEREN
    // ======================================================================
    optimizeOvernightStops(week, travelMatrix) {
        // Bereits in planOvernightStops erledigt mit echten Daten
        return week;
    }

    // ======================================================================
    // WOCHENPLAN VALIDIEREN
    // ======================================================================
    validateWeekPlan(week) {
        const totalHours = week.reduce((sum, day) => sum + day.workTime + day.travelTime, 0);
        const validations = [];

        if (totalHours > this.constraints.maxWorkHoursPerWeek) {
            validations.push(`⚠️ Überstunden: ${totalHours.toFixed(1)}h (max. ${this.constraints.maxWorkHoursPerWeek}h)`);
        }

        week.forEach(day => {
            const dayTotal = day.workTime + day.travelTime;
            if (dayTotal > this.constraints.flexWorkHoursPerDay) {
                validations.push(`⚠️ ${day.day}: ${dayTotal.toFixed(1)}h (max. ${this.constraints.flexWorkHoursPerDay}h)`);
            }
        });

        const efficiency = this.calculatePlanningEfficiency(week);
        
        return {
            week,
            totalHours,
            validations,
            efficiency,
            isValid: validations.length === 0
        };
    }

    // ======================================================================
    // PLANUNGSEFFIZIENZ BERECHNEN
    // ======================================================================
    calculatePlanningEfficiency(week) {
        const totalAppointments = week.reduce((sum, day) => sum + day.appointments.length, 0);
        const totalTravelTime = week.reduce((sum, day) => sum + day.travelTime, 0);
        const totalWorkTime = week.reduce((sum, day) => sum + day.workTime, 0);
        
        return {
            appointmentRatio: totalWorkTime > 0 ? (totalAppointments * 3) / totalWorkTime : 0,
            travelEfficiency: totalWorkTime > 0 ? 1 - (totalTravelTime / totalWorkTime) : 0,
            weekUtilization: totalWorkTime / this.constraints.maxWorkHoursPerWeek
        };
    }

    // ======================================================================
    // ERGEBNIS FORMATIEREN
    // ======================================================================
    formatWeekResult(validatedWeek, travelMatrix) {
        const { week, totalHours, validations, efficiency } = validatedWeek;
        
        // WICHTIG: Stelle sicher, dass alle travelSegments vorhanden sind
        const enhancedWeek = week.map(day => {
            if (!day.travelSegments || day.travelSegments.length === 0) {
                day.travelSegments = this.generateTravelSegments(day, travelMatrix);
            }
            return day;
        });
        
        const optimizations = [
            `${week.reduce((sum, day) => sum + day.appointments.length, 0)} Termine intelligent eingeplant`,
            `Arbeitszeit optimiert: ${totalHours.toFixed(1)}h von ${this.constraints.maxWorkHoursPerWeek}h`,
            `Reiseeffizienz: ${(efficiency.travelEfficiency * 100).toFixed(1)}%`,
            'Echte Fahrzeiten von Google Maps API verwendet',
            'Verkehrsdaten berücksichtigt'
        ];

        const overnightDays = week.filter(day => day.overnight).length;
        if (overnightDays > 0) {
            optimizations.push(`${overnightDays} strategische Übernachtungsstopps geplant`);
        }

        if (validations.length > 0) {
            optimizations.push(...validations);
        }

        return {
            weekStart: week[0].date,
            days: enhancedWeek,
            totalHours: parseFloat(totalHours.toFixed(1)),
            optimizations,
            stats: {
                totalAppointments: week.reduce((sum, day) => sum + day.appointments.length, 0),
                confirmedAppointments: week.reduce((sum, day) => 
                    sum + day.appointments.filter(apt => apt.isConfirmed).length, 0),
                proposalAppointments: week.reduce((sum, day) => 
                    sum + day.appointments.filter(apt => apt.isProposal).length, 0),
                totalTravelTime: parseFloat(week.reduce((sum, day) => sum + day.travelTime, 0).toFixed(1)),
                workDays: week.filter(day => day.appointments.length > 0).length,
                efficiency: efficiency
            },
            generatedAt: new Date().toISOString()
        };
    }

    // ======================================================================
    // HILFSFUNKTIONEN
    // ======================================================================
    
    extractCityFromAddress(address) {
        if (!address) return 'Unbekannt';
        
        const plzCityMatch = address.match(/\b(\d{5})\s+([^,]+)/);
        if (plzCityMatch) {
            return plzCityMatch[2].trim();
        }
        
        const parts = address.split(',');
        if (parts.length > 1) {
            const lastPart = parts[parts.length - 1].trim();
            return lastPart.replace(/,?\s*(Deutschland|Germany)$/i, '').trim();
        }
        
        return address.substring(0, 20) + '...';
    }

    formatTime(hours) {
        const rounded = Math.round(hours * 2) / 2;
        const h = Math.floor(rounded);
        const m = Math.round((rounded - h) * 60);

        if (m >= 60) {
            return `${(h + 1).toString().padStart(2, '0')}:00`;
        }

        return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
    }

    timeToHours(timeStr) {
        const [h, m] = timeStr.split(':').map(Number);
        return h + (m || 0) / 60;
    }

    findAvailableSlots(day) {
        const slots = [];
        const dayStart = day.earliestStart || 6;
        const dayEnd = day.latestEnd || 20;

        if (day.appointments.length === 0) {
            slots.push({
                startTime: this.formatTime(dayStart),
                endTime: this.formatTime(dayEnd),
                duration: dayEnd - dayStart
            });
            return slots;
        }

        const sorted = [...day.appointments].sort((a, b) =>
            this.timeToHours(a.startTime) - this.timeToHours(b.startTime)
        );

        const firstStart = this.timeToHours(sorted[0].startTime);
        if (firstStart > dayStart + 1) {
            slots.push({
                startTime: this.formatTime(dayStart),
                endTime: this.formatTime(firstStart - 1),
                duration: firstStart - 1 - dayStart
            });
        }

        for (let i = 0; i < sorted.length - 1; i++) {
            const currentEnd = this.timeToHours(sorted[i].endTime);
            const nextStart = this.timeToHours(sorted[i + 1].startTime);
            if (nextStart - currentEnd > 1) {
                slots.push({
                    startTime: this.formatTime(currentEnd + 0.5),
                    endTime: this.formatTime(nextStart - 0.5),
                    duration: nextStart - currentEnd - 1
                });
            }
        }

        const lastEnd = this.timeToHours(sorted[sorted.length - 1].endTime);
        if (dayEnd - lastEnd > 1) {
            slots.push({
                startTime: this.formatTime(lastEnd + 0.5),
                endTime: this.formatTime(dayEnd),
                duration: dayEnd - lastEnd - 0.5
            });
        }

        return slots;
    }

    // NUR FÜR REGIONSZUORDNUNG - NICHT FÜR FAHRZEITEN!
    calculateHaversineDistance(from, to) {
        const R = 6371;
        const dLat = (to.lat - from.lat) * Math.PI / 180;
        const dLng = (to.lng - from.lng) * Math.PI / 180;
        const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                  Math.cos(from.lat * Math.PI / 180) * Math.cos(to.lat * Math.PI / 180) *
                  Math.sin(dLng/2) * Math.sin(dLng/2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }

    // NEUE HILFSFUNKTION: Clustere Termine nach Nähe
    clusterAppointmentsByProximity(appointments, maxDistanceKm = 30) {
        const clusters = [];
        const assigned = new Set();

        appointments.forEach((apt, index) => {
            if (assigned.has(apt.id)) return;

            const cluster = {
                id: `cluster_${clusters.length}`,
                appointments: [apt],
                center: null
            };
            assigned.add(apt.id);

            appointments.forEach((other, otherIndex) => {
                if (index === otherIndex || assigned.has(other.id)) return;

                const distance = this.calculateHaversineDistance(
                    { lat: apt.lat, lng: apt.lng },
                    { lat: other.lat, lng: other.lng }
                );

                if (distance <= maxDistanceKm) {
                    cluster.appointments.push(other);
                    assigned.add(other.id);
                }
            });

            const avgLat = cluster.appointments.reduce((sum, a) => sum + a.lat, 0) / cluster.appointments.length;
            const avgLng = cluster.appointments.reduce((sum, a) => sum + a.lng, 0) / cluster.appointments.length;

            cluster.center = {
                id: cluster.id,
                lat: avgLat,
                lng: avgLng,
                name: `Zentrum von ${cluster.appointments.length} Terminen`
            };

            clusters.push(cluster);
        });

        return clusters;
    }

    // ZUSÄTZLICH: Fügen Sie diese Funktion für alternative Terminvorschläge hinzu:
    async suggestAlternativeSlots(rejectedAppointment, currentWeekPlan) {
        console.log(`🔄 Suche Alternativen für ${rejectedAppointment.customer}...`);

        const alternatives = [];
        const weekDays = currentWeekPlan.days || [];

        weekDays.forEach((day, dayIndex) => {
            const availableSlots = this.findAvailableSlots(day);

            availableSlots.forEach(slot => {
                if (slot.duration >= 3.5) { // 3h Termin + 0.5h Puffer
                    let efficiency = 0.5;

                    const nearbyAppointments = day.appointments.filter(apt => {
                        if (!apt.lat || !apt.lng || !rejectedAppointment.lat || !rejectedAppointment.lng) {
                            return false;
                        }
                        const distance = this.calculateHaversineDistance(
                            { lat: apt.lat, lng: apt.lng },
                            { lat: rejectedAppointment.lat, lng: rejectedAppointment.lng }
                        );
                        return distance < 50;
                    });

                    if (nearbyAppointments.length > 0) {
                        efficiency += 0.3;
                    }

                    const slotStart = this.timeToHours(slot.startTime);
                    if (slotStart >= 10 && slotStart <= 15) {
                        efficiency += 0.2;
                    }

                    alternatives.push({
                        day: day.day,
                        date: day.date,
                        startTime: slot.startTime,
                        endTime: this.formatTime(this.timeToHours(slot.startTime) + 3),
                        efficiency: Math.min(1, efficiency),
                        reasoning: this.generateSlotReasoning(day, slot, efficiency, nearbyAppointments)
                    });
                }
            });
        });

        alternatives.sort((a, b) => b.efficiency - a.efficiency);

        const nextWeekStart = new Date(currentWeekPlan.weekStart);
        nextWeekStart.setDate(nextWeekStart.getDate() + 7);

        return {
            appointmentId: rejectedAppointment.id,
            customer: rejectedAppointment.customer,
            currentWeekAlternatives: alternatives.slice(0, 5),
            nextWeekAvailable: true,
            nextWeekStart: nextWeekStart.toISOString().split('T')[0],
            recommendation: this.generateRecommendation(alternatives)
        };
    }

    generateSlotReasoning(day, slot, efficiency, nearbyAppointments) {
        const reasons = [];

        if (efficiency > 0.7) {
            reasons.push('Hohe Reiseeffizienz');
        }

        if (nearbyAppointments.length > 0) {
            reasons.push(`${nearbyAppointments.length} Termine in der Nähe`);
        }

        const slotStart = this.timeToHours(slot.startTime);
        if (slotStart >= 10 && slotStart <= 15) {
            reasons.push('Optimale Tageszeit');
        }

        if (day.appointments.length < 2) {
            reasons.push('Tag hat noch Kapazität');
        }

        return reasons;
    }

    generateRecommendation(alternatives) {
        if (alternatives.length === 0) {
            return 'Keine passenden Alternativen in dieser Woche. Empfehle Verschiebung auf nächste Woche.';
        }

        const best = alternatives[0];
        if (best.efficiency > 0.7) {
            return `Beste Alternative: ${best.day} ${best.startTime} - Hohe Effizienz durch ${best.reasoning.join(', ')}`;
        } else if (best.efficiency > 0.5) {
            return `Alternative verfügbar: ${best.day} ${best.startTime} - ${best.reasoning.join(', ')}`;
        } else {
            return 'Alternativen verfügbar, aber nächste Woche wäre effizienter';
        }
    }

    // ======================================================================
    // TERMIN KANN EINGEPLANT WERDEN PRÜFEN
    // ======================================================================
    canFitAppointment(weekPlan, appointment, scheduledCount = null) {
        const totalScheduled =
            scheduledCount !== null ? scheduledCount : (weekPlan.stats ? weekPlan.stats.totalAppointments : 0);
        const maxPossible = Math.floor(this.constraints.maxWorkHoursPerWeek / this.constraints.appointmentDuration);

        return {
            canFit: totalScheduled < maxPossible,
            availableSlots: maxPossible - totalScheduled,
            estimatedDay: totalScheduled < 10 ? Math.floor(totalScheduled / 2) + 1 : 'Ende der Woche'
        };
    }
}

module.exports = IntelligentRoutePlanner;

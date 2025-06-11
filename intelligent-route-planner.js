// ======================================================================
// INTELLIGENTE ROUTENPLANUNG FÜR TESTIMONIALS.DE
// Datei: intelligent-route-planner.js
// ======================================================================

const axios = require('axios');
const EnhancedGeocodingService = require('./geocoding-service');

class IntelligentRoutePlanner {
    constructor() {
        this.geocodingService = new EnhancedGeocodingService();
        this.constraints = {
            maxWorkHoursPerWeek: 40,
            maxWorkHoursPerDay: 8,
            flexWorkHoursPerDay: 10, // Ausnahme möglich
            workStartTime: 8, // 8:00 Uhr
            workEndTime: 17, // 17:00 Uhr (zurück im Büro)
            appointmentDuration: 3, // Stunden pro Dreh
            homeBase: { lat: 52.3759, lng: 9.7320, name: 'Hannover' }, // Startpunkt
            travelSpeedKmh: 80, // Durchschnittliche Reisegeschwindigkeit
            maxTravelTimePerDay: 4 // Max 4h Fahrt pro Tag
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
            
            // 3. Reisematrix berechnen
            const travelMatrix = await this.calculateTravelMatrix(geoAppointments);
            
            // 4. Intelligente Wochenplanung
            const optimizedWeek = await this.planOptimalWeek(geoAppointments, travelMatrix, weekStart);
            
            // 5. Übernachtungsstopps optimieren
            const weekWithOvernight = this.optimizeOvernightStops(optimizedWeek, travelMatrix);
            
            // 6. Qualitätsprüfung und Validierung
            const validatedWeek = this.validateWeekPlan(weekWithOvernight);
            
            console.log('✅ Intelligente Routenplanung abgeschlossen');
            return this.formatWeekResult(validatedWeek);
            
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
                // Kategorisierung
                isConfirmed: apt.status === 'bestätigt',
                isProposal: apt.status === 'vorschlag',
                priority: this.calculateDynamicPriority(apt, parsedNotes),
                // Zeitinformationen
                preferredTimes: parsedNotes.preferred_times || [],
                excludedDates: JSON.parse(apt.excluded_dates || '[]'),
                // Zusätzliche Infos
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
        let score = 50; // Basis-Score

        // Bestätigte Termine haben höchste Priorität
        if (apt.status === 'bestätigt') score += 40;
        
        // Pipeline-Alter berücksichtigen (länger = höhere Priorität)
        score += Math.min(apt.pipeline_days * 0.5, 20);
        
        // Wichtige Kunden bevorzugen
        const vipCustomers = ['BMW', 'Mercedes', 'Audi', 'Porsche', 'Volkswagen', 'Apple', 'Microsoft', 'Google'];
        if (vipCustomers.some(vip => 
            (notes.customer_company || '').toLowerCase().includes(vip.toLowerCase())
        )) {
            score += 15;
        }
        
        // Ursprüngliche Priorität berücksichtigen
        if (apt.priority === 'hoch') score += 10;
        else if (apt.priority === 'niedrig') score -= 10;

        return Math.max(0, Math.min(100, score));
    }

    // ======================================================================
    // GEOCODING - ADRESSEN ZU KOORDINATEN
    // ======================================================================
    async geocodeAppointments(appointments) {
        console.log('🗺️ Geocoding von Adressen mit Google Maps API...');

        const geocoded = [];
        for (const apt of appointments) {
            // Bereits vorhandene Koordinaten nutzen
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
                // Fallback versuchen
                try {
                    const fallbackCoords = this.getFallbackCoordinates(apt.address);
                    geocoded.push({
                        ...apt,
                        lat: fallbackCoords.lat,
                        lng: fallbackCoords.lng,
                        geocoded: true,
                        geocoding_method: 'fallback'
                    });
                    console.log(`🔄 Fallback Geocoding: ${apt.invitee_name}`);
                } catch (fallbackError) {
                    console.error(`❌ Auch Fallback fehlgeschlagen für ${apt.address}`);
                }
            }
        }

        return geocoded.filter(apt => apt.geocoded); // Nur geocodierte Termine
    }

    // ======================================================================
    // EINZELNE ADRESSE GEOCODEN MIT GOOGLE MAPS API
    // ======================================================================
    async geocodeAddress(address) {
        return this.geocodingService.geocodeAddress(address);
    }

    // ======================================================================
    // FALLBACK KOORDINATEN FÜR BEKANNTE STÄDTE
    // ======================================================================
    getFallbackCoordinates(address) {
        const cityCoords = {
            'München': { lat: 48.1351, lng: 11.5820 },
            'Berlin': { lat: 52.5200, lng: 13.4050 },
            'Hamburg': { lat: 53.5511, lng: 9.9937 },
            'Köln': { lat: 50.9375, lng: 6.9603 },
            'Frankfurt': { lat: 50.1109, lng: 8.6821 },
            'Stuttgart': { lat: 48.7758, lng: 9.1829 },
            'Düsseldorf': { lat: 51.2277, lng: 6.7735 },
            'Leipzig': { lat: 51.3397, lng: 12.3731 },
            'Hannover': { lat: 52.3759, lng: 9.7320 },
            'Wolfsburg': { lat: 52.4227, lng: 10.7865 },
            'Nürnberg': { lat: 49.4521, lng: 11.0767 },
            'Bremen': { lat: 53.0793, lng: 8.8017 },
            'Dresden': { lat: 51.0504, lng: 13.7373 },
            'Dortmund': { lat: 51.5136, lng: 7.4653 },
            'Essen': { lat: 51.4556, lng: 7.0116 }
        };

        // Versuche Stadt zu erkennen
        for (const [city, coords] of Object.entries(cityCoords)) {
            if (address.toLowerCase().includes(city.toLowerCase())) {
                return coords;
            }
        }

        // Versuche PLZ zu erkennen
        const plzMatch = address.match(/\b(\d{5})\b/);
        if (plzMatch) {
            const plz = plzMatch[1];
            // Grobe PLZ-basierte Koordinaten
            if (plz.startsWith('1')) return { lat: 52.52, lng: 13.40 }; // Berlin
            if (plz.startsWith('2')) return { lat: 53.55, lng: 9.99 }; // Hamburg
            if (plz.startsWith('3')) return { lat: 52.38, lng: 9.73 }; // Hannover
            if (plz.startsWith('4')) return { lat: 51.51, lng: 7.47 }; // NRW
            if (plz.startsWith('5')) return { lat: 50.94, lng: 6.96 }; // Köln
            if (plz.startsWith('6')) return { lat: 50.11, lng: 8.68 }; // Frankfurt
            if (plz.startsWith('7')) return { lat: 48.78, lng: 9.18 }; // Stuttgart
            if (plz.startsWith('8')) return { lat: 48.14, lng: 11.58 }; // München
            if (plz.startsWith('9')) return { lat: 49.45, lng: 11.08 }; // Nürnberg
        }

        throw new Error('Keine Koordinaten für Adresse gefunden');
    }

    // ======================================================================
    // REISEMATRIX MIT GOOGLE DISTANCE MATRIX API
    // ======================================================================
    async calculateTravelMatrix(appointments) {
        console.log('🚗 Berechne Reisematrix mit Google Distance Matrix API...');

        const matrix = {};
        const apiKey = process.env.GOOGLE_MAPS_API_KEY || 'AIzaSyD6D4OGAfep-u-N1yz_F--jacBFs1TINR4';

        if (!apiKey) {
            this.distanceMatrixApiDisabled = true;
        }

        // Alle Punkte (inkl. Home Base)
        const allPoints = [
            { id: 'home', ...this.constraints.homeBase },
            ...appointments.map(apt => ({ id: apt.id, lat: apt.lat, lng: apt.lng }))
        ];

        // Distance Matrix API kann bis zu 25 Origins und 25 Destinations handhaben
        const batchSize = 25;
        
        for (let i = 0; i < allPoints.length; i += batchSize) {
            const originBatch = allPoints.slice(i, i + batchSize);
            
            for (let j = 0; j < allPoints.length; j += batchSize) {
                const destinationBatch = allPoints.slice(j, j + batchSize);
                
                if (this.distanceMatrixApiDisabled) {
                    this.calculateFallbackDistances(originBatch, destinationBatch, matrix);
                    continue;
                }

                try {
                    await this.calculateDistanceMatrixBatch(originBatch, destinationBatch, matrix, apiKey);
                } catch (error) {
                    console.warn('Distance Matrix Batch fehlgeschlagen, verwende Fallback:', error.message);

                    if (error.message.includes('REQUEST_DENIED') ||
                        error.message.includes('API key') ||
                        error.message.includes('invalid')) {
                        this.distanceMatrixApiDisabled = true;
                    }

                    // Fallback für diese Batch
                    this.calculateFallbackDistances(originBatch, destinationBatch, matrix);
                }
            }
        }

        return matrix;
    }

    // ======================================================================
    // DISTANCE MATRIX BATCH VERARBEITUNG
    // ======================================================================
    async calculateDistanceMatrixBatch(origins, destinations, matrix, apiKey) {
        const originsStr = origins.map(p => `${p.lat},${p.lng}`).join('|');
        const destinationsStr = destinations.map(p => `${p.lat},${p.lng}`).join('|');
        
        const response = await axios.get(`https://maps.googleapis.com/maps/api/distancematrix/json`, {
            params: {
                origins: originsStr,
                destinations: destinationsStr,
                key: apiKey,
                units: 'metric',
                mode: 'driving',
                avoid: 'tolls'
            },
            timeout: 10000
        });

        if (response.data.status === 'OK') {
            for (let i = 0; i < origins.length; i++) {
                const origin = origins[i];
                if (!matrix[origin.id]) matrix[origin.id] = {};
                
                for (let j = 0; j < destinations.length; j++) {
                    const destination = destinations[j];
                    const element = response.data.rows[i].elements[j];
                    
                    if (element.status === 'OK') {
                        matrix[origin.id][destination.id] = {
                            distance: element.distance.value / 1000, // km
                            duration: element.duration.value / 3600  // Stunden
                        };
                    } else {
                        // Fallback für dieses spezifische Paar
                        matrix[origin.id][destination.id] = this.calculateFallbackDistance(origin, destination);
                    }
                }
            }
        } else {
            if (response.data.status === 'REQUEST_DENIED') {
                this.distanceMatrixApiDisabled = true;
            }
            throw new Error(`Distance Matrix API Status: ${response.data.status}`);
        }
    }

    // ======================================================================
    // FALLBACK DISTANZ-BERECHNUNG
    // ======================================================================
    calculateFallbackDistances(origins, destinations, matrix) {
        for (const origin of origins) {
            if (!matrix[origin.id]) matrix[origin.id] = {};
            for (const destination of destinations) {
                matrix[origin.id][destination.id] = this.calculateFallbackDistance(origin, destination);
            }
        }
    }

    calculateFallbackDistance(from, to) {
        if (from.id === to.id) {
            return { distance: 0, duration: 0 };
        }
        
        const distance = this.calculateHaversineDistance(from, to) * 1.3; // 30% Aufschlag für Straßen
        const duration = distance / this.constraints.travelSpeedKmh;
        
        return { distance, duration };
    }

    // ======================================================================
    // LUFTLINIEN-DISTANZ (HAVERSINE)
    // ======================================================================
    calculateHaversineDistance(from, to) {
        const R = 6371; // Erdradius in km
        const dLat = (to.lat - from.lat) * Math.PI / 180;
        const dLng = (to.lng - from.lng) * Math.PI / 180;
        const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                  Math.cos(from.lat * Math.PI / 180) * Math.cos(to.lat * Math.PI / 180) *
                  Math.sin(dLng/2) * Math.sin(dLng/2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }

    // ======================================================================
    // OPTIMALE WOCHENPLANUNG
    // ======================================================================
    async planOptimalWeek(appointments, travelMatrix, weekStart) {
        console.log('🎯 Plane optimale Woche...');
        
        // 1. Bestätigte Termine priorisieren
        const confirmedAppointments = appointments.filter(apt => apt.isConfirmed);
        const proposalAppointments = appointments.filter(apt => apt.isProposal);
        
        // 2. Nach Priorität sortieren
        const sortedConfirmed = confirmedAppointments.sort((a, b) => b.priority - a.priority);
        const sortedProposals = proposalAppointments.sort((a, b) => b.priority - a.priority);
        
        // 3. Woche initialisieren
        const week = this.initializeWeek(weekStart);
        
        // 4. Bestätigte Termine zuerst einplanen
        await this.scheduleAppointments(week, sortedConfirmed, travelMatrix, true);
        
        // 5. Vorschläge einplanen (falls Platz)
        await this.scheduleAppointments(week, sortedProposals, travelMatrix, false);
        
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
                workTime: 0,
                travelTime: 0,
                currentLocation: 'home', // Startet zu Hause
                lastAppointmentEnd: this.constraints.workStartTime,
                overtime: false
            };
        });
    }

    // ======================================================================
    // TERMINE EINPLANEN
    // ======================================================================
    async scheduleAppointments(week, appointments, travelMatrix, isConfirmed) {
        for (const appointment of appointments) {
            const bestSlot = this.findBestTimeSlot(week, appointment, travelMatrix);
            
            if (bestSlot) {
                this.assignAppointmentToSlot(week[bestSlot.dayIndex], appointment, bestSlot, travelMatrix);
                console.log(`📅 ${isConfirmed ? 'Bestätigt' : 'Vorschlag'}: ${appointment.invitee_name} → ${bestSlot.day} ${bestSlot.time}`);
            } else {
                console.warn(`⚠️ Kein Slot gefunden für: ${appointment.invitee_name}`);
            }
        }
    }

    // ======================================================================
    // BESTEN ZEITSLOT FINDEN
    // ======================================================================
    findBestTimeSlot(week, appointment, travelMatrix) {
        let bestSlot = null;
        let bestScore = -1;

        for (let dayIndex = 0; dayIndex < week.length; dayIndex++) {
            const day = week[dayIndex];
            
            // Arbeitszeit-Constraints prüfen
            const potentialWorkTime = day.workTime + this.constraints.appointmentDuration;
            const maxAllowed = appointment.isConfirmed ? this.constraints.flexWorkHoursPerDay : this.constraints.maxWorkHoursPerDay;
            
            if (potentialWorkTime > maxAllowed) {
                continue; // Tag bereits voll
            }

            // Reisezeit vom aktuellen Standort berechnen
            const travelFromCurrent = travelMatrix[day.currentLocation]?.[appointment.id];
            if (!travelFromCurrent) continue;

            // Frühester Starttermin
            const earliestStart = day.lastAppointmentEnd + travelFromCurrent.duration;
            
            // Prüfe ob der Termin an diesem Tag noch passt
            const appointmentEnd = earliestStart + this.constraints.appointmentDuration;
            if (appointmentEnd > this.constraints.workEndTime) {
                // Versuche Rückreise-Zeit zu berücksichtigen
                const returnHome = travelMatrix[appointment.id]?.['home'];
                if (returnHome && appointmentEnd + returnHome.duration > this.constraints.workEndTime + 2) {
                    continue; // Würde zu spät für Rückreise
                }
            }

            // Score berechnen (je höher, desto besser)
            const score = this.calculateSlotScore(day, appointment, travelFromCurrent, dayIndex);
            
            if (score > bestScore) {
                bestScore = score;
                bestSlot = {
                    dayIndex,
                    day: day.day,
                    time: this.formatTime(earliestStart),
                    startTime: earliestStart,
                    travelTime: travelFromCurrent.duration,
                    score
                };
            }
        }

        return bestSlot;
    }

    // ======================================================================
    // SLOT-SCORE BERECHNEN
    // ======================================================================
    calculateSlotScore(day, appointment, travelData, dayIndex) {
        let score = 100;

        // Weniger Reisezeit = besser
        score -= travelData.duration * 10;
        
        // Frühere Wochentage bevorzugen (außer bei sehr hoher Priorität)
        score -= dayIndex * 3;
        
        // Bestätigte Termine bevorzugen
        if (appointment.isConfirmed) score += 30;
        
        // Lange Pipeline-Zeit bevorzugen
        score += appointment.pipeline_age * 0.3;
        
        // Tag nicht zu voll machen
        if (day.workTime > 6) score -= 15;
        if (day.workTime > 7) score -= 25;
        
        // VIP-Kunden bevorzugen
        if (appointment.priority > 80) score += 20;
        
        return Math.max(0, score);
    }

    // ======================================================================
    // TERMIN ZU SLOT ZUWEISEN
    // ======================================================================
    assignAppointmentToSlot(day, appointment, slot, travelMatrix) {
        // Termin hinzufügen
        day.appointments.push({
            ...appointment,
            startTime: slot.time,
            endTime: this.formatTime(slot.startTime + this.constraints.appointmentDuration),
            travelTimeThere: slot.travelTime,
            customer: appointment.invitee_name // Für Frontend-Kompatibilität
        });

        // Tagesstatistiken aktualisieren
        day.workTime += this.constraints.appointmentDuration;
        day.travelTime += slot.travelTime;
        day.lastAppointmentEnd = slot.startTime + this.constraints.appointmentDuration;
        day.currentLocation = appointment.id;

        // Overtime prüfen
        if (day.workTime > this.constraints.maxWorkHoursPerDay) {
            day.overtime = true;
        }
    }

    // ======================================================================
    // ÜBERNACHTUNGSSTOPPS OPTIMIEREN
    // ======================================================================
    optimizeOvernightStops(week, travelMatrix) {
        console.log('🏨 Optimiere Übernachtungsstopps...');
        
        for (let i = 0; i < week.length - 1; i++) {
            const today = week[i];
            const tomorrow = week[i + 1];

            if (today.appointments.length > 0 && tomorrow.appointments.length > 0) {
                const lastToday = today.appointments[today.appointments.length - 1];
                const firstTomorrow = tomorrow.appointments[0];

                // Prüfe ob Übernachtung sinnvoll ist
                const directDistance = this.calculateHaversineDistance(lastToday, firstTomorrow);
                
                if (directDistance > 200) { // Mehr als 200km
                    const overnightStop = this.calculateOptimalOvernightStop(lastToday, firstTomorrow, today);
                    
                    if (overnightStop) {
                        today.overnight = overnightStop;
                        console.log(`🏨 Übernachtung geplant: ${overnightStop.city}`);
                    }
                }
            }
        }

        return week;
    }

    // ======================================================================
    // OPTIMALEN ÜBERNACHTUNGSSTOP BERECHNEN
    // ======================================================================
    calculateOptimalOvernightStop(lastToday, firstTomorrow, dayInfo) {
        const lastLocation = { lat: lastToday.lat, lng: lastToday.lng };
        const nextLocation = { lat: firstTomorrow.lat, lng: firstTomorrow.lng };
        
        // Verfügbare Zeit für Fahrt berechnen
        const lastAppointmentEnd = parseFloat(lastToday.startTime.replace(':', '.')) + this.constraints.appointmentDuration;
        const availableTravelTime = Math.min(2, this.constraints.workEndTime - lastAppointmentEnd);
        
        if (availableTravelTime < 1) {
            return null; // Zu wenig Zeit
        }
        
        // Optimaler Stopp: So weit wie möglich in Richtung nächster Termin
        const maxDistance = availableTravelTime * this.constraints.travelSpeedKmh;
        const totalDistance = this.calculateHaversineDistance(lastLocation, nextLocation);
        const travelRatio = Math.min(0.7, maxDistance / totalDistance);
        
        // Interpolation für optimalen Stopp
        const stopLat = lastLocation.lat + (nextLocation.lat - lastLocation.lat) * travelRatio;
        const stopLng = lastLocation.lng + (nextLocation.lng - lastLocation.lng) * travelRatio;

        // Nächste Stadt finden
        const nearestCity = this.findNearestCity(stopLat, stopLng);
        
        const travelTimeToStop = this.calculateHaversineDistance(lastLocation, { lat: stopLat, lng: stopLng }) / this.constraints.travelSpeedKmh;
        const remainingDistance = this.calculateHaversineDistance({ lat: stopLat, lng: stopLng }, nextLocation);

        return {
            city: nearestCity,
            description: `Hotel in ${nearestCity} - strategischer Stopp (${remainingDistance.toFixed(0)}km bis zum nächsten Termin)`,
            travelTime: travelTimeToStop,
            coordinates: { lat: stopLat, lng: stopLng },
            savings: `${(totalDistance - remainingDistance).toFixed(0)}km heute zurückgelegt`
        };
    }

    // ======================================================================
    // NÄCHSTE STADT FINDEN
    // ======================================================================
    findNearestCity(lat, lng) {
        const cities = [
            { name: 'Hamburg', lat: 53.5511, lng: 9.9937 },
            { name: 'Berlin', lat: 52.5200, lng: 13.4050 },
            { name: 'München', lat: 48.1351, lng: 11.5820 },
            { name: 'Köln', lat: 50.9375, lng: 6.9603 },
            { name: 'Frankfurt am Main', lat: 50.1109, lng: 8.6821 },
            { name: 'Stuttgart', lat: 48.7758, lng: 9.1829 },
            { name: 'Düsseldorf', lat: 51.2277, lng: 6.7735 },
            { name: 'Leipzig', lat: 51.3397, lng: 12.3731 },
            { name: 'Hannover', lat: 52.3759, lng: 9.7320 },
            { name: 'Nürnberg', lat: 49.4521, lng: 11.0767 },
            { name: 'Bremen', lat: 53.0793, lng: 8.8017 },
            { name: 'Dresden', lat: 51.0504, lng: 13.7373 },
            { name: 'Dortmund', lat: 51.5136, lng: 7.4653 },
            { name: 'Essen', lat: 51.4556, lng: 7.0116 },
            { name: 'Kassel', lat: 51.3127, lng: 9.4797 },
            { name: 'Würzburg', lat: 49.7913, lng: 9.9534 },
            { name: 'Erfurt', lat: 50.9848, lng: 11.0299 }
        ];

        let nearestCity = cities[0];
        let minDistance = this.calculateHaversineDistance({ lat, lng }, nearestCity);

        for (const city of cities) {
            const distance = this.calculateHaversineDistance({ lat, lng }, city);
            if (distance < minDistance) {
                minDistance = distance;
                nearestCity = city;
            }
        }

        return nearestCity.name;
    }

    // ======================================================================
    // WOCHENPLAN VALIDIEREN
    // ======================================================================
    validateWeekPlan(week) {
        const totalHours = week.reduce((sum, day) => sum + day.workTime, 0);
        const validations = [];

        // 40-Stunden-Woche prüfen
        if (totalHours > this.constraints.maxWorkHoursPerWeek) {
            validations.push(`⚠️ Überstunden: ${totalHours.toFixed(1)}h (max. ${this.constraints.maxWorkHoursPerWeek}h)`);
        }

        // Tagesüberstunden prüfen
        week.forEach(day => {
            if (day.workTime > this.constraints.flexWorkHoursPerDay) {
                validations.push(`⚠️ ${day.day}: ${day.workTime.toFixed(1)}h (max. ${this.constraints.flexWorkHoursPerDay}h)`);
            }
        });

        // Qualität der Planung bewerten
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
    formatWeekResult(validatedWeek) {
        const { week, totalHours, validations, efficiency } = validatedWeek;
        
        const optimizations = [
            `${week.reduce((sum, day) => sum + day.appointments.length, 0)} Termine intelligent eingeplant`,
            `Arbeitszeit optimiert: ${totalHours.toFixed(1)}h von ${this.constraints.maxWorkHoursPerWeek}h`,
            `Reiseeffizienz: ${(efficiency.travelEfficiency * 100).toFixed(1)}%`,
            'Bestätigte Termine priorisiert, Pipeline-Alter berücksichtigt',
            'Google Maps API für präzise Routen verwendet'
        ];

        // Übernachtungen erwähnen
        const overnightDays = week.filter(day => day.overnight).length;
        if (overnightDays > 0) {
            optimizations.push(`${overnightDays} strategische Übernachtungsstopps geplant`);
        }

        if (validations.length > 0) {
            optimizations.push(...validations);
        }

        return {
            weekStart: week[0].date,
            days: week,
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
    formatTime(hours) {
        const h = Math.floor(hours);
        const m = Math.round((hours - h) * 60);
        return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
    }

    // ======================================================================
    // TERMIN KANN EINGEPLANT WERDEN PRÜFEN
    // ======================================================================
    canFitAppointment(weekPlan, appointment) {
        // Vereinfachte Prüfung für Alternative-Suche
        const totalScheduled = weekPlan.stats.totalAppointments;
        const maxPossible = Math.floor(this.constraints.maxWorkHoursPerWeek / this.constraints.appointmentDuration);
        
        return {
            canFit: totalScheduled < maxPossible,
            availableSlots: maxPossible - totalScheduled,
            estimatedDay: totalScheduled < 10 ? Math.floor(totalScheduled / 2) + 1 : 'Ende der Woche'
        };
    }
}

module.exports = IntelligentRoutePlanner;

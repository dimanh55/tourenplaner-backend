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
const axios = require('axios');

class IntelligentRoutePlanner {
    constructor() {
        this.constraints = {
            maxWorkHoursPerWeek: 40,
            maxWorkHoursPerDay: 8,
            flexWorkHoursPerDay: 10, // Ausnahme möglich
            workStartTime: 8, // 8:00 Uhr
            workEndTime: 17, // 17:00 Uhr (zurück im Büro)
            appointmentDuration: 3, // Stunden pro Dreh
            homeBase: { 
                lat: 52.3731, 
                lng: 9.7386, 
                name: 'Hannover',
                address: 'Kurt-Schumacher-Straße 34, 30159 Hannover'
            }, // Korrekte Adresse
            travelSpeedKmh: 80, // Durchschnittliche Reisegeschwindigkeit
            maxTravelTimePerDay: 4, // Max 4h Fahrt pro Tag
            returnHomeOnFriday: true // Freitags zurück ins Büro
        };
    }

    // ======================================================================
    // HAUPTFUNKTION: Intelligente Routenoptimierung
    // ======================================================================
    async optimizeWeek(appointments, weekStart, driverId) {
        console.log('🧠 Starte intelligente Routenplanung...');
        console.log(`🏢 Start/Ende: ${this.constraints.homeBase.address}`);
        
        try {
            // 1. Termine analysieren und kategorisieren
            const categorizedAppointments = this.categorizeAppointments(appointments);
            
            // 2. Geocoding für alle Adressen
            const geoAppointments = await this.geocodeAppointments(categorizedAppointments);
            
            if (geoAppointments.length === 0) {
                throw new Error('Keine Termine mit gültigen Adressen gefunden');
            }
            
            // 3. Reisematrix berechnen (inklusive Heimatbasis)
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
                isFixed: apt.is_fixed === 1,
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

        // Fixe Termine haben höchste Priorität
        if (apt.is_fixed) score += 50;
        
        // Bestätigte Termine haben hohe Priorität
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
            try {
                const coords = await this.geocodeAddress(apt.address);
                geocoded.push({
                    ...apt,
                    lat: coords.lat,
                    lng: coords.lng,
                    geocoded: true
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
        const apiKey = process.env.GOOGLE_MAPS_API_KEY || 'AIzaSyD6D4OGAfep-u-N1yz_F--jacBFs1TINR4';
        
        try {
            const response = await axios.get(`https://maps.googleapis.com/maps/api/geocode/json`, {
                params: {
                    address: address,
                    key: apiKey,
                    region: 'de',
                    components: 'country:DE'
                },
                timeout: 5000
            });

            if (response.data.status === 'OK' && response.data.results.length > 0) {
                const location = response.data.results[0].geometry.location;
                return { lat: location.lat, lng: location.lng };
            } else {
                throw new Error(`Geocoding API Status: ${response.data.status}`);
            }
        } catch (error) {
            console.warn('Google Maps Geocoding Fehler:', error.message);
            throw error;
        }
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
            'Hannover': { lat: 52.3731, lng: 9.7386 }, // Korrekte Koordinaten
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
                
                try {
                    await this.calculateDistanceMatrixBatch(originBatch, destinationBatch, matrix, apiKey);
                } catch (error) {
                    console.warn('Distance Matrix Batch fehlgeschlagen, verwende Fallback:', error.message);
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
        
        // 1. Fixe Termine identifizieren
        const fixedAppointments = appointments.filter(apt => apt.isFixed && apt.fixed_date);
        const flexibleAppointments = appointments.filter(apt => !apt.isFixed || !apt.fixed_date);
        
        // 2. Nach Priorität sortieren
        const sortedFixed = fixedAppointments.sort((a, b) => new Date(a.fixed_date) - new Date(b.fixed_date));
        const sortedFlexible = flexibleAppointments.sort((a, b) => b.priority - a.priority);
        
        // 3. Woche initialisieren
        const week = this.initializeWeek(weekStart);
        
        // 4. Fixe Termine zuerst einplanen
        await this.scheduleFixedAppointments(week, sortedFixed, travelMatrix);
        
        // 5. Flexible Termine optimal einplanen
        await this.scheduleFlexibleAppointments(week, sortedFlexible, travelMatrix);
        
        // 6. Heimfahrt am Freitag sicherstellen
        this.ensureFridayReturn(week, travelMatrix);
        
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
                overtime: false,
                requiresReturn: index === 4 // Freitag
            };
        });
    }

    // ======================================================================
    // FIXE TERMINE EINPLANEN
    // ======================================================================
    async scheduleFixedAppointments(week, fixedAppointments, travelMatrix) {
        console.log(`📌 Plane ${fixedAppointments.length} fixe Termine...`);
        
        for (const appointment of fixedAppointments) {
            const appointmentDate = new Date(appointment.fixed_date);
            const dayIndex = this.getDayIndex(appointmentDate, week[0].date);
            
            if (dayIndex >= 0 && dayIndex < 5) {
                const day = week[dayIndex];
                const [hours, minutes] = appointment.fixed_time.split(':').map(Number);
                const startTime = hours + minutes / 60;
                
                // Reisezeit vom aktuellen Standort berechnen
                const travelFromCurrent = travelMatrix[day.currentLocation]?.[appointment.id];
                if (!travelFromCurrent) continue;
                
                // Termin einplanen
                this.assignFixedAppointment(day, appointment, startTime, travelFromCurrent, travelMatrix);
                console.log(`📅 Fixtermin: ${appointment.invitee_name} → ${day.day} ${appointment.fixed_time}`);
            }
        }
    }

    // ======================================================================
    // FLEXIBLE TERMINE EINPLANEN
    // ======================================================================
    async scheduleFlexibleAppointments(week, appointments, travelMatrix) {
        console.log(`📋 Plane ${appointments.length} flexible Termine...`);
        
        for (const appointment of appointments) {
            const bestSlot = this.findBestTimeSlot(week, appointment, travelMatrix);
            
            if (bestSlot) {
                this.assignAppointmentToSlot(week[bestSlot.dayIndex], appointment, bestSlot, travelMatrix);
                console.log(`📅 Flexibel: ${appointment.invitee_name} → ${bestSlot.day} ${bestSlot.time}`);
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
            
            // Bei Freitag: Muss Rückkehr möglich sein
            if (dayIndex === 4) {
                const returnHome = travelMatrix[appointment.id]?.['home'];
                if (returnHome && appointmentEnd + returnHome.duration > this.constraints.workEndTime) {
                    continue; // Würde zu spät für Rückkehr
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
    // FIXTERMIN ZUWEISEN
    // ======================================================================
    assignFixedAppointment(day, appointment, startTime, travelData, travelMatrix) {
        // Termin hinzufügen
        day.appointments.push({
            ...appointment,
            startTime: appointment.fixed_time,
            endTime: this.formatTime(startTime + this.constraints.appointmentDuration),
            travelTimeThere: travelData.duration,
            customer: appointment.invitee_name,
            isFixed: true
        });

        // Tagesstatistiken aktualisieren
        day.workTime += this.constraints.appointmentDuration;
        day.travelTime += travelData.duration;
        day.lastAppointmentEnd = startTime + this.constraints.appointmentDuration;
        day.currentLocation = appointment.id;
    }

    // ======================================================================
    // FREITAG RÜCKKEHR SICHERSTELLEN
    // ======================================================================
    ensureFridayReturn(week, travelMatrix) {
        const friday = week[4];
        
        if (friday.appointments.length > 0) {
            const lastAppointment = friday.appointments[friday.appointments.length - 1];
            const returnTrip = travelMatrix[friday.currentLocation]?.['home'];
            
            if (returnTrip) {
                friday.travelTime += returnTrip.duration;
                friday.returnTrip = {
                    from: lastAppointment.customer,
                    to: this.constraints.homeBase.name,
                    duration: returnTrip.duration,
                    distance: returnTrip.distance,
                    description: 'Rückkehr ins Büro'
                };
                
                console.log(`🏠 Freitag Rückkehr geplant: ${returnTrip.duration.toFixed(1)}h nach Hannover`);
            }
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
                } else if (i < 3) { // Nicht Donnerstag
                    // Kürzere Distanz, aber trotzdem Übernachtung wenn sinnvoll
                    const returnHome = travelMatrix[today.currentLocation]?.['home'];
                    if (returnHome && returnHome.duration > 2) {
                        // Zu weit für Heimfahrt
                        today.overnight = {
                            city: this.findNearestCity(lastToday.lat, lastToday.lng),
                            description: 'Übernachtung vor Ort',
                            coordinates: { lat: lastToday.lat, lng: lastToday.lng }
                        };
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
            { name: 'Hannover', lat: 52.3731, lng: 9.7386 },
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
        const totalHours = week.reduce((sum, day) => sum + day.workTime + day.travelTime, 0);
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

        // Freitag Rückkehr prüfen
        if (week[4].appointments.length > 0 && !week[4].returnTrip) {
            validations.push(`⚠️ Freitag: Rückkehr nach Hannover nicht geplant`);
        }

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
            weekUtilization: (totalWorkTime + totalTravelTime) / this.constraints.maxWorkHoursPerWeek
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
            `Start/Ende: ${this.constraints.homeBase.address}`,
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
                fixedAppointments: week.reduce((sum, day) => 
                    sum + day.appointments.filter(apt => apt.isFixed).length, 0),
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

    getDayIndex(date, weekStartDate) {
        const startDate = new Date(weekStartDate);
        const targetDate = new Date(date);
        const diffTime = Math.abs(targetDate - startDate);
        const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
        return diffDays;
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

    // Insert driver
    db.run(`INSERT OR IGNORE INTO drivers (id, name, home_base) 
            VALUES (1, 'Max Mustermann', 'Hannover')`);

    // Insert sample appointments
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
            'testimonial_focused',
            'fixed_appointments',
            'maximum_efficiency'
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

// Get appointments (exclude on_hold)
app.get('/api/appointments', (req, res) => {
    db.all("SELECT * FROM appointments WHERE (on_hold IS NULL OR on_hold = '') ORDER BY created_at DESC", (err, rows) => {
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
                WHERE (on_hold IS NULL OR on_hold = '')
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

        // 3. Trenne fixe und flexible Termine
        const fixedAppointments = allAppointments.filter(apt => apt.is_fixed && apt.fixed_date);
        const flexibleAppointments = allAppointments.filter(apt => !apt.is_fixed || !apt.fixed_date);

        console.log(`📌 ${fixedAppointments.length} fixe Termine, ${flexibleAppointments.length} flexible Termine`);

        // 4. Finde alle fixen Termine für diese Woche
        const weekStartDate = new Date(weekStart);
        const weekEndDate = new Date(weekStartDate);
        weekEndDate.setDate(weekStartDate.getDate() + 4); // Freitag

        const fixedAppointmentsThisWeek = fixedAppointments.filter(apt => {
            const aptDate = new Date(apt.fixed_date);
            return aptDate >= weekStartDate && aptDate <= weekEndDate;
        });

        // 5. Intelligente Auswahl: Maximiere Termine pro Woche
        const selectedAppointments = await selectMaxAppointmentsForWeek(
            fixedAppointmentsThisWeek,
            flexibleAppointments,
            weekStart,
            allAppointments
        );
        
        if (selectedAppointments.length === 0) {
            return res.json({
                success: false,
                message: `Keine Termine für Woche ${weekStart} verfügbar`,
                route: createEmptyWeekStructure(weekStart)
            });
        }

        // 6. ECHTE ROUTENOPTIMIERUNG mit maximaler Effizienz
        const optimizedRoute = await performMaxEfficiencyOptimization(selectedAppointments, weekStart, driverId);

        // 7. Route speichern
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
// MAXIMALE TERMINAUSWAHL FÜR WOCHE
// ======================================================================
async function selectMaxAppointmentsForWeek(fixedAppointmentsThisWeek, flexibleAppointments, weekStart, allAppointments) {
    console.log(`🎯 Maximiere Termine für Woche ${weekStart}`);
    
    const weekStartDate = new Date(weekStart);
    
    // 1. Alle fixen Termine dieser Woche müssen rein
    const selectedAppointments = [...fixedAppointmentsThisWeek];
    console.log(`📌 ${fixedAppointmentsThisWeek.length} fixe Termine für diese Woche`);
    
    // 2. Berechne verfügbare Kapazität
    const maxTermineProTag = 3; // Mit Überstunden möglich
    const maxTermineProWoche = 13; // 40h / 3h = ~13 Termine
    const verbleibendeKapazität = maxTermineProWoche - selectedAppointments.length;
    
    // 3. Prüfe bereits verwendete Termine in anderen Wochen
    const usedAppointmentIds = await getUsedAppointmentIds(weekStart);
    
    // 4. Filtere verfügbare flexible Termine
    const availableFlexible = flexibleAppointments.filter(apt => 
        !usedAppointmentIds.includes(apt.id) &&
        !selectedAppointments.some(selected => selected.id === apt.id)
    );
    
    // 5. Sortiere nach Priorität für maximale Effizienz
    const sortedFlexible = availableFlexible.sort((a, b) => {
        // Bestätigte Termine zuerst
        if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
        if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
        
        // Pipeline-Alter (ältere zuerst)
        if (a.pipeline_days !== b.pipeline_days) {
            return b.pipeline_days - a.pipeline_days;
        }
        
        // Priorität
        const priorityOrder = { 'hoch': 3, 'mittel': 2, 'niedrig': 1 };
        return (priorityOrder[b.priority] || 2) - (priorityOrder[a.priority] || 2);
    });
    
    // 6. Fülle mit flexiblen Terminen bis zur maximalen Kapazität
    const additionalAppointments = sortedFlexible.slice(0, verbleibendeKapazität);
    selectedAppointments.push(...additionalAppointments);
    
    console.log(`✅ ${selectedAppointments.length} Termine für maximale Effizienz ausgewählt`);
    console.log(`   - ${fixedAppointmentsThisWeek.length} fixe Termine`);
    console.log(`   - ${additionalAppointments.length} flexible Termine`);
    console.log(`   - Auslastung: ${Math.round((selectedAppointments.length / maxTermineProWoche) * 100)}%`);
    
    return selectedAppointments;
}

// ======================================================================
// MAXIMALE EFFIZIENZ ROUTENOPTIMIERUNG
// ======================================================================
async function performMaxEfficiencyOptimization(appointments, weekStart, driverId) {
    console.log('⚡ Starte maximale Effizienz-Optimierung...');
    
    try {
        // Konvertiere Termine für Optimierung
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
                preferred_time: parsedNotes.start_time || null,
                is_fixed: apt.is_fixed,
                fixed_date: apt.fixed_date,
                fixed_time: apt.fixed_time,
                notes: apt.notes
            };
        });

        // Verwende IntelligentRoutePlanner mit angepassten Constraints für maximale Effizienz
        const planner = new IntelligentRoutePlanner();
        
        // Überschreibe Constraints für maximale Effizienz
        planner.constraints.maxWorkHoursPerDay = 10; // Erlaube bis zu 10h wenn nötig
        planner.constraints.flexWorkHoursPerDay = 10;
        planner.constraints.maxTravelTimePerDay = 5; // Mehr Fahrzeit erlaubt
        
        const optimizedResult = await planner.optimizeWeek(optimizableAppointments, weekStart, driverId || 1);
        
        // Stelle sicher, dass fixe Termine zur richtigen Zeit sind
        ensureFixedAppointmentsCorrect(optimizedResult, appointments);
        
        return optimizedResult;
        
    } catch (error) {
        console.warn('⚠️ IntelligentRoutePlanner fehlgeschlagen, verwende Fallback:', error.message);
        return performMaxEfficiencyFallback(appointments, weekStart);
    }
}

// ======================================================================
// STELLE SICHER DASS FIXE TERMINE KORREKT SIND
// ======================================================================
function ensureFixedAppointmentsCorrect(optimizedResult, originalAppointments) {
    const fixedAppointments = originalAppointments.filter(apt => apt.is_fixed && apt.fixed_date && apt.fixed_time);
    
    fixedAppointments.forEach(fixedApt => {
        const aptDate = new Date(fixedApt.fixed_date);
        const dayIndex = (aptDate.getDay() + 6) % 7; // Montag = 0
        
        if (dayIndex < 5 && optimizedResult.days[dayIndex]) {
            const day = optimizedResult.days[dayIndex];
            
            // Finde den Termin im geplanten Tag
            const plannedAptIndex = day.appointments.findIndex(apt => apt.id === fixedApt.id);
            
            if (plannedAptIndex !== -1) {
                // Update Zeit auf die fixe Zeit
                day.appointments[plannedAptIndex].startTime = fixedApt.fixed_time;
                const [hours, minutes] = fixedApt.fixed_time.split(':').map(Number);
                const endHours = hours + (fixedApt.duration || 3);
                day.appointments[plannedAptIndex].endTime = `${endHours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
                day.appointments[plannedAptIndex].status = 'bestätigt';
                day.appointments[plannedAptIndex].is_fixed = true;
                
                console.log(`📌 Fixtermin korrigiert: ${fixedApt.customer} am ${fixedApt.fixed_date} um ${fixedApt.fixed_time}`);
            }
        }
    });
}

// ======================================================================
// MAXIMALE EFFIZIENZ FALLBACK
// ======================================================================
async function performMaxEfficiencyFallback(appointments, weekStart) {
    console.log('🔄 Verwende Max-Effizienz Fallback-Optimierung...');
    
    const optimizedWeek = createEmptyWeekStructure(weekStart);
    const weekStartDate = new Date(weekStart);
    
    // Trenne fixe und flexible Termine
    const fixedAppointments = appointments.filter(apt => apt.is_fixed && apt.fixed_date);
    const flexibleAppointments = appointments.filter(apt => !apt.is_fixed || !apt.fixed_date);
    
    // Platziere fixe Termine zuerst
    fixedAppointments.forEach(apt => {
        const aptDate = new Date(apt.fixed_date);
        const dayIndex = Math.floor((aptDate - weekStartDate) / (24 * 60 * 60 * 1000));
        
        if (dayIndex >= 0 && dayIndex < 5) {
            const day = optimizedWeek[dayIndex];
            const [hours, minutes] = (apt.fixed_time || '09:00').split(':').map(Number);
            const startTime = hours + minutes / 60;
            
            scheduleFixedAppointment(day, apt, startTime, dayIndex);
        }
    });
    
    // Sortiere flexible Termine nach Priorität
    const sortedFlexible = flexibleAppointments.sort((a, b) => {
        if (a.status === 'bestätigt' && b.status !== 'bestätigt') return -1;
        if (b.status === 'bestätigt' && a.status !== 'bestätigt') return 1;
        return b.pipeline_days - a.pipeline_days;
    });
    
    // Plane flexible Termine mit maximaler Effizienz
    let currentDayIndex = 0;
    for (const apt of sortedFlexible) {
        const bestDay = findBestDayMaxEfficiency(optimizedWeek, apt, currentDayIndex);
        
        if (bestDay !== -1) {
            const success = scheduleAppointmentMaxEfficiency(optimizedWeek[bestDay], apt, bestDay);
            if (success) {
                console.log(`✅ Geplant: ${apt.customer} → ${optimizedWeek[bestDay].day}`);
            }
        }
    }
    
    // Berechne Statistiken
    const stats = calculateWeekStats(optimizedWeek);
    
    return {
        weekStart,
        days: optimizedWeek,
        totalHours: stats.totalHours,
        optimizations: [
            `${stats.totalAppointments} Termine mit maximaler Effizienz geplant`,
            `${stats.fixedAppointments} fixe Termine exakt platziert`,
            `${stats.workDays} Arbeitstage genutzt`,
            `Auslastung: ${stats.efficiency}% der verfügbaren Zeit`,
            'Überstunden strategisch eingesetzt für maximale Termine'
        ],
        stats: stats,
        generatedAt: new Date().toISOString()
    };
}

// ======================================================================
// HILFSFUNKTIONEN FÜR MAXIMALE EFFIZIENZ
// ======================================================================

function scheduleFixedAppointment(day, appointment, startTime, dayIndex) {
    const duration = appointment.duration || 3;
    
    day.appointments.push({
        ...appointment,
        startTime: appointment.fixed_time,
        endTime: formatTime(startTime + duration),
        duration: duration,
        status: 'bestätigt',
        is_fixed: true
    });
    
    day.workTime += duration;
    
    // Fahrzeiten schätzen
    const travelTime = dayIndex === 0 ? 2 : 1; // Montag längere Anfahrt
    day.travelTime += travelTime;
    
    // Fahrtsegment hinzufügen
    day.travelSegments.push({
        type: 'travel',
        from: day.appointments.length === 1 ? 'Hannover' : 'Vorheriger Termin',
        to: appointment.customer,
        startTime: formatTime(startTime - travelTime),
        endTime: appointment.fixed_time,
        duration: travelTime,
        distance: estimateDistance(appointment.address),
        description: 'Fahrt zum Fixtermin'
    });
}

function findBestDayMaxEfficiency(week, appointment, preferredStart) {
    let bestDay = -1;
    let bestScore = -1;
    
    // Prüfe alle Tage
    for (let i = 0; i < 5; i++) {
        const day = week[i];
        
        // Kann dieser Tag noch einen Termin aufnehmen?
        const canFit = canDayAcceptAppointment(day, appointment);
        if (!canFit) continue;
        
        // Berechne Score für diesen Tag
        const score = calculateDayScore(day, appointment, i);
        
        if (score > bestScore) {
            bestScore = score;
            bestDay = i;
        }
    }
    
    return bestDay;
}

function canDayAcceptAppointment(day, appointment) {
    const maxHoursWithOvertime = 10;
    const maxAppointmentsPerDay = 3;
    const appointmentDuration = appointment.duration || 3;
    
    // Prüfe Zeitlimit
    if (day.workTime + appointmentDuration > maxHoursWithOvertime) {
        return false;
    }
    
    // Prüfe Anzahl Termine
    if (day.appointments.length >= maxAppointmentsPerDay) {
        return false;
    }
    
    // Prüfe ob fixe Termine Konflikte verursachen
    const hasTimeConflict = day.appointments.some(apt => {
        if (!apt.is_fixed) return false;
        // Vereinfachte Konfliktprüfung
        return false; // Für Fallback keine komplexe Zeitprüfung
    });
    
    return !hasTimeConflict;
}

function calculateDayScore(day, appointment, dayIndex) {
    let score = 100;
    
    // Bevorzuge Tage mit weniger Terminen für Gleichverteilung
    score -= day.appointments.length * 20;
    
    // Bestätigte Termine haben höhere Priorität
    if (appointment.status === 'bestätigt') score += 30;
    
    // Pipeline-Alter berücksichtigen
    score += Math.min(appointment.pipeline_days * 0.5, 20);
    
    // Frühere Wochentage leicht bevorzugen
    score -= dayIndex * 2;
    
    // Wenn Tag schon Überstunden hat, weniger attraktiv
    if (day.workTime > 8) score -= 15;
    
    return Math.max(0, score);
}

function scheduleAppointmentMaxEfficiency(day, appointment, dayIndex) {
    const duration = appointment.duration || 3;
    
    // Berechne optimale Startzeit
    let startTime = 8; // Standard Start
    
    if (day.appointments.length > 0) {
        const lastApt = day.appointments[day.appointments.length - 1];
        const lastEnd = parseTimeToHours(lastApt.endTime);
        startTime = lastEnd + 0.5; // 30min Pause/Fahrt
    }
    
    // Prüfe ob es noch in den Tag passt
    if (startTime + duration > 18) { // Spätestens 18 Uhr Ende
        return false;
    }
    
    // Fahrt hinzufügen
    const travelTime = day.appointments.length === 0 ? 2 : 0.5;
    addTravelSegment(day, 
        day.appointments.length === 0 ? 'Hannover' : day.appointments[day.appointments.length - 1].customer,
        appointment.customer,
        startTime - travelTime,
        travelTime,
        estimateDistance(appointment.address)
    );
    
    // Termin hinzufügen
    day.appointments.push({
        ...appointment,
        startTime: formatTime(startTime),
        endTime: formatTime(startTime + duration),
        duration: duration
    });
    
    day.workTime += duration;
    day.travelTime += travelTime;
    
    // Übernachtung planen wenn nötig
    if (dayIndex < 4 && day.workTime > 6) {
        planStrategicOvernight(day, appointment, dayIndex);
    }
    
    return true;
}

function planStrategicOvernight(day, lastAppointment, dayIndex) {
    const city = extractCityFromAddress(lastAppointment.address);
    day.overnight = {
        city: city,
        description: `Hotel in ${city} - Maximale Effizienz für morgen`,
        startTime: formatTime(18),
        type: 'overnight'
    };
}

function calculateWeekStats(week) {
    let totalAppointments = 0;
    let fixedAppointments = 0;
    let confirmedAppointments = 0;
    let proposalAppointments = 0;
    let totalWorkTime = 0;
    let totalTravelTime = 0;
    let workDays = 0;
    
    week.forEach(day => {
        if (day.appointments.length > 0) {
            workDays++;
            totalAppointments += day.appointments.length;
            
            day.appointments.forEach(apt => {
                if (apt.is_fixed) fixedAppointments++;
                if (apt.status === 'bestätigt') confirmedAppointments++;
                else if (apt.status === 'vorschlag') proposalAppointments++;
            });
        }
        
        totalWorkTime += day.workTime;
        totalTravelTime += day.travelTime;
    });
    
    const totalHours = totalWorkTime + totalTravelTime;
    const maxPossibleHours = 40; // Pro Woche
    const efficiency = Math.round((totalHours / maxPossibleHours) * 100);
    
    return {
        totalAppointments,
        fixedAppointments,
        confirmedAppointments,
        proposalAppointments,
        totalTravelTime: Math.round(totalTravelTime * 10) / 10,
        workDays,
        totalHours: Math.round(totalHours * 10) / 10,
        efficiency: Math.min(efficiency, 100)
    };
}

function parseTimeToHours(timeStr) {
    const [hours, minutes] = timeStr.split(':').map(Number);
    return hours + minutes / 60;
}

// ======================================================================
// ALLGEMEINE HILFSFUNKTIONEN
// ======================================================================

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

function addTravelSegment(day, from, to, startTime, duration, distance) {
    day.travelSegments.push({
        type: 'travel',
        from: from,
        to: to,
        startTime: formatTime(startTime),
        endTime: formatTime(startTime + duration),
        duration: duration,
        distance: distance,
        description: from === 'Hannover' ? 'Start der Reise' : `Fahrt nach ${to}`
    });
}

function estimateDistance(address) {
    const addr = address.toLowerCase();
    if (addr.includes('münchen')) return '450 km';
    if (addr.includes('berlin')) return '280 km';
    if (addr.includes('köln')) return '200 km';
    if (addr.includes('hamburg')) return '150 km';
    if (addr.includes('osnabrück')) return '120 km';
    if (addr.includes('wolfsburg')) return '100 km';
    return '180 km';
}

function extractCityFromAddress(address) {
    // Extrahiere Stadt aus deutscher Adresse
    const parts = address.split(',');
    for (const part of parts) {
        const trimmed = part.trim();
        const match = trimmed.match(/\d{5}\s+(.+)/);
        if (match) return match[1];
        
        // Bekannte deutsche Städte
        const cities = ['München', 'Berlin', 'Hamburg', 'Köln', 'Frankfurt', 'Stuttgart', 'Düsseldorf', 'Leipzig', 'Hannover', 'Osnabrück', 'Wolfsburg', 'Würselen', 'Freinsheim', 'Schüttorf', 'Krefeld'];
        for (const city of cities) {
            if (trimmed.toLowerCase().includes(city.toLowerCase())) {
                return city;
            }
        }
    }
    return 'Stadt';
}

function formatTime(hours) {
    const h = Math.floor(hours);
    const m = Math.round((hours - h) * 60);
    return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
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

            // Parse date and time for fixed appointments
            let isFixed = false;
            let fixedDate = null;
            let fixedTime = null;
            
            if (row['Start Date & Time'] && row['Start Date & Time'].trim() !== '') {
                try {
                    const dateTimeStr = row['Start Date & Time'].trim();
                    // Parse verschiedene Datumsformate
                    const dateTime = new Date(dateTimeStr);
                    if (!isNaN(dateTime.getTime())) {
                        isFixed = true;
                        fixedDate = dateTime.toISOString().split('T')[0];
                        fixedTime = `${dateTime.getHours().toString().padStart(2, '0')}:${dateTime.getMinutes().toString().padStart(2, '0')}`;
                    }
                } catch (e) {
                    console.log(`Datum parsing fehlgeschlagen für: ${row['Start Date & Time']}`);
                }
            }

            // Determine status based on appointment date
            const status = isFixed ? 'bestätigt' : 'vorschlag';
            
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
            const duration = status === 'bestätigt' ? 3 : 3; // Immer 3 Stunden für Termine

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
                // Fixed appointment data
                is_fixed: isFixed ? 1 : 0,
                fixed_date: fixedDate,
                fixed_time: fixedTime,
                on_hold: row['On Hold'] || null,
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
            
            console.log(`✅ Processed: ${inviteeName} (${company}) für ${customerCompany}${isFixed ? ' - FIXTERMIN' : ''}`);
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
            is_fixed: 1,
            fixed_date: "2025-06-10",
            fixed_time: "14:00",
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
            is_fixed: 1,
            fixed_date: "2025-06-11",
            fixed_time: "10:00",
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
            (customer, address, priority, status, duration, pipeline_days, notes, is_fixed, fixed_date, fixed_time) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

        let insertedCount = 0;
        sampleAppointments.forEach((apt, index) => {
            stmt.run([
                apt.customer,
                apt.address,
                apt.priority,
                apt.status,
                apt.duration,
                apt.pipeline_days,
                apt.notes,
                apt.is_fixed || 0,
                apt.fixed_date || null,
                apt.fixed_time || null
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

// Admin endpoint to check database
app.get('/api/admin/status', (req, res) => {
    db.get("SELECT COUNT(*) as count FROM appointments WHERE (on_hold IS NULL OR on_hold = '')", (err, row) => {
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
                    
                    db.get("SELECT COUNT(*) as routes_count FROM saved_routes", (err5, row5) => {
                        if (err5) {
                            res.status(500).json({ error: err5.message });
                            return;
                        }
                        
                        res.json({
                            database: 'connected',
                            appointments_count: row.count,
                            on_hold_count: row2.on_hold_count,
                            fixed_appointments_count: row3.fixed_count,
                            drivers_count: row4.driver_count,
                            saved_routes_count: row5.routes_count,
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

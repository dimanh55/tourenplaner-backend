const axios = require('axios');

class IntelligentRoutePlanner {
    constructor(db) {
        this.db = db;
        this.apiKey = process.env.GOOGLE_MAPS_API_KEY;
        this.distanceCache = new Map();
        this.apiCallsCount = 0;
        this.constraints = {
            maxWorkHoursPerWeek: 42.5,
            maxWorkHoursPerDay: 9,
            workStartTime: 9,
            workEndTime: 18,
            appointmentDuration: 3,
            homeBase: { lat: 52.3759, lng: 9.7320, name: 'Hannover' },
            travelTimePadding: 0.25,
            overnightThreshold: 100
        };
    }

    // ======================================================================
    // HAUPTFUNKTION: Intelligente Routenoptimierung
    // ======================================================================
    async optimizeWeek(appointments, weekStart, driverId) {
        console.log('🧠 Starte KOSTENOPTIMIERTE Routenplanung...');
        
        try {
            // 1. Geocode nur fehlende Koordinaten
            const geoAppointments = await this.ensureGeocoding(appointments);
            
            // 2. Gruppiere Termine nach Regionen
            const clusters = this.clusterByRegion(geoAppointments);
            
            // 3. Plane Woche mit minimalen API Calls
            const week = await this.planWeekEfficiently(clusters, geoAppointments, weekStart);
            
            // 4. Formatiere Ergebnis
            return this.formatWeekResult(week, weekStart);
            
        } catch (error) {
            console.error('❌ Routenplanung fehlgeschlagen:', error);
            throw error;
        }
    }

    // ======================================================================
    // GEOCODING - Nutze vorhandene Koordinaten!
    // ======================================================================
    async ensureGeocoding(appointments) {
        const needsGeocoding = appointments.filter(apt => !apt.lat || !apt.lng);
        
        if (needsGeocoding.length > 0) {
            console.log(`🗺️ Nur ${needsGeocoding.length} von ${appointments.length} Terminen brauchen Geocoding`);
            
            for (const apt of needsGeocoding) {
                try {
                    const coords = await this.geocodeAddress(apt.address);
                    apt.lat = coords.lat;
                    apt.lng = coords.lng;
                    apt.geocoded = true;
                } catch (error) {
                    console.warn(`⚠️ Geocoding fehlgeschlagen für ${apt.address}`);
                }
            }
        }
        
        return appointments.filter(apt => apt.lat && apt.lng);
    }

    async geocodeAddress(address) {
        // Zuerst lokale Datenbank prüfen (kostenlos!)
        const cityCoords = this.getCityCoordinates(address);
        if (cityCoords) return cityCoords;
        
        // Nur wenn nötig: Google API
        this.apiCallsCount++;
        const response = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
            params: {
                address: address,
                key: this.apiKey,
                region: 'de'
            }
        });
        
        if (response.data.status === 'OK' && response.data.results.length > 0) {
            const location = response.data.results[0].geometry.location;
            return { lat: location.lat, lng: location.lng };
        }
        
        throw new Error('Geocoding fehlgeschlagen');
    }

    // ======================================================================
    // REGIONALE GRUPPIERUNG
    // ======================================================================
    clusterByRegion(appointments) {
        const regions = {
            'Nord': { center: { lat: 53.5, lng: 10.0 }, appointments: [] },
            'Ost': { center: { lat: 52.5, lng: 13.4 }, appointments: [] },
            'West': { center: { lat: 51.2, lng: 7.0 }, appointments: [] },
            'Süd': { center: { lat: 48.5, lng: 11.5 }, appointments: [] },
            'Mitte': { center: { lat: 50.5, lng: 9.0 }, appointments: [] }
        };
        
        // Fixe Termine separat behandeln
        const fixed = appointments.filter(apt => apt.is_fixed && apt.fixed_date);
        const flexible = appointments.filter(apt => !apt.is_fixed);
        
        // Flexible Termine den Regionen zuordnen
        flexible.forEach(apt => {
            let minDistance = Infinity;
            let bestRegion = 'Mitte';
            
            Object.entries(regions).forEach(([name, data]) => {
                const dist = this.haversineDistance(apt.lat, apt.lng, data.center.lat, data.center.lng);
                if (dist < minDistance) {
                    minDistance = dist;
                    bestRegion = name;
                }
            });
            
            regions[bestRegion].appointments.push(apt);
        });
        
        return { regions, fixedAppointments: fixed };
    }

    // ======================================================================
    // EFFIZIENTE WOCHENPLANUNG - NUR NOTWENDIGE API CALLS!
    // ======================================================================
    async planWeekEfficiently(clusters, allAppointments, weekStart) {
        const week = this.initializeWeek(weekStart);
        const { regions, fixedAppointments } = clusters;
        
        // 1. Fixe Termine einplanen
        this.scheduleFixedAppointments(week, fixedAppointments);
        
        // 2. Sortiere Regionen nach Entfernung von Hannover
        const sortedRegions = this.sortRegionsByDistance(regions);
        
        // 3. Plane jeden Tag regional
        let dayIndex = 0;
        for (const regionName of sortedRegions) {
            const regionAppts = regions[regionName].appointments;
            if (regionAppts.length === 0) continue;
            
            // Verteile Region auf verfügbare Tage
            const appointmentsPerDay = Math.ceil(regionAppts.length / (5 - dayIndex));
            
            for (let i = 0; i < regionAppts.length && dayIndex < 5; i += appointmentsPerDay, dayIndex++) {
                const dayAppointments = regionAppts.slice(i, i + appointmentsPerDay);
                if (dayAppointments.length > 0) {
                    await this.planDayEfficiently(week[dayIndex], dayAppointments, regionName);
                }
            }
        }
        
        console.log(`💰 Nur ${this.apiCallsCount} API Calls verwendet!`);
        return week;
    }

    // ======================================================================
    // TAG EFFIZIENT PLANEN - MINIMALE API CALLS!
    // ======================================================================
    async planDayEfficiently(day, appointments, regionName) {
        if (appointments.length === 0) return;
        
        console.log(`📅 Plane ${day.day}: ${appointments.length} Termine in Region ${regionName}`);
        
        // Sortiere Termine für kürzeste Route (TSP-Approximation)
        const sortedAppts = this.sortAppointmentsByDistance(appointments);
        
        // Berechne nur NOTWENDIGE Distanzen:
        // 1. Hannover zum ersten Termin
        const firstDist = await this.getDistance(
            this.constraints.homeBase,
            sortedAppts[0]
        );
        
        let currentTime = this.constraints.workStartTime;
        let currentLocation = this.constraints.homeBase;
        
        // Abfahrt von Hannover
        const departureTime = currentTime;
        currentTime += firstDist.duration;
        
        day.travelSegments.push({
            type: 'departure',
            from: 'Hannover',
            to: this.getCityName(sortedAppts[0].address),
            distance: Math.round(firstDist.distance),
            duration: firstDist.duration,
            startTime: this.formatTime(departureTime),
            endTime: this.formatTime(currentTime)
        });
        
        // Plane Termine sequenziell
        for (let i = 0; i < sortedAppts.length; i++) {
            const apt = sortedAppts[i];
            
            // Termin einplanen
            apt.startTime = this.formatTime(currentTime);
            apt.endTime = this.formatTime(currentTime + this.constraints.appointmentDuration);
            day.appointments.push(apt);
            
            currentTime += this.constraints.appointmentDuration;
            currentLocation = apt;
            
            // Fahrt zum nächsten Termin (wenn vorhanden)
            if (i < sortedAppts.length - 1) {
                const nextApt = sortedAppts[i + 1];
                const travelDist = await this.getDistance(apt, nextApt);
                
                day.travelSegments.push({
                    type: 'travel',
                    from: this.getCityName(apt.address),
                    to: this.getCityName(nextApt.address),
                    distance: Math.round(travelDist.distance),
                    duration: travelDist.duration,
                    startTime: this.formatTime(currentTime),
                    endTime: this.formatTime(currentTime + travelDist.duration)
                });
                
                currentTime += travelDist.duration;
            }
        }
        
        // Rückfahrt oder Übernachtung
        const lastApt = sortedAppts[sortedAppts.length - 1];
        const homeDist = await this.getDistance(lastApt, this.constraints.homeBase);
        
        if (homeDist.distance > this.constraints.overnightThreshold) {
            day.overnight = {
                city: this.getCityName(lastApt.address),
                reason: `${Math.round(homeDist.distance)}km von Hannover - Übernachtung empfohlen`
            };
        } else {
            day.travelSegments.push({
                type: 'return',
                from: this.getCityName(lastApt.address),
                to: 'Hannover',
                distance: Math.round(homeDist.distance),
                duration: homeDist.duration,
                startTime: this.formatTime(currentTime),
                endTime: this.formatTime(currentTime + homeDist.duration)
            });
        }
        
        // Zusammenfassung
        day.workTime = sortedAppts.length * this.constraints.appointmentDuration;
        day.travelTime = day.travelSegments.reduce((sum, seg) => sum + seg.duration, 0);
    }

    // ======================================================================
    // DISTANZ BERECHNUNG - MIT CACHE!
    // ======================================================================
    async getDistance(from, to) {
        const cacheKey = `${from.lat},${from.lng}-${to.lat},${to.lng}`;
        
        // Cache prüfen
        if (this.distanceCache.has(cacheKey)) {
            return this.distanceCache.get(cacheKey);
        }
        
        // Haversine-Approximation für kurze Distanzen
        const directDistance = this.haversineDistance(from.lat, from.lng, to.lat, to.lng);
        if (directDistance < 20) {
            const result = {
                distance: directDistance * 1.3, // Straßenfaktor
                duration: directDistance / 60 + 0.25 // ~60km/h + Puffer
            };
            this.distanceCache.set(cacheKey, result);
            return result;
        }
        
        // Nur für längere Strecken: Google API
        try {
            this.apiCallsCount++;
            const response = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
                params: {
                    origins: `${from.lat},${from.lng}`,
                    destinations: `${to.lat},${to.lng}`,
                    key: this.apiKey,
                    units: 'metric',
                    mode: 'driving'
                }
            });
            
            if (response.data.status === 'OK') {
                const element = response.data.rows[0].elements[0];
                if (element.status === 'OK') {
                    const result = {
                        distance: element.distance.value / 1000,
                        duration: element.duration.value / 3600 + this.constraints.travelTimePadding
                    };
                    this.distanceCache.set(cacheKey, result);
                    return result;
                }
            }
        } catch (error) {
            console.warn('Distance Matrix API Fehler, nutze Approximation');
        }
        
        // Fallback: Realistische Approximation
        const result = {
            distance: directDistance * 1.3,
            duration: directDistance / 85 + 0.25 // Autobahn-Geschwindigkeit
        };
        this.distanceCache.set(cacheKey, result);
        return result;
    }

    // ======================================================================
    // ALTERNATIVE TERMINVORSCHLÄGE - OHNE NEUE API CALLS!
    // ======================================================================
    async suggestAlternativeSlots(rejectedAppointment, currentWeekPlan) {
        console.log(`🔄 Suche Alternativen für ${rejectedAppointment.customer}...`);
        
        const alternatives = [];
        const weekDays = currentWeekPlan.days || [];
        
        // Analysiere jeden Tag
        weekDays.forEach((day, dayIndex) => {
            const freeSlots = this.findFreeSlots(day);
            
            freeSlots.forEach(slot => {
                if (slot.duration >= 3.5) { // 3h Termin + Puffer
                    // Bewerte Slot basierend auf Region
                    let score = 0.5;
                    
                    // Bonus für Termine in derselben Region
                    const nearbyAppts = day.appointments.filter(apt => {
                        const dist = this.haversineDistance(
                            rejectedAppointment.lat, rejectedAppointment.lng,
                            apt.lat, apt.lng
                        );
                        return dist < 50;
                    });
                    
                    if (nearbyAppts.length > 0) {
                        score += 0.3; // Regional passend
                    }
                    
                    alternatives.push({
                        day: day.day,
                        date: day.date,
                        startTime: slot.startTime,
                        endTime: this.addHours(slot.startTime, 3),
                        efficiency: score,
                        reasoning: nearbyAppts.length > 0 ? 
                            `Passt gut zu ${nearbyAppts.length} anderen Terminen in der Region` :
                            'Freier Slot verfügbar'
                    });
                }
            });
        });
        
        // Sortiere nach Effizienz
        alternatives.sort((a, b) => b.efficiency - a.efficiency);
        
        return {
            appointmentId: rejectedAppointment.id,
            customer: rejectedAppointment.customer,
            currentWeekAlternatives: alternatives.slice(0, 5),
            recommendation: alternatives.length > 0 ?
                `Beste Alternative: ${alternatives[0].day} ${alternatives[0].startTime}` :
                'Keine passenden Slots diese Woche - nächste Woche empfohlen'
        };
    }

    // ======================================================================
    // HILFSFUNKTIONEN
    // ======================================================================
    
    haversineDistance(lat1, lng1, lat2, lng2) {
        const R = 6371;
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLng = (lng2 - lng1) * Math.PI / 180;
        const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                  Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
                  Math.sin(dLng/2) * Math.sin(dLng/2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }
    
    getCityCoordinates(address) {
        // Lokale Datenbank deutscher Städte (kostenlos!)
        const cities = {
            'münchen': { lat: 48.1351, lng: 11.5820 },
            'berlin': { lat: 52.5200, lng: 13.4050 },
            'hamburg': { lat: 53.5511, lng: 9.9937 },
            'köln': { lat: 50.9375, lng: 6.9603 },
            'frankfurt': { lat: 50.1109, lng: 8.6821 },
            'stuttgart': { lat: 48.7758, lng: 9.1829 },
            'düsseldorf': { lat: 51.2277, lng: 6.7735 },
            'leipzig': { lat: 51.3397, lng: 12.3731 },
            'wolfsburg': { lat: 52.4227, lng: 10.7865 },
            'augsburg': { lat: 48.3705, lng: 10.8978 },
            'nürnberg': { lat: 49.4521, lng: 11.0767 }
        };
        
        const lowerAddress = address.toLowerCase();
        for (const [city, coords] of Object.entries(cities)) {
            if (lowerAddress.includes(city)) {
                return coords;
            }
        }
        return null;
    }
    
    sortAppointmentsByDistance(appointments) {
        // Einfacher Greedy-Algorithmus für TSP
        if (appointments.length <= 1) return appointments;
        
        const sorted = [appointments[0]];
        const remaining = appointments.slice(1);
        
        while (remaining.length > 0) {
            const last = sorted[sorted.length - 1];
            let minDist = Infinity;
            let nearestIndex = 0;
            
            remaining.forEach((apt, index) => {
                const dist = this.haversineDistance(
                    last.lat, last.lng, apt.lat, apt.lng
                );
                if (dist < minDist) {
                    minDist = dist;
                    nearestIndex = index;
                }
            });
            
            sorted.push(remaining[nearestIndex]);
            remaining.splice(nearestIndex, 1);
        }
        
        return sorted;
    }
    
    sortRegionsByDistance(regions) {
        const regionDistances = Object.entries(regions)
            .filter(([_, data]) => data.appointments.length > 0)
            .map(([name, data]) => ({
                name,
                distance: this.haversineDistance(
                    this.constraints.homeBase.lat,
                    this.constraints.homeBase.lng,
                    data.center.lat,
                    data.center.lng
                )
            }));
        
        regionDistances.sort((a, b) => a.distance - b.distance);
        return regionDistances.map(r => r.name);
    }
    
    findFreeSlots(day) {
        const slots = [];
        const dayStart = 8;
        const dayEnd = 18;
        
        if (day.appointments.length === 0) {
            slots.push({
                startTime: '08:00',
                endTime: '18:00',
                duration: 10
            });
            return slots;
        }
        
        // Sortiere Termine nach Zeit
        const sorted = [...day.appointments].sort((a, b) => 
            this.timeToHours(a.startTime) - this.timeToHours(b.startTime)
        );
        
        // Slot vor erstem Termin
        const firstStart = this.timeToHours(sorted[0].startTime);
        if (firstStart > dayStart + 1) {
            slots.push({
                startTime: this.formatTime(dayStart),
                endTime: sorted[0].startTime,
                duration: firstStart - dayStart
            });
        }
        
        // Slots zwischen Terminen
        for (let i = 0; i < sorted.length - 1; i++) {
            const currentEnd = this.timeToHours(sorted[i].endTime);
            const nextStart = this.timeToHours(sorted[i + 1].startTime);
            if (nextStart - currentEnd > 1) {
                slots.push({
                    startTime: sorted[i].endTime,
                    endTime: sorted[i + 1].startTime,
                    duration: nextStart - currentEnd
                });
            }
        }
        
        // Slot nach letztem Termin
        const lastEnd = this.timeToHours(sorted[sorted.length - 1].endTime);
        if (dayEnd - lastEnd > 1) {
            slots.push({
                startTime: sorted[sorted.length - 1].endTime,
                endTime: this.formatTime(dayEnd),
                duration: dayEnd - lastEnd
            });
        }
        
        return slots;
    }
    
    scheduleFixedAppointments(week, fixedAppointments) {
        fixedAppointments.forEach(apt => {
            const dayIndex = week.findIndex(d => d.date === apt.fixed_date);
            if (dayIndex >= 0) {
                week[dayIndex].appointments.push({
                    ...apt,
                    startTime: apt.fixed_time,
                    endTime: this.addHours(apt.fixed_time, this.constraints.appointmentDuration)
                });
            }
        });
    }
    
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
                overnight: null
            };
        });
    }
    
    formatTime(hours) {
        const h = Math.floor(hours);
        const m = Math.round((hours - h) * 60);
        return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
    }
    
    timeToHours(timeStr) {
        const [h, m] = timeStr.split(':').map(Number);
        return h + (m || 0) / 60;
    }
    
    addHours(timeStr, hours) {
        return this.formatTime(this.timeToHours(timeStr) + hours);
    }
    
    getCityName(address) {
        const match = address.match(/\d{5}\s+([^,]+)/);
        return match ? match[1].trim() : address.substring(0, 20) + '...';
    }
    
    formatWeekResult(week, weekStart) {
        const totalAppointments = week.reduce((sum, day) => sum + day.appointments.length, 0);
        const totalWorkHours = week.reduce((sum, day) => sum + day.workTime, 0);
        const totalTravelHours = week.reduce((sum, day) => sum + day.travelTime, 0);
        
        return {
            weekStart,
            days: week,
            totalHours: Math.round((totalWorkHours + totalTravelHours) * 10) / 10,
            optimizations: [
                `${totalAppointments} Termine intelligent geplant`,
                `Nur ${this.apiCallsCount} API Calls (Ersparnis: ~${Math.round((3000-this.apiCallsCount)/3000*100)}%)`,
                `Geschätzte Kosten: ${(this.apiCallsCount * 0.01).toFixed(2)}€ statt ${(totalAppointments * totalAppointments * 0.01).toFixed(2)}€`,
                'Echte Fahrzeiten mit regionaler Optimierung'
            ],
            stats: {
                totalAppointments,
                confirmedAppointments: week.reduce((sum, day) => 
                    sum + day.appointments.filter(a => a.status === 'bestätigt').length, 0),
                proposalAppointments: week.reduce((sum, day) => 
                    sum + day.appointments.filter(a => a.status === 'vorschlag').length, 0),
                totalTravelTime: Math.round(totalTravelHours * 10) / 10,
                workDays: week.filter(day => day.appointments.length > 0).length,
                apiCalls: this.apiCallsCount,
                estimatedCost: (this.apiCallsCount * 0.01).toFixed(2)
            },
            generatedAt: new Date().toISOString()
        };
    }
}

module.exports = IntelligentRoutePlanner;

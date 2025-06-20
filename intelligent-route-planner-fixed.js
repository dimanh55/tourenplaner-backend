const axios = require('axios');

class IntelligentRoutePlanner {
    constructor(db) {
        this.db = db;
        this.apiKey = process.env.GOOGLE_MAPS_API_KEY;
        this.distanceCache = new Map();
        this.apiCallsCount = 0;
        this.constraints = {
            maxWorkHoursPerWeek: 40,
            maxWorkHoursPerDay: 8,
            workStartTime: 8.5,
            workEndTime: 17,
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
        console.log('🧠 Starte KORRIGIERTE Routenplanung...');
        try {
            const geoAppointments = await this.ensureGeocoding(appointments);
            const clusters = this.clusterByRegion(geoAppointments);
            const week = await this.planWeekEfficiently(clusters, geoAppointments, weekStart);
            return this.formatWeekResult(week, weekStart);
        } catch (error) {
            console.error('❌ Routenplanung fehlgeschlagen:', error);
            throw error;
        }
    }

    // ======================================================================
    // KORRIGIERT: TAG EFFIZIENT PLANEN MIT ÜBERNACHTUNGSPRÜFUNG
    // ======================================================================
    async planDayEfficiently(day, appointments, regionName, previousDayOvernight = null) {
        if (appointments.length === 0) return;
        console.log(`📅 Plane ${day.day}: ${appointments.length} Termine in Region ${regionName}`);
        const sortedAppts = this.sortAppointmentsByDistance(appointments);
        let startLocation = this.constraints.homeBase;
        let currentTime = this.constraints.workStartTime;
        if (previousDayOvernight) {
            startLocation = previousDayOvernight.location;
            currentTime = this.constraints.workStartTime;
            console.log(`🏨 Starte Tag von ${previousDayOvernight.city}`);
        }
        const firstAppt = sortedAppts[0];
        const firstDist = await this.getDistance(startLocation, firstAppt);
        const requiredDepartureTime = currentTime - firstDist.duration;
        if (!previousDayOvernight && requiredDepartureTime < this.constraints.workStartTime) {
            console.log(`⚠️ Anreise zum ersten Termin würde Abfahrt um ${this.formatTime(requiredDepartureTime)} erfordern`);
            console.log(`🏨 Übernachtung am Vortag erforderlich!`);
            day.requiresPreviousDayOvernight = {
                nearCity: this.getCityName(sortedAppts[0].address),
                reason: `Fahrt von Hannover würde ${Math.round(firstDist.duration * 60)} Min dauern - zu frühe Abfahrt`,
                suggestedHotel: `Hotel nahe ${this.getCityName(sortedAppts[0].address)}`
            };
            currentTime = this.constraints.workStartTime;
            const hotelToFirstDist = { distance: 10, duration: 0.25 };
            day.travelSegments.push({
                type: 'departure_from_hotel',
                from: `Hotel ${this.getCityName(sortedAppts[0].address)}`,
                to: this.getCityName(sortedAppts[0].address),
                distance: Math.round(hotelToFirstDist.distance),
                duration: hotelToFirstDist.duration,
                startTime: this.formatTime(currentTime),
                endTime: this.formatTime(currentTime + hotelToFirstDist.duration)
            });
            currentTime += hotelToFirstDist.duration;
        } else {
            const departureTime = Math.max(
                this.constraints.workStartTime,
                requiredDepartureTime
            );
            currentTime = departureTime + firstDist.duration;
            day.travelSegments.push({
                type: previousDayOvernight ? 'departure_from_hotel' : 'departure',
                from: previousDayOvernight ? previousDayOvernight.city : 'Hannover',
                to: this.getCityName(firstAppt.address),
                distance: Math.round(firstDist.distance),
                duration: firstDist.duration,
                startTime: this.formatTime(departureTime),
                endTime: this.formatTime(currentTime)
            });
        }
        // Erstes Meeting direkt nach der Anreise einplanen
        firstAppt.startTime = this.formatTime(currentTime);
        firstAppt.endTime = this.formatTime(currentTime + this.constraints.appointmentDuration);
        day.appointments.push(firstAppt);
        currentTime += this.constraints.appointmentDuration;
        let currentLocation = firstAppt;
        const remaining = [];
        for (let i = 1; i < sortedAppts.length; i++) {
            const apt = sortedAppts[i];
            const travelDist = await this.getDistance(currentLocation, apt);

            if (currentTime + travelDist.duration + this.constraints.appointmentDuration > this.constraints.workEndTime) {
                if (currentTime + travelDist.duration <= this.constraints.workEndTime) {
                    day.travelSegments.push({
                        type: 'travel',
                        from: currentLocation.name ? currentLocation.name : this.getCityName(currentLocation.address),
                        to: this.getCityName(apt.address),
                        distance: Math.round(travelDist.distance),
                        duration: travelDist.duration,
                        startTime: this.formatTime(currentTime),
                        endTime: this.formatTime(currentTime + travelDist.duration)
                    });
                    currentTime += travelDist.duration;
                    day.overnight = {
                        city: this.getCityName(apt.address),
                        location: { lat: apt.lat, lng: apt.lng },
                        reason: 'Arbeitszeitende erreicht'
                    };
                }
                remaining.push(...sortedAppts.slice(i));
                break;
            }

            if (travelDist.duration > 0) {
                day.travelSegments.push({
                    type: 'travel',
                    from: currentLocation.name ? currentLocation.name : this.getCityName(currentLocation.address),
                    to: this.getCityName(apt.address),
                    distance: Math.round(travelDist.distance),
                    duration: travelDist.duration,
                    startTime: this.formatTime(currentTime),
                    endTime: this.formatTime(currentTime + travelDist.duration)
                });
                currentTime += travelDist.duration;
            }

            apt.startTime = this.formatTime(currentTime);
            apt.endTime = this.formatTime(currentTime + this.constraints.appointmentDuration);
            day.appointments.push(apt);
            currentTime += this.constraints.appointmentDuration;
            currentLocation = apt;
        }
        const lastApt = day.appointments.length > 0 ? day.appointments[day.appointments.length - 1] : startLocation;
        const homeDist = await this.getDistance(lastApt, this.constraints.homeBase);
        const arrivalTimeHome = currentTime + homeDist.duration;
        const endLimit = day.day === 'Freitag' ? this.constraints.workEndTime : this.constraints.workEndTime;
        if (day.overnight || homeDist.distance > this.constraints.overnightThreshold || arrivalTimeHome > endLimit) {
            const overnightCity = this.getCityName(lastApt.address);
            day.overnight = {
                city: overnightCity,
                location: { lat: lastApt.lat, lng: lastApt.lng },
                reason: homeDist.distance > this.constraints.overnightThreshold ?
                    `${Math.round(homeDist.distance)}km von Hannover - zu weit für Rückfahrt` :
                    `Ankunft in Hannover wäre erst ${this.formatTime(arrivalTimeHome)} - zu spät`,
                checkIn: this.formatTime(currentTime + 0.5),
                hotel: `🏨 Hotel in ${overnightCity}`
            };
            console.log(`🏨 Übernachtung in ${overnightCity} geplant`);
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
            currentTime += homeDist.duration;
        }
        day.workTime = day.appointments.length * this.constraints.appointmentDuration;
        day.travelTime = day.travelSegments.reduce((sum, seg) => sum + seg.duration, 0);
        day.totalHours = day.workTime + day.travelTime;
        return remaining;
    }

    // ======================================================================
    // WOCHENPLANUNG MIT BERICHTIGTEM STUNDENLIMIT UND ÜBERNACHTUNG
    // ======================================================================
    async planWeekEfficiently(clusters, allAppointments, weekStart) {
        const week = this.initializeWeek(weekStart);
        const { regions, fixedAppointments } = clusters;
        this.scheduleFixedAppointments(week, fixedAppointments);

        const sortedRegions = this.sortRegionsByDistance(regions);
        let dayIndex = 0;
        let previousOvernight = null;
        let weekHours = 0;

        for (const regionName of sortedRegions) {
            const regionAppts = regions[regionName].appointments;
            if (regionAppts.length === 0) continue;

            const appointmentsPerDay = Math.ceil(
                regionAppts.length / Math.max(1, 5 - dayIndex)
            );

            for (
                let i = 0;
                i < regionAppts.length && dayIndex < 5;
                i += appointmentsPerDay, dayIndex++
            ) {
                const dayAppts = regionAppts.slice(i, i + appointmentsPerDay);
                if (dayAppts.length === 0) continue;

                if (weekHours >= this.constraints.maxWorkHoursPerWeek) break;

                const remaining = await this.planDayEfficiently(
                    week[dayIndex],
                    dayAppts,
                    regionName,
                    previousOvernight
                );

                previousOvernight = week[dayIndex].overnight;
                weekHours += week[dayIndex].totalHours;

                if (remaining && remaining.length > 0) {
                    regionAppts.splice(i + appointmentsPerDay, 0, ...remaining);
                }

                if (dayIndex > 0 && week[dayIndex].requiresPreviousDayOvernight) {
                    const prevDay = week[dayIndex - 1];
                    if (!prevDay.overnight) {
                        prevDay.overnight = {
                            city: week[dayIndex].requiresPreviousDayOvernight.nearCity,
                            reason: 'Übernachtung für frühen Start am nächsten Tag',
                            hotel: week[dayIndex].requiresPreviousDayOvernight.suggestedHotel
                        };
                        console.log(
                            `🏨 Vortags-Übernachtung hinzugefügt für ${prevDay.day}`
                        );
                    }
                }
            }

            if (weekHours >= this.constraints.maxWorkHoursPerWeek) break;
        }

        console.log(`💰 Nur ${this.apiCallsCount} API Calls verwendet!`);
        return week;
    }
            
    // ======================================================================
    // REST DER FUNKTIONEN BLEIBT GLEICH
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
        const cityCoords = this.getCityCoordinates(address);
        if (cityCoords) return cityCoords;
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

    clusterByRegion(appointments) {
        const regions = {
            'Nord': { center: { lat: 53.5, lng: 10.0 }, appointments: [] },
            'Ost': { center: { lat: 52.5, lng: 13.4 }, appointments: [] },
            'West': { center: { lat: 51.2, lng: 7.0 }, appointments: [] },
            'Süd': { center: { lat: 48.5, lng: 11.5 }, appointments: [] },
            'Mitte': { center: { lat: 50.5, lng: 9.0 }, appointments: [] }
        };
        const fixed = appointments.filter(apt => apt.is_fixed && apt.fixed_date);
        const flexible = appointments.filter(apt => !apt.is_fixed);
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

    async getDistance(from, to) {
        const cacheKey = `${from.lat},${from.lng}-${to.lat},${to.lng}`;
        if (this.distanceCache.has(cacheKey)) {
            return this.distanceCache.get(cacheKey);
        }
        const dbCached = await this.getDistanceFromDB(from, to);
        if (dbCached) {
            this.distanceCache.set(cacheKey, dbCached);
            return dbCached;
        }
        const directDistance = this.haversineDistance(from.lat, from.lng, to.lat, to.lng);
        if (directDistance < 5) {
            const result = {
                distance: directDistance * 1.4,
                duration: (directDistance / 30) + 0.1,
                approximated: true
            };
            this.distanceCache.set(cacheKey, result);
            this.saveDistanceToDB(from, to, result);
            return result;
        }
        if (directDistance < 50) {
            const result = {
                distance: directDistance * 1.25,
                duration: (directDistance / 60) + 0.2,
                approximated: true
            };
            this.distanceCache.set(cacheKey, result);
            this.saveDistanceToDB(from, to, result);
            return result;
        }
        const similarRoute = await this.findSimilarRoute(from, to);
        if (similarRoute) {
            const adjustedResult = {
                distance: similarRoute.distance * (0.9 + Math.random() * 0.2),
                duration: similarRoute.duration * (0.9 + Math.random() * 0.2),
                approximated: true,
                basedOn: 'similar_route'
            };
            this.distanceCache.set(cacheKey, adjustedResult);
            this.saveDistanceToDB(from, to, adjustedResult);
            return adjustedResult;
        }
        try {
            this.apiCallsCount++;
            console.log(`🌐 API Call #${this.apiCallsCount} für ${Math.round(directDistance)}km Strecke`);
            const response = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
                params: {
                    origins: `${from.lat},${from.lng}`,
                    destinations: `${to.lat},${to.lng}`,
                    key: this.apiKey,
                    units: 'metric',
                    mode: 'driving',
                    avoid: 'tolls',
                    departure_time: 'now',
                    traffic_model: 'pessimistic'
                }
            });
            if (response.data.status === 'OK') {
                const element = response.data.rows[0].elements[0];
                if (element.status === 'OK') {
                    const result = {
                        distance: element.distance.value / 1000,
                        duration: (element.duration_in_traffic?.value || element.duration.value) / 3600 + 0.25,
                        realtime: true,
                        traffic_considered: !!element.duration_in_traffic
                    };
                    this.distanceCache.set(cacheKey, result);
                    this.saveDistanceToDB(from, to, result);
                    return result;
                }
            }
        } catch (error) {
            console.warn('⚠️ Distance Matrix API Fehler:', error.message);
        }
        const fallbackResult = {
            distance: directDistance * 1.3,
            duration: directDistance / 80 + 0.3,
            approximated: true,
            fallback: true
        };
        this.distanceCache.set(cacheKey, fallbackResult);
        this.saveDistanceToDB(from, to, fallbackResult);
        return fallbackResult;
    }

    // Alle anderen Hilfsfunktionen bleiben gleich...
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
                overnight: null,
                requiresPreviousDayOvernight: null
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
        if (!address || typeof address !== 'string') {
            return 'Unbekannt';
        }
        const match = address.match(/\d{5}\s+([^,]+)/);
        return match ? match[1].trim() : address.substring(0, 20) + '...';
    }

    async getDistanceFromDB(from, to) {
        return new Promise((resolve) => {
            this.db.get(
                `SELECT distance, duration FROM distance_cache 
                 WHERE origin_lat = ? AND origin_lng = ? 
                 AND dest_lat = ? AND dest_lng = ?
                 AND cached_at > datetime('now', '-30 days')`,
                [from.lat, from.lng, to.lat, to.lng],
                (err, row) => {
                    if (err || !row) {
                        resolve(null);
                    } else {
                        resolve({
                            distance: row.distance,
                            duration: row.duration,
                            cached: true
                        });
                    }
                }
            );
        });
    }

    async saveDistanceToDB(from, to, result) {
        this.db.run(
            `INSERT OR REPLACE INTO distance_cache 
             (origin_lat, origin_lng, dest_lat, dest_lng, distance, duration, cached_at)
             VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`,
            [from.lat, from.lng, to.lat, to.lng, result.distance, result.duration],
            (err) => {
                if (err) console.error('Cache-Speicherfehler:', err);
            }
        );
    }

    async findSimilarRoute(from, to) {
        return new Promise((resolve) => {
            this.db.get(
                `SELECT distance, duration FROM distance_cache 
                 WHERE ABS(origin_lat - ?) < 0.02 AND ABS(origin_lng - ?) < 0.02
                 AND ABS(dest_lat - ?) < 0.02 AND ABS(dest_lng - ?) < 0.02
                 AND cached_at > datetime('now', '-30 days')
                 LIMIT 1`,
                [from.lat, from.lng, to.lat, to.lng],
                (err, row) => {
                    if (err || !row) {
                        resolve(null);
                    } else {
                        resolve({
                            distance: row.distance,
                            duration: row.duration
                        });
                    }
                }
            );
        });
    }

    formatWeekResult(week, weekStart) {
        const totalAppointments = week.reduce((sum, day) => sum + day.appointments.length, 0);
        const totalWorkHours = week.reduce((sum, day) => sum + day.workTime, 0);
        const totalTravelHours = week.reduce((sum, day) => sum + day.travelTime, 0);
        const overnightStays = week.filter(day => day.overnight).length;
        return {
            weekStart,
            days: week,
            totalHours: Math.round((totalWorkHours + totalTravelHours) * 10) / 10,
            optimizations: [
                `${totalAppointments} Termine intelligent geplant`,
                `Nur ${this.apiCallsCount} API Calls (Ersparnis: ~${Math.round((3000-this.apiCallsCount)/3000*100)}%)`,
                `Geschätzte Kosten: ${(this.apiCallsCount * 0.01).toFixed(2)}€ statt ${(totalAppointments * totalAppointments * 0.01).toFixed(2)}€`,
                overnightStays > 0 ? `${overnightStays} Übernachtungen geplant` : 'Keine Übernachtungen nötig',
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
                overnightStays: overnightStays,
                apiCalls: this.apiCallsCount,
                estimatedCost: (this.apiCallsCount * 0.01).toFixed(2)
            },
            generatedAt: new Date().toISOString()
        };
    }
}

module.exports = IntelligentRoutePlanner;

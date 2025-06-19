class APIBudgetController {
    constructor(dailyBudgetEUR = 5.0) {
        this.dailyBudgetEUR = dailyBudgetEUR;
        this.costs = {
            geocoding: 0.005,           // $0.005 pro Anfrage
            distanceMatrix: 0.01,       // $0.01 pro Element
            distanceMatrixAdvanced: 0.02 // $0.02 mit Verkehrsdaten
        };
        
        // Tägliche Limits basierend auf Budget
        this.dailyLimits = {
            geocoding: Math.floor(dailyBudgetEUR / this.costs.geocoding * 0.3), // 30% für Geocoding
            distanceMatrix: Math.floor(dailyBudgetEUR / this.costs.distanceMatrix * 0.7) // 70% für Distance
        };
        
        // Zähler (sollte in DB gespeichert werden)
        this.todayUsage = {
            geocoding: 0,
            distanceMatrix: 0,
            totalCostEUR: 0,
            date: new Date().toISOString().split('T')[0]
        };
        
        console.log('💰 API Budget Controller initialisiert:');
        console.log(`   Tagesbudget: ${dailyBudgetEUR}€`);
        console.log(`   Max Geocoding Calls: ${this.dailyLimits.geocoding}`);
        console.log(`   Max Distance Matrix: ${this.dailyLimits.distanceMatrix}`);
    }
    
    // Prüfe ob API Call erlaubt ist
    canMakeAPICall(type) {
        this.checkNewDay();
        
        if (this.todayUsage.totalCostEUR >= this.dailyBudgetEUR) {
            console.log(`🛑 Tagesbudget von ${this.dailyBudgetEUR}€ erreicht!`);
            return false;
        }
        
        if (type === 'geocoding' && this.todayUsage.geocoding >= this.dailyLimits.geocoding) {
            console.log(`🛑 Geocoding Limit erreicht: ${this.todayUsage.geocoding}/${this.dailyLimits.geocoding}`);
            return false;
        }
        
        if (type === 'distanceMatrix' && this.todayUsage.distanceMatrix >= this.dailyLimits.distanceMatrix) {
            console.log(`🛑 Distance Matrix Limit erreicht: ${this.todayUsage.distanceMatrix}/${this.dailyLimits.distanceMatrix}`);
            return false;
        }
        
        return true;
    }
    
    // Registriere API Call
    registerAPICall(type, count = 1) {
        this.checkNewDay();
        
        const cost = this.costs[type] * count;
        this.todayUsage[type] = (this.todayUsage[type] || 0) + count;
        this.todayUsage.totalCostEUR += cost;
        
        console.log(`💵 API Call: ${type} x${count} = ${cost.toFixed(3)}€ (Heute: ${this.todayUsage.totalCostEUR.toFixed(2)}€)`);
        
        // Warnung bei 80% Budget
        if (this.todayUsage.totalCostEUR > this.dailyBudgetEUR * 0.8) {
            console.log(`⚠️ WARNUNG: 80% des Tagesbudgets verbraucht!`);
        }
        
        return {
            allowed: true,
            todaySpent: this.todayUsage.totalCostEUR,
            remainingBudget: Math.max(0, this.dailyBudgetEUR - this.todayUsage.totalCostEUR)
        };
    }
    
    // Reset bei neuem Tag
    checkNewDay() {
        const today = new Date().toISOString().split('T')[0];
        if (this.todayUsage.date !== today) {
            console.log('📅 Neuer Tag - Reset API Usage');
            this.todayUsage = {
                geocoding: 0,
                distanceMatrix: 0,
                totalCostEUR: 0,
                date: today
            };
        }
    }
    
    // Status abrufen
    getStatus() {
        this.checkNewDay();
        return {
            date: this.todayUsage.date,
            budget: {
                daily: this.dailyBudgetEUR,
                spent: this.todayUsage.totalCostEUR.toFixed(2),
                remaining: Math.max(0, this.dailyBudgetEUR - this.todayUsage.totalCostEUR).toFixed(2),
                percentage: Math.round((this.todayUsage.totalCostEUR / this.dailyBudgetEUR) * 100)
            },
            usage: {
                geocoding: `${this.todayUsage.geocoding}/${this.dailyLimits.geocoding}`,
                distanceMatrix: `${this.todayUsage.distanceMatrix}/${this.dailyLimits.distanceMatrix}`
            },
            limits: this.dailyLimits
        };
    }
}

// ======================================================================
// OPTIMIERTE DISTANCE MATRIX STRATEGIE
// ======================================================================

class SmartDistanceCalculator {
    constructor(budgetController) {
        this.budgetController = budgetController;
        this.localCache = new Map();
    }
    
    async calculateDistance(from, to, useAPI = false) {
        const cacheKey = `${from.lat},${from.lng}-${to.lat},${to.lng}`;
        
        // 1. Check Cache
        if (this.localCache.has(cacheKey)) {
            return this.localCache.get(cacheKey);
        }
        
        // 2. Berechne Luftlinie
        const directDistance = this.haversineDistance(
            from.lat, from.lng, to.lat, to.lng
        );
        
        // 3. Entscheide ob API nötig ist
        if (!useAPI || directDistance < 20 || !this.budgetController.canMakeAPICall('distanceMatrix')) {
            // Nutze lokale Schätzung
            const estimate = this.estimateRoadDistance(directDistance);
            this.localCache.set(cacheKey, estimate);
            return estimate;
        }
        
        // 4. API Call nur wenn Budget vorhanden
        try {
            // Hier würde der echte API Call stehen
            // Für jetzt: Simulation
            this.budgetController.registerAPICall('distanceMatrix', 1);
            
            const apiResult = {
                distance: directDistance * 1.25,
                duration: (directDistance / 80) + 0.3,
                method: 'api'
            };
            
            this.localCache.set(cacheKey, apiResult);
            return apiResult;
            
        } catch (error) {
            // Fallback auf Schätzung
            return this.estimateRoadDistance(directDistance);
        }
    }
    
    estimateRoadDistance(directDistance) {
        let factor, speed;
        
        if (directDistance < 10) {
            factor = 1.4;  // Stadt
            speed = 30;
        } else if (directDistance < 50) {
            factor = 1.3;  // Überland
            speed = 60;
        } else if (directDistance < 200) {
            factor = 1.2;  // Autobahn
            speed = 90;
        } else {
            factor = 1.15; // Lange Strecke
            speed = 100;
        }
        
        return {
            distance: directDistance * factor,
            duration: (directDistance * factor / speed) + 0.25,
            method: 'estimated',
            confidence: directDistance < 50 ? 'high' : 'medium'
        };
    }
    
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
}

// ======================================================================
// BATCH-OPTIMIERTE API CALLS
// ======================================================================

class BatchAPIOptimizer {
    constructor(budgetController) {
        this.budgetController = budgetController;
        this.pendingGeocoding = [];
        this.pendingDistances = [];
    }
    
    // Sammle Geocoding Anfragen
    queueGeocoding(address) {
        this.pendingGeocoding.push(address);
        
        // Batch bei 10 Adressen
        if (this.pendingGeocoding.length >= 10) {
            return this.flushGeocoding();
        }
        
        return null;
    }
    
    // Führe Batch Geocoding aus
    async flushGeocoding() {
        if (this.pendingGeocoding.length === 0) return [];
        
        const batch = this.pendingGeocoding.splice(0, 10);
        
        if (!this.budgetController.canMakeAPICall('geocoding')) {
            console.log('⚠️ Geocoding-Budget aufgebraucht - nutze lokale Schätzung');
            return batch.map(addr => this.estimateCoordinates(addr));
        }
        
        // Simuliere Batch API Call
        this.budgetController.registerAPICall('geocoding', batch.length);
        
        return batch.map(addr => ({
            address: addr,
            lat: 52.3759 + (Math.random() - 0.5),
            lng: 9.7320 + (Math.random() - 0.5),
            method: 'batch_api'
        }));
    }
    
    // Lokale Koordinaten-Schätzung
    estimateCoordinates(address) {
        const plzMatch = address.match(/\b(\d{5})\b/);
        if (!plzMatch) {
            return { address, lat: 52.3759, lng: 9.7320, method: 'fallback' };
        }
        
        const plzRegions = {
            '0': { lat: 51.05, lng: 13.74 },  // Dresden
            '1': { lat: 52.52, lng: 13.40 },  // Berlin
            '2': { lat: 53.55, lng: 9.99 },   // Hamburg
            '3': { lat: 52.38, lng: 9.73 },   // Hannover
            '4': { lat: 51.51, lng: 7.47 },   // Dortmund
            '5': { lat: 50.94, lng: 6.96 },   // Köln
            '6': { lat: 50.11, lng: 8.68 },   // Frankfurt
            '7': { lat: 48.78, lng: 9.18 },   // Stuttgart
            '8': { lat: 48.14, lng: 11.58 },  // München
            '9': { lat: 49.45, lng: 11.08 }   // Nürnberg
        };
        
        const region = plzRegions[plzMatch[1][0]] || plzRegions['5'];
        
        return {
            address,
            lat: region.lat + (Math.random() - 0.5) * 0.5,
            lng: region.lng + (Math.random() - 0.5) * 0.5,
            method: 'plz_estimate'
        };
    }
}

// Export für Node.js
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        APIBudgetController,
        SmartDistanceCalculator,
        BatchAPIOptimizer
    };
}

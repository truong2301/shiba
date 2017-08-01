'use strict';

const crypto =  require('crypto');

module.exports =
  { sha256:
      function(data) {
        let hash = crypto.createHash('sha256');
        hash.update(data);
        return hash.digest('hex');
      },
    isInvalidUsername:
      function(input) {
        if (typeof input !== 'string')      return 'NOT_STRING';
        if (input.length === 0)             return 'NOT_PROVIDED';
        if (input.length < 3)               return 'TOO_SHORT';
        if (input.length > 50)              return 'TOO_LONG';
        if (!/^[a-z0-9_\-]*$/i.test(input)) return 'INVALID_CHARS';
        if (input === '__proto__')          return 'INVALID_CHARS';
        return false;
      },
    formatTimeDiff:
      function(diff) {
        diff = Math.floor(diff / 1000);

        let s  = diff % 60; diff = Math.floor(diff/60);
        let m  = diff % 60; diff = Math.floor(diff/60);
        let h  = diff % 24; diff = Math.floor(diff/24);
        let d  = diff;

        let words = [];
        let elems = 0;
        if (d > 0) { words.push('' + d + 'd'); ++elems; }
        if (h > 0) { words.push('' + h + 'h'); ++elems; }
        if (elems >= 2) return words.join(' ');
        if (m > 0) { words.push('' + m + 'm'); ++elems; }
        if (elems >= 2) return words.join(' ');
        if (s > 0) { words.push('' + s + 's'); ++elems; }
        return words.join(' ');
      },
    formatFactor:
      function(f) {
        return (f/100).toFixed(2);
      },
    formatFactorShort:
      function(f) {
        if (f === 0) return '0';

        // Scale the f upfront. We could do that later to preserve
        // precision, but the code is obfuscated enough already.
        f /= 100;

        // Calculate the exponent that would be used in scientific
        // notation. We apply some selective rounding to overcome
        // numerical errors in calculating the base 10 log.
        let e = Math.log(f) / Math.LN10;
        e = Math.round(1e8 * e) / 1e8;
        e = Math.floor(e);

        // The modifier that we want to use, e.g. k or m.
        let mod;

        if (e < 4) {
          mod = '';
        } else if (e < 6) {
          mod = 'k';
          f /= 1e3;
          e -= 3;
        } else {
          mod = 'M';
          f /= 1e6;
          e -= 6;
        }

        // The number of decimal places right to the decimal point in
        // scientific notation that we wish to keep.
        let places;
        switch (e) {
        case 0:  places = 4; break;
        case 1:  places = 3; break;
        case 2:  places = 4; break;
        case 3:  places = 5; break;
        default: places = 0; break;
        }

        e = Math.min(e,places);
        f = Math.round(f / Math.pow(10, e-places));
        /* Make sure that the exponent is positive during rescaling. */
        f = e-places >= 0 ? f * Math.pow(10, e-places) : f / Math.pow(10, places-e);
        f = f.toFixed(Math.max(0, places-e));
        /* Remove unnecessary zeroes. */
        f = f.replace(/(\.[0-9]*[1-9])0*$|\.0*$/,'$1');

        return f + mod;
      },
    duration:
      function(cp) {
        return Math.ceil(this.inverseGrowth(cp + 1));
      },
    growthFunc:
      function(ms) {
        let r = 0.00006;
        return Math.floor(100 * Math.pow(Math.E, r * ms));
      },
    inverseGrowth:
      function(result) {
        let c = 16666.66666667;
        return c * Math.log(0.01 * result);
      },
    divisible:
      function(hash, mod) {
        /* Reduce the hash digit by digit to stay in the signed 32-bit integer range. */
        let val = hash.split('').reduce(function(r,d) {
          return ((r << 4) + parseInt(d,16)) % mod ; }, 0);
        return val === 0;
      },
    clientSeed:
      '000000000000000007a9a31ff7f07463d91af6b5454241d5faf282e5e0fe1b3a',
    crashPoint:
      function(serverSeed) {
        console.assert(typeof serverSeed === 'string');
        let hash =
          crypto
            .createHmac('sha256', serverSeed)
            .update(this.clientSeed)
            .digest('hex');

        // In 1 of 101 games the game crashes instantly.
        if (this.divisible(hash, 101))
          return 0;

        // Use the most significant 52-bit from the hash to calculate the crash point
        let h = parseInt(hash.slice(0,52/4),16);
        let e = Math.pow(2,52);

        return Math.floor((100 * e - h) / (e - h));
      },
    winProb:
      function (factor) {
        return 9900 / (101*(factor-1));
      },
    houseEdge:
      function (factor) {
        let p1,p2,p3;
        let v1,v2,v3;

        // Instant crash.
        p1 = 1/101;
        v1 = 1;

        // Player win.
        p2 = this.winProb(factor);
        v2 = - 0.01 - (factor - 100)/100;

        // Player loss.
        p3 = 1 - p1 - p2;
        v3 = 0.99;

        // Expected value.
        return p1 * v1 + p2 * v2 + p3 * v3;
      }
  };

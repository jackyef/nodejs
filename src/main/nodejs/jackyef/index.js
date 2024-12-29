import * as fs from 'node:fs';
import { Buffer } from 'node:buffer';

// Using hash as key improved the time from 2:16 to 1:45.
// We do this to avoid Buffer.toString().
// We now surpassed Edgar's total CPU time of 1:57
function hashBuffer(buffer) {
  let hash = 0;
  for (let i = 0; i < buffer.length; i++) {
    hash = (hash * 31 + buffer[i]) >>> 0; // Simple hash
  }
  return hash;
}
const stationHashMap = new Map()

const fileName = process.argv[2];
const stream = fs.createReadStream(fileName, {
  highWaterMark: 128 * 1024, // 128kb, following M4 Pro L1d cache size
});


const semicolon = ';'.charCodeAt(0);
const newline = '\n'.charCodeAt(0);
const minus = '-'.charCodeAt(0);
const dot = '.'.charCodeAt(0);
const zero = '0'.charCodeAt(0);
const map = new Map();
let buffer = Buffer.allocUnsafe(0)


stream.on('data', (chunk) => {
  // Should not happen
  if (typeof chunk === 'string') return;

  let offset = 0;

  // Prepend the previous buffer if it exists
  if (buffer && buffer.length) {
    chunk = Buffer.concat([buffer, chunk]);
    buffer = Buffer.allocUnsafe(0);
  }

  while (offset < chunk.length) {
    const newlineIndex = chunk.indexOf(newline, offset);
    
    if (newlineIndex === -1) {
      // Incomplete data, need to process next chunk.
      // Retain the unprocessed portion for the next chunk
      buffer = chunk.slice(offset);
      break;
    }
    const semicolonIndex = chunk.indexOf(semicolon, offset);
    
    const stationBuffer = chunk.slice(offset, semicolonIndex);
    const stationHash = hashBuffer(stationBuffer);
    const nameFromCache = stationHashMap.get(stationHash)
    const stationName = nameFromCache || stationBuffer.toString()
    if (!nameFromCache) stationHashMap.set(stationHash, stationName)

    const temp = fastParseFloat(chunk.slice(semicolonIndex + 1, newlineIndex));

    const existing = map.get(stationName);

    if (!existing) {
      map.set(stationName, {
        min: temp,
        max: temp,
        sum: temp,
        count: 1
      });
    } else {
      existing.min = Math.min(existing.min, temp);
      existing.max = Math.max(existing.max, temp);
      existing.sum += temp;
      existing.count += 1;
    }

    offset = newlineIndex + 1; // Move to the next entry
  }
});

stream.on('end', () => {
  stream.close()
  printCompiledResults(map)
})

/**
 * @typedef {Map<string, {min: number, max: number, sum: number, count: number}>} CalcResultsCont
 */

/**
 * @param {CalcResultsCont} compiledResults
 */
function printCompiledResults(compiledResults) {
  const sortedStations = Array.from(compiledResults.keys()).sort();

  process.stdout.write('{');
  for (let i = 0; i < sortedStations.length; i++) {
    if (i > 0) {
      process.stdout.write(', ');
    }
    const data = compiledResults.get(sortedStations[i]);
    process.stdout.write(sortedStations[i]);
    process.stdout.write('=');
    process.stdout.write(
      round(data.min / 10) +
        '/' +
        round(data.sum / 10 / data.count) +
        '/' +
        round(data.max / 10)
    );
  }
  process.stdout.write('}\n');
}

/**
 * @example
 * round(1.2345) // "1.2"
 * round(1.55) // "1.6"
 * round(1) // "1.0"
 *
 * @param {number} num
 * @returns {string}
 */
function round(num) {
  const fixed = Math.round(10 * num) / 10;

  return fixed.toFixed(1);
}

/**
 * 
 * @param {Buffer} input 
 * @returns 
 */
function fastParseFloat(input) {
  // This function improves the perf from parseFloat()
  // With this: 2:16
  // Without this: 2:56
  // Pretty close to Edgar's total CPU time of 1:57
  let isNegative = input[0] === minus;
  if (isNegative) {
    return -fastParseFloat(input.slice(1));
  }

  let inputLength = input.length;
  let pow = 0;
  let sum = 0;
  for (let i = inputLength - 1; i >= 0; i--) {
    if (input[i] !== dot) {
      sum += (input[i] - zero) * (10 ** pow)
      pow += 1;
    }
  }

  return sum
}

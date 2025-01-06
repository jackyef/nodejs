import * as fs from 'node:fs';

const fileName = process.argv[2];
const stream = fs.createReadStream(fileName);

const aggregations = new Map();

stream.on('end', () => {
  stream.close();
  printCompiledResults(aggregations);
})

const NEW_LINE_CHARACTER = '\n'.charCodeAt(0);
const SEMICOLON_CHARACTER = ';'.charCodeAt(0);
const LOOKING_FOR_SEMICOLON = 0;
const LOOKING_FOR_NEWLINE = 1;

let state = LOOKING_FOR_SEMICOLON;
let stationBuffer = Buffer.alloc(100); // Allocate 100 bytes buffer to store station name
let tempBuffer = Buffer.alloc(5); // Allocate 5 bytes buffer to store temperature
let stationBufferIndex = 0;
let tempBufferIndex = 0;

stream.on('data', (chunk) => {
  for (let i = 0; i < chunk.length; i++) {
    const byte = chunk[i];

    if (state === LOOKING_FOR_SEMICOLON) {
      if (byte === SEMICOLON_CHARACTER) {
        state = LOOKING_FOR_NEWLINE;
      } else {
        stationBuffer[stationBufferIndex] = byte;
        stationBufferIndex += 1;
      }
    } else if (state === LOOKING_FOR_NEWLINE) {
      if (byte === NEW_LINE_CHARACTER) {
        const temp = specificNumberConversion(tempBuffer, tempBufferIndex - 1);
        const stationName = stationBuffer.toString('utf-8', 0, stationBufferIndex);

        const existing = aggregations.get(stationName);

        if (!existing) {
          aggregations.set(stationName, {
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

        // Reset everything for next entry
        state = LOOKING_FOR_SEMICOLON;

        stationBufferIndex = 0;
        tempBufferIndex = 0;
      } else {
        tempBuffer[tempBufferIndex] = byte;
        tempBufferIndex += 1;
      }
    }
  }
})

const DOT_CHARACTER = '.'.charCodeAt(0);
const MINUS_CHARACTER = '-'.charCodeAt(0);
const ZERO_CHARACTER = '0'.charCodeAt(0);

function specificNumberConversion(buffer, lastIndex) {
  let value = 0;
  let pow = 0;

  for (let i = lastIndex; i >= 0; i--) {
    if (buffer[i] !== DOT_CHARACTER) {
      if (buffer[i] === MINUS_CHARACTER) {
        value *= -1;
      } else {
        value += (buffer[i] - ZERO_CHARACTER) * (10 ** pow++);
      }
    }
  }

  return value;
}

/**
 * @param {Map} aggregations
 *
 * @returns {void}
 */
function printCompiledResults(aggregations) {
  const sortedStations = Array.from(aggregations.keys()).sort();

  let result =
    '{' +
    sortedStations
      .map((station) => {
        const data = aggregations.get(station);
        return `${station}=${round(data.min / 10)}/${round(
          data.sum / 10 / data.count
        )}/${round(data.max / 10)}`;
      })
      .join(', ') +
    '}';

  console.log(result);
}

/**
 * @example
 * round(1.2345) // "1.2"
 * round(1.55) // "1.6"
 * round(1) // "1.0"
 *
 * @param {number} num
 *
 * @returns {string}
 */
function round(num) {
  const fixed = Math.round(10 * num) / 10;

  return fixed.toFixed(1);
}

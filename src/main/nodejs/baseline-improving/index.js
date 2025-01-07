import * as fs from 'node:fs';
import * as wt from 'node:worker_threads';
import * as os from 'node:os';
import * as fsp from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const aggregations = new Map();
const NEW_LINE_CHARACTER = '\n'.charCodeAt(0);

if (wt.isMainThread) {
  const __filename = fileURLToPath(import.meta.url);
  const THREADS_COUNT = os.cpus().length;
  const splitPoints = [];
  const fileName = process.argv[2];
  const fileHandle = await fsp.open(fileName, 'r'); // Open file in read-only mode
  const totalBytes = (await fileHandle.stat()).size;
  const maxBytesPerThread = Math.ceil(totalBytes / THREADS_COUNT)

  const chunkSize = 107;
  const bufferForReadingFile = Buffer.alloc(chunkSize);
  let bytesRead = 0;

  while (true) {
    bytesRead += maxBytesPerThread;

    if (bytesRead >= totalBytes) {
      splitPoints.push(totalBytes)
      fileHandle.close()
      break;
    }

    // Zero-fill the buffer to start from a clean slate
    bufferForReadingFile.fill(0)
    await fileHandle.read(bufferForReadingFile, 0, chunkSize, bytesRead);

    const newlineIndex = bufferForReadingFile.indexOf(NEW_LINE_CHARACTER)

    splitPoints.push(bytesRead + newlineIndex + 1)
  }

  const workers = [];
  let finishedWorker = 0;

  for (let i = 0; i < THREADS_COUNT; i++) {
    const worker =  new wt.Worker(__filename, {
      workerData: {
        fileName,
        start: i === 0 ? 0 : splitPoints[i - 1],
        end: splitPoints[i] || totalBytes
      }
    });
    workers.push(worker);
  }

  workers.forEach(worker => {
    worker.addListener('message',
      (workerResults) => {
        finishedWorker += 1

        // Merge the results
        workerResults.forEach((workerData, key) => {
          const masterData = aggregations.get(key)
          if (!masterData) {
            aggregations.set(key, workerData)
          } else {
            masterData.min = Math.min(masterData.min, workerData.min);
            masterData.max = Math.max(masterData.max, workerData.max);
            masterData.sum += workerData.sum;
            masterData.count += workerData.count;
          }
        })

        worker.terminate();
        
        if (finishedWorker === THREADS_COUNT) {
          printCompiledResults(aggregations)
          return
        }
      }
    );
  });
} else {
  const { fileName, start, end } = wt.workerData;
  const stream = fs.createReadStream(fileName, {
    start,
    end,
  });

  stream.on('end', () => {
    stream.close();

    wt.parentPort.postMessage(aggregations);
  });
  
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
}


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

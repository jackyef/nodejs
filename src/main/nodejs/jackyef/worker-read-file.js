import * as fs from 'node:fs';
import * as fsp from 'node:fs/promises';
import { Buffer } from 'node:buffer';
import * as wt from 'node:worker_threads';
import * as os from 'node:os';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);

const semicolon = ';'.charCodeAt(0);
const newline = '\n'.charCodeAt(0);
const minus = '-'.charCodeAt(0);
const dot = '.'.charCodeAt(0);
const zero = '0'.charCodeAt(0);

if (wt.isMainThread) {
  // This is the main thread. We create workers and assign them chunk of work.
  /**
   * @type {wt.Worker[]}
   */
  const workers = []
  const THREADS_COUNT = os.cpus().length
  const results = new Map();

  function stopWorkers() {
    workers.forEach(worker => {
      worker.terminate()
    })
  }

  const splitPoints = []
  // Open the file and read the chunk
  const fileName = process.argv[2];
  const fileHandle = await fsp.open(fileName, 'r'); // Open file in read-only mode
  const totalBytes = (await fileHandle.stat()).size;
  const maxBytesPerThread = Math.ceil(totalBytes / THREADS_COUNT)
  
  // 128bytes. Each entry can only be a maximum of 106bytes long, so we don't need a large buffer.
  // - 100 bytes of station name,
  // - 1 byte of semicolon character,
  // - 5 bytes of temperature characters (e.g.: -99.9)
  // - 1 byte of newline character
  const chunkSize = 128; 
  // Reusing a buffer for reading is almost 2x faster compared to a simple fs.createReadStream and listening to on('data')
  // I suspect this is because of the overhead of creating a new buffer for each chunk
  const reusedBufferForReading = Buffer.alloc(chunkSize); 
  let bytesRead = 0;

  while (true) {
    bytesRead += maxBytesPerThread;

    if (bytesRead >= totalBytes) {
      splitPoints.push(totalBytes)
      fileHandle.close()
      break;
    }

    // Time to find a new split point
    reusedBufferForReading.fill(0)
    await fileHandle.read(reusedBufferForReading, 0, chunkSize, bytesRead);

    const newlineIndex = reusedBufferForReading.indexOf(newline)

    splitPoints.push(bytesRead + newlineIndex + 1)
  }

  function createWorkers() {
    let finishedWorker = 0

    for (let i = 0; i < THREADS_COUNT; i++) {
      // Splitting the file evenly to each worker is fine, but the work for each
      // worker might not be evenly distributed. Some chunks maybe harder to process
      // even with same amount of bytes.
      // Perhaps a smarter work distribution could improve the performance here.
      const worker =  new wt.Worker(__filename, {
        workerData: {
          fileName,
          start: i === 0 ? 0 : splitPoints[i - 1],
          end: splitPoints[i] || totalBytes
        }
      });
      workers.push(worker);

      worker.addListener('message',
        /**
         * 
         * @param {Map} workerResults 
         * @returns 
         */
        (workerResults) => {
          finishedWorker += 1
          console.log('finishedWorker', finishedWorker)

          // Merge the results
          workerResults.forEach((workerData, key) => {
            const masterData = results.get(key)
            if (!masterData) {
              results.set(key, workerData)
            } else {
              masterData.min = Math.min(masterData.min, workerData.min);
              masterData.max = Math.max(masterData.max, workerData.max);
              masterData.sum += workerData.sum;
              masterData.count += workerData.count;
            }
          })
          
          if (finishedWorker === THREADS_COUNT) {
            stopWorkers()

            printCompiledResults(results)
            return
          }
        }
      );

      worker.addListener('error', err => {
        console.error(`Worker error:`, err);

        process.exit(1)
      });
    }
  }

  createWorkers()
} else {
  // This is a worker thread. It read the file from the given start and end point.
  // It will process them and send the result back to main thread to be merged.
  const results = new Map();
  const { fileName, start, end } = wt.workerData;

  const stream = fs.createReadStream(fileName, {
    start,
    end,
    highWaterMark: 128 * 1024 // 128kB, adjusted to L1d cache size of M4 Pro
  })

  let LOOKING_FOR_SEMICOLON = 0;
  let LOOKING_FOR_NEWLINE = 1;
  let state = LOOKING_FOR_SEMICOLON;

  const stationBuffer = Buffer.alloc(100); // Station name can be at most 100 bytes
  const tempBuffer = Buffer.alloc(5); // Temperature can be at most 5 bytes (e.g.: -99.9)
  let stationBufferCursor = 0
  let tempBufferCursor = 0

  stream.on('data', 
    /**
     * 
     * @param {Buffer} chunk 
     */
    (chunk) => {
      for (let i = 0; i < chunk.length; i++) {
        const byte = chunk[i];

        if (state === LOOKING_FOR_SEMICOLON) {
          if (byte === semicolon) {
            state = LOOKING_FOR_NEWLINE;
          } else {
            stationBuffer[stationBufferCursor] = byte;
            stationBufferCursor += 1;
          }
        } else if (state === LOOKING_FOR_NEWLINE) {
          if (byte === newline) {
            const temp = fastParseFloat(tempBuffer, tempBufferCursor - 1);
            const stationName = stationBuffer.toString('utf-8', 0, stationBufferCursor);

            const existing = results.get(stationName);

            if (!existing) {
              results.set(stationName, {
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

            stationBufferCursor = 0;
            tempBufferCursor = 0;
          } else {
            tempBuffer[tempBufferCursor] = byte;
            tempBufferCursor += 1;
          }
        }
      }
  });

  stream.on('end', () => {
    // Send the results back to main thread
    wt.parentPort.postMessage(results)
  })
}

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
 * @param {Buffer} input - 5 bytes buffer, contains text representation of temperature e.g.: -99.9
 * @param {number} lastIndex - Last index of the buffer that contains data
 * @returns 
 */
function fastParseFloat(input, lastIndex) {
  // We know all the temperature has 1 number behind the decimal point
  // So we can do some optimization here.
  // We also treat 12.3 as 123 to avoid precision issue
  let value = 0
  let pow = 0

  for (let i = lastIndex; i >= 0; i--) {
    if (input[i] !== dot) {
      if (input[i] === minus) {
        value *= -1;
      } else {
        value += (input[i] - zero) * (10 ** pow++);
      }
    }
  }

  return value
}

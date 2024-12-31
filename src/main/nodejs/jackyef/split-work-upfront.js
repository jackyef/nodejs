import * as fs from 'node:fs';
import { Buffer } from 'node:buffer';
import * as wt from 'node:worker_threads';
import * as os from 'node:os';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);

const WORKER_DONE_MESSAGE = 1;
const semicolon = ';'.charCodeAt(0);
const newline = '\n'.charCodeAt(0);
const minus = '-'.charCodeAt(0);
const dot = '.'.charCodeAt(0);
const zero = '0'.charCodeAt(0);

const stationHashMap = new Map();

function hashBuffer(buffer) {
  let hash = 0;
  for (let i = 0; i < buffer.length; i++) {
    hash = (hash * 31 + buffer[i]) >>> 0; // Simple hash
  }
  return hash;
}

if (wt.isMainThread) {
  // This is the main thread. We create workers and assign them chunk of work.
  /**
   * @type {wt.Worker[]}
   */
  const workers = []
  const THREADS_COUNT = os.cpus().length
  const results = new Map();

  function createWorker() {
    const worker =  new wt.Worker(__filename);

    workers.push(worker);

    return worker
  }

  function stopWorkers() {
    workers.forEach(worker => {
      worker.terminate()
    })
  }

  let finishedWorker = 0
  for (let i = 0; i < THREADS_COUNT; i++) {
    const worker = createWorker();

    worker.addListener('message',
      /**
       * 
       * @param {Map} workerResults 
       * @returns 
       */
      (workerResults) => {
        finishedWorker += 1

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

  // Open the file and read the chunk
  const fileName = process.argv[2];
  const totalBytes = fs.statSync(fileName).size;
  const stream = fs.createReadStream(fileName, {
    // 128kb, following M4 Pro L1d cache size
    // Or appropriately sized chunk to ensure each worker get at least 1 chunk
    highWaterMark: Math.min(128 * 1024, Math.ceil(totalBytes / THREADS_COUNT)), 
  });

  stream.on('end', () => {
    stream.close()

    workers.forEach(worker => {
      worker.postMessage(WORKER_DONE_MESSAGE)
    })
  })

  let buffer = Buffer.allocUnsafe(0)
  let workerIndex = 0
  stream.on('data', (chunk) => {
    // Should not happen
    if (typeof chunk === 'string') return;
  
    // Prepend the previous buffer if it exists
    if (buffer && buffer.length) {
      chunk = Buffer.concat([buffer, chunk]);
      buffer = Buffer.allocUnsafe(0);
    }

    const lastNewlineIndex = chunk.lastIndexOf(newline)

    if (lastNewlineIndex !== -1) {
      const passedArray = chunk.slice(0, lastNewlineIndex + 1)
      workers[workerIndex].postMessage(passedArray)
      // Store the remainder to be processed in the next batch
      buffer = chunk.slice(lastNewlineIndex + 1)
    } else {
      buffer = chunk
    }

    if (stream.bytesRead >= ((totalBytes / THREADS_COUNT) * (workerIndex + 1))) {
      // This worker has received enough data, we can stop sending more data to it
      workerIndex += 1
    }
  })
} else {
  // This is a worker thread. It will receive chunk of works.
  // It will process them and send the result back to main thread to be merged.
  const results = new Map();
  const textDecoder = new TextDecoder();
  const uint8ArrayToString = (uint8Array) => textDecoder.decode(uint8Array);

  wt.parentPort.on('message', 
    /**
     * 
     * @param {Buffer | 1} chunk 
     */
    (chunk) => {
      if (chunk === WORKER_DONE_MESSAGE) {
        wt.parentPort.postMessage(results)
        return
      }
      // console.log('[WORKER THREAD] Received chunk from parent:', chunk.slice(0, 100).toString()) 
      // This chunk is guaranteed to end on a newline character
      let offset = 0;
      while (offset < chunk.length) {
        const newlineIndex = chunk.indexOf(newline, offset);
        
        if (newlineIndex === -1) {
          // No more data, we can stop processing
          break;
        }

        const hasNextWork = chunk.indexOf(newline, newlineIndex + 1) !== -1
        const semicolonIndex = chunk.indexOf(semicolon, offset);
        const stationBuffer = chunk.slice(offset, semicolonIndex);
        const tempBuffer = chunk.slice(semicolonIndex + 1, newlineIndex)
            
        const stationHash = hashBuffer(stationBuffer);
        const nameFromCache = stationHashMap.get(stationHash)
        const stationName = nameFromCache || uint8ArrayToString(stationBuffer)
        if (!nameFromCache) stationHashMap.set(stationHash, stationName)

        const temp = fastParseFloat(tempBuffer);

        const existing = results.get(stationName);

        const cb = () => {
          if (!hasNextWork) {
            wt.parentPort.postMessage(results)
          }
        }

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
    
        offset = newlineIndex + 1; // Move to the next entry
      }
  });
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

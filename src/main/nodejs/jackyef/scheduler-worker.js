import * as fs from 'node:fs';
import { Buffer } from 'node:buffer';
import * as wt from 'node:worker_threads';
import * as os from 'node:os';
import { fileURLToPath } from 'node:url';
import * as SharedMap from './shared-map.js';

const __filename = fileURLToPath(import.meta.url);

const WORKER_DONE_MESSAGE = 1;

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

  function createWorker() {
    const worker =  new wt.Worker(__filename, {
      workerData: { typedArray: SharedMap.typedArray, stationNamesTypedArray: SharedMap.stationNamesTypedArray }
    });

    workers.push(worker);

    return worker
  }

  function stopWorkers() {
    workers.forEach(worker => {
      worker.terminate()
    })
  }

  let sentChunk = 0
  let receivedChunk = 0
  let allSent = false
  for (let i = 0; i < THREADS_COUNT; i++) {
    const worker = createWorker();

    worker.addListener('message',
      () => {
        receivedChunk += 1

        if (allSent && receivedChunk === sentChunk) {
          stopWorkers()
          printCompiledResults()
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
  const stream = fs.createReadStream(fileName, {
    highWaterMark: 128 * 1024, // 128kb, following M4 Pro L1d cache size
  });

  stream.on('end', () => {
    stream.close()
    allSent = true
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
      const nextWorker = workers[workerIndex]
      workerIndex = (workerIndex + 1) % THREADS_COUNT

      const passedArray = chunk.slice(0, lastNewlineIndex + 1)
      nextWorker.postMessage(passedArray)
      sentChunk += 1
      // Store the remainder to be processed in the next batch
      buffer = chunk.slice(lastNewlineIndex + 1)
    } else {
      buffer = chunk
    }
  })
} else {
  // This is a worker thread. It will receive chunk of works.
  // It will process them and send the result back to main thread to be merged.
  wt.parentPort.on('message', 
    /**
     * 
     * @param {Buffer | 1} chunk 
     */
    (chunk) => {
      if (chunk === WORKER_DONE_MESSAGE) {
        wt.parentPort.postMessage(WORKER_DONE_MESSAGE)
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

        
        const temp = fastParseFloat(tempBuffer);
        const existingCount = SharedMap.get(wt.workerData.typedArray, stationBuffer, 0);
        const cb = () => {
          if (!hasNextWork) wt.parentPort.postMessage(WORKER_DONE_MESSAGE)
        }

        // if (new TextDecoder().decode(stationBuffer) === 'İzmir') {
        //   console.log({ existingCount, temp })
        // }

        if (!existingCount) {
          SharedMap.set(wt.workerData.typedArray, stationBuffer, 1, SharedMap.COUNT_OFFSET);
          SharedMap.set(wt.workerData.typedArray, stationBuffer, temp, SharedMap.MIN_OFFSET);
          SharedMap.set(wt.workerData.typedArray, stationBuffer, temp, SharedMap.MAX_OFFSET);
          SharedMap.set(wt.workerData.typedArray, stationBuffer, temp, SharedMap.SUM_OFFSET);
          SharedMap.set(wt.workerData.stationNamesTypedArray, stationBuffer, stationBuffer, 0, cb);
        } else {
          SharedMap.set(wt.workerData.typedArray, stationBuffer, existingCount + 1, SharedMap.COUNT_OFFSET);
          
          const existingMin = SharedMap.get(wt.workerData.typedArray, stationBuffer, SharedMap.MIN_OFFSET);
          SharedMap.set(wt.workerData.typedArray, stationBuffer, Math.min(existingMin, temp), SharedMap.MIN_OFFSET);

          const existingMax = SharedMap.get(wt.workerData.typedArray, stationBuffer, SharedMap.MAX_OFFSET);
          SharedMap.set(wt.workerData.typedArray, stationBuffer, Math.max(existingMax, temp), SharedMap.MAX_OFFSET);

          const existingSum = SharedMap.get(wt.workerData.typedArray, stationBuffer, SharedMap.SUM_OFFSET);
          SharedMap.set(wt.workerData.typedArray, stationBuffer, temp + existingSum, SharedMap.SUM_OFFSET, cb);

          // console.log({
          //   name: new TextDecoder().decode(stationBuffer),
          //   existingCount,
          //   existingMin,
          //   existingMax,
          //   existingSum,
          // })
        }
    
        offset = newlineIndex + 1; // Move to the next entry
      }
  });
}

/**
 * @typedef {Map<string, {min: number, max: number, sum: number, count: number}>} CalcResultsCont
 */

function printCompiledResults() {
  const sortedStations = Array.from({ length: SharedMap.BLOCK_COUNT }).fill(0).map((_, i) => {
    const startIndex = i * SharedMap.STATION_NAME_BYTE_SIZE
    const slice = SharedMap.stationNamesTypedArray.slice(startIndex, startIndex + SharedMap.STATION_NAME_BYTE_SIZE).filter(Boolean)

    if (slice.length === 0) return null

    return { originalIndex: i, name: new TextDecoder().decode(slice) }
  }).filter(Boolean).sort((a, b) => a.name < b.name ? -1 : 1);

  const totalStationCount = sortedStations.length;

  // console.log('stored', SharedMap.typedArray);

  process.stdout.write('{');
  for (let i = 0; i < totalStationCount; i++) {
    if (i > 0) {
      process.stdout.write(', ');
    }
    const index = sortedStations[i].originalIndex * 4;
    const count = SharedMap.typedArray[index + SharedMap.COUNT_OFFSET];
    const min = SharedMap.typedArray[index + SharedMap.MIN_OFFSET];
    const max = SharedMap.typedArray[index + SharedMap.MAX_OFFSET];
    const sum = SharedMap.typedArray[index + SharedMap.SUM_OFFSET];

    // console.log('slice', 
    //   sortedStations[i].name,
    //   index, 
    //   SharedMap.typedArray.slice(index, index + 4), 
    //   new TextDecoder().decode(SharedMap.stationNamesTypedArray.slice(sortedStations[i].originalIndex * 100, sortedStations[i].originalIndex * 100 + 100)))

    process.stdout.write(sortedStations[i].name);
    process.stdout.write('=');
    process.stdout.write(
      round(min / 10) +
        '/' +
        round(sum / 10 / count) +
        '/' +
        round(max / 10)
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

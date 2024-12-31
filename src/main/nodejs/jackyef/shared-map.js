import * as wt from 'node:worker_threads';
/**
 * Simple implementation of Map using SharedArrayBuffer
 * This allows sharing data between threads without cloning
 */
// We want to store the data in such a way:
// [count, min, max, sum]
// [4bytes, 4bytes, 4bytes, 4bytes] = 16bytes


// Since there's can only be a maximum of 10k unique stations, 
// we'll need to prepare 160kB block of memory
// Allocate a SharedArrayBuffer
export const BLOCK_SIZE = 4;
// There can be 10k unique stations in the dataset
// We use 10007 as it's the closest prime number to 10k to reduce potential of collision
// for our hashing function
export const BLOCK_COUNT = 10_007; 

// Each station names can be up to 100 bytes
export const STATION_NAME_BYTE_SIZE = 100;

export const COUNT_OFFSET = 0; 
export const MIN_OFFSET = 1;
export const MAX_OFFSET = 2;
export const SUM_OFFSET = 3;

// ArrayBuffer is essentially an array of bytes. Since we want to store Int32, we multiply by 4
export const sharedBuffer = new SharedArrayBuffer(BLOCK_COUNT * BLOCK_SIZE * 4);
// Using Int32Array so each index holds 4 bytes
export const typedArray = new Int32Array(sharedBuffer);

export const stationNamesSharedBuffer = new SharedArrayBuffer(STATION_NAME_BYTE_SIZE * BLOCK_COUNT);
// Using Uint8Array to store station names as bytes
export const stationNamesTypedArray = new Uint8Array(stationNamesSharedBuffer);

/**
 * @type {Map<number, {name: string; index: number}>}
 */
export const stationNames = new Map();

const foundKeys = {};

function fnv1aHash(buffer) {
  let hash = 2166136261; // FNV offset basis
  for (let i = 0; i < buffer.length; i++) {
    hash ^= buffer[i];
    hash = (hash * 16777619) >>> 0; // 32-bit FNV-1a prime
  }
  return hash;
}

/**
 * @param {Buffer} key 
 * @param {number} entrySize
 * @param {Int32Array | Buffer} ta
 * @returns 
 */
export function getIndex(key, entrySize, ta) {
  let hash = fnv1aHash(key);
  const index = hash % BLOCK_COUNT;
  const actualIndex = index * entrySize;

  // TODO: This is still vulnerable to collision

  // if (foundKeys[index]) {
  //   if (foundKeys[index] !== new TextDecoder().decode(key) && new TextDecoder().decode(key) === 'İzmir') {
  //     console.error(`[COLLISION][id: ${wt.threadId}]`, {
  //       key: new TextDecoder().decode(key),
  //       index,
  //       hash,
  //       prev: foundKeys[index],
  //     })
  //   }
  // } else {
  //   foundKeys[index] = new TextDecoder().decode(key);
  // }
  return actualIndex;
}

/**
 * 
 * @param {Int32Array | Buffer} ta - TypedArray
 * @param {Buffer} key 
 * @param {number | Buffer} value
 * @param {number} offset
 * @param {() => void} [cb]
 */
export function set(ta, key, value, offset, cb) {
  const index = getIndex(key, typeof value === 'number' ? BLOCK_SIZE : STATION_NAME_BYTE_SIZE, ta);

  // console.log(`[id: ${wt.threadId}]`, {
  //   key: new TextDecoder().decode(key),
  //   index,
  //   value,
  //   offset,
  // })

  try {
    if (typeof value === 'number') {
      let result = Atomics.store(ta, index + offset, value);

      if (cb) cb()

      return result;
    } else {
      // It's an Uint8Array/Buffer
      value.forEach((byte, i) => {
        Atomics.store(ta, index + offset + i, byte);
      })
      
      if (cb) cb()
    }
  } catch (err) {
    console.error(`[id: ${wt.threadId}]`, err);
  }
}

/**
 * @param {Int32Array | Buffer} ta - TypedArray
 * @param {Buffer} key
 * @param {number} offset
 * @returns 
 */
export function get(ta, key, offset) {
  const index = getIndex(key, BLOCK_SIZE, ta);

  // console.log(`[id: ${wt.threadId}]`, {
  //   key: new TextDecoder().decode(key),
  //   index,
  // })

  try {
    return Atomics.load(ta, index + offset);
  } catch (err) {
    console.error(`[id: ${wt.threadId}]`, err);
  }
}


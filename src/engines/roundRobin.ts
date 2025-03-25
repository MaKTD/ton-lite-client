import EventEmitter from 'events';
import { TLFunction } from 'ton-tl';
import { LiteEngine } from './types';
import { sleep } from '../utils';

export interface LiteRoundRobinEngineOptions {
  defaultTimeout?: number
  maxErrorsBeforeThrow?: number
  maxAttemptsUntilFoundReadyEngine?: number
  retryEnginesDelay?: number
}

const defaults: Required<LiteRoundRobinEngineOptions> = {
  defaultTimeout: 5000,
  maxErrorsBeforeThrow: 20,
  maxAttemptsUntilFoundReadyEngine: 100,
  retryEnginesDelay: 100,
};

export class LiteRoundRobinEngine extends EventEmitter implements LiteEngine {
  private allEngines: LiteEngine[] = [];
  private readyEngines: LiteEngine[] = [];

  private closed = false;
  private enginesCounter = 0;
  private opts: Required<LiteRoundRobinEngineOptions>;

  constructor(engines: LiteEngine[], opts: LiteRoundRobinEngineOptions = {}) {
    super();

    this.opts = { ...defaults };
    if (opts.defaultTimeout) {
      this.opts.defaultTimeout = opts.defaultTimeout;
    }
    if (opts.maxErrorsBeforeThrow) {
      this.opts.maxErrorsBeforeThrow = opts.maxErrorsBeforeThrow;
    }
    if (opts.maxAttemptsUntilFoundReadyEngine) {
      this.opts.maxAttemptsUntilFoundReadyEngine = opts.maxAttemptsUntilFoundReadyEngine;
    }
    if (opts.retryEnginesDelay) {
      this.opts.retryEnginesDelay = opts.retryEnginesDelay;
    }

    for (const engine of engines) {
      this.addSingleEngine(engine);
    }
  }

  addSingleEngine(engine: LiteEngine) {
    const existing = this.allEngines.find(e => e === engine);
    if (existing) {
      throw new Error('Engine already exists');
    }

    this.allEngines.push(engine);

    engine.on('ready', () => {
      this.readyEngines.push(engine);
    });

    engine.on('close', () => {
      this.readyEngines = this.readyEngines.filter(e => e !== engine);
    });

    engine.on('error', () => {
      this.readyEngines = this.readyEngines.filter(e => e !== engine);
    });

    if (engine.isReady()) {
      this.readyEngines.push(engine);
    }
  }

  async query<REQ, RES>(f: TLFunction<REQ, RES>, req: REQ, args?: { timeout?: number, awaitSeqno?: number }): Promise<RES> {
    if (this.closed) {
      throw new Error('Engine is closed');
    }

    let id = (this.enginesCounter % this.readyEngines.length) || 0;
    this.enginesCounter += 1;
    if (this.enginesCounter % this.readyEngines.length === 0) {
      this.enginesCounter = 0;
    }
    let attempts = 0;
    let errorsCount = 0;

    while (true) {
      if (!this.readyEngines[id]?.isReady()) {
        id = ((id + 1) % this.readyEngines.length) || 0;
        attempts++;

        if (attempts >= this.readyEngines.length) {
          await sleep(this.opts.retryEnginesDelay);
        }
        if (attempts > this.opts.maxAttemptsUntilFoundReadyEngine) {
          throw new Error('No engines are available');
        }
        continue;
      }

      try {
        const finalArgs = { timeout: this.opts.defaultTimeout, ...args };
        const res = await this.readyEngines[id].query(f, req, finalArgs);

        return res;
      } catch (e) {
        id = ((id + 1) % this.readyEngines.length) || 0;
        errorsCount++;

        if (errorsCount > this.opts.maxErrorsBeforeThrow) {
          throw e;
        }

        await sleep(this.opts.retryEnginesDelay);
      }
    }
  }

  checkAtLeastOneReady() {
    for (let e of this.allEngines) {
      if (e.isReady()) {
        return true;
      }
    }

    return false;
  }

  close() {
    for (let q of this.allEngines) {
      q.close();
    }
    this.closed = true;
  }

  isClosed() {
    return this.closed;
  }

  isReady() {
    return !this.closed;
  }
}


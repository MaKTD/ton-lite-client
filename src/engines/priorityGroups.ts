import EventEmitter from 'events';
import { TLFunction } from 'ton-tl';
import { LiteRoundRobinEngine } from './roundRobin';
import { LiteEngine } from './types';

export interface LitePriorityGroupEngineOptions {
  maxErrorsUntilSuppressed?: number
  minAvailableToBeUnsuppressed?: number
  requireContinuousAvailability?: boolean
  errCounterTtlMs?: number
  debug?: (msg: string) => void,
}

const defaults: Required<LitePriorityGroupEngineOptions> = {
  maxErrorsUntilSuppressed: 10,
  minAvailableToBeUnsuppressed: 8,
  requireContinuousAvailability: false,
  errCounterTtlMs: 60_000,
  debug: () => {},
};

interface ControlledEngine {
  e: LiteRoundRobinEngine,
  suppressed: boolean
  errCounter: number
  availableCounter: number
  firstErrTs?: number
}

export class LitePriorityGroupsEngine extends EventEmitter implements LiteEngine {
  private opts: Required<LitePriorityGroupEngineOptions>;
  private engines: ControlledEngine[];
  private lastQueryTs: number = Date.now();
  private closed = false;

  constructor(engines: LiteRoundRobinEngine[], opts: LitePriorityGroupEngineOptions = {}) {
    super();

    if (engines.length <= 1) {
      throw new Error('LitePriorityGroupsEngine can work with at least 2 engines groups');
    }

    this.opts = { ...defaults };
    if (opts.maxErrorsUntilSuppressed) {
      this.opts.maxErrorsUntilSuppressed = opts.maxErrorsUntilSuppressed;
    }
    if (opts.minAvailableToBeUnsuppressed) {
      this.opts.minAvailableToBeUnsuppressed = opts.minAvailableToBeUnsuppressed;
    }
    if (typeof opts.requireContinuousAvailability === 'boolean') {
      this.opts.requireContinuousAvailability = opts.requireContinuousAvailability;
    }
    if (opts.errCounterTtlMs) {
      this.opts.errCounterTtlMs = opts.errCounterTtlMs;
    }
    if (opts.debug) {
      this.opts.debug = opts.debug;
    }

    this.engines = engines.map((e) => ({
      e,
      suppressed: false,
      errCounter: 0,
      availableCounter: 0,
    }));
  }

  async query<REQ, RES>(f: TLFunction<REQ, RES>, req: REQ, args?: { timeout?: number, awaitSeqno?: number }): Promise<RES> {
    if (this.closed) {
      throw new Error('Engine is closed');
    }

    this.lastQueryTs = Date.now();

    const suppressedGroups = this.engines.filter((g) => g.suppressed);
    for (let i = 0; i < suppressedGroups.length; i++) {
      const group = suppressedGroups[i];
      const ready = group.e.checkAtLeastOneReady();
      if (!ready && this.opts.requireContinuousAvailability) {
        group.availableCounter = 0;
      } else if (ready) {
        group.availableCounter += 1;
        this.opts.debug(`suppressed group No${i} signaled ready`);
      }

      if (group.availableCounter >= this.opts.minAvailableToBeUnsuppressed) {
        group.suppressed = false;
        group.errCounter = 0;
        group.availableCounter = 0;
        group.firstErrTs = undefined;
        this.opts.debug(`suppressed group No${i} was unsuppressed`);
      }
    }

    // last group can not be suppressed --> at least one not suppressed group always available
    const workingGroupIdx = this.engines.findIndex((g) => !g.suppressed);
    const workingGroup = this.engines[workingGroupIdx];

    try {
      return await workingGroup.e.query(f, req, args);
    } catch (err) {
      const isLastGroup = workingGroupIdx === (this.engines.length  - 1);
      if (isLastGroup) {
        throw err;
      }

      if (!workingGroup.firstErrTs) {
        workingGroup.firstErrTs = Date.now();
      }

      const diffMs = this.lastQueryTs - workingGroup.firstErrTs;
      if (diffMs >= this.opts.errCounterTtlMs) {
        this.opts.debug(`working group No${workingGroupIdx} err counter was reset due to ttl`);
        workingGroup.errCounter = 0;
        workingGroup.firstErrTs = Date.now();
      }

      workingGroup.errCounter += 1;
      if (workingGroup.errCounter >= this.opts.maxErrorsUntilSuppressed) {
        this.opts.debug(`working group No${workingGroupIdx} was suppressed, accumulated errors = ${workingGroup.errCounter}`);
        workingGroup.suppressed = true;
        workingGroup.errCounter = 0;
        workingGroup.availableCounter = 0;
        workingGroup.firstErrTs = undefined;
      }

      throw err;
    }
  }

  close() {
    for (let g of this.engines) {
      g.e.close();
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
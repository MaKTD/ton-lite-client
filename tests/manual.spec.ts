/* eslint-disable no-console */
import * as console from 'node:console';
import * as process from 'node:process';
import dotenv from 'dotenv';
import {
  LiteClient,
  LitePriorityGroupsEngine,
  LiteRoundRobinEngine,
  LiteSingleEngine,
} from '../src';
import { sleep } from '../src/utils';

dotenv.config({ path: './tests/.env.test' });

describe('manual spec', () => {
  it('priority groups manual tests', async () => {
    const firstGroupUrlStrRaw = process.env.LT_FIRST_GROUP!.split(',');
    const secondGroupUrlStrRaw = process.env.LF_SECOND_GROUP!.split(',');
    const groups = [firstGroupUrlStrRaw, secondGroupUrlStrRaw]
      .map((raw) => raw.map((c) => c.replaceAll(' ', '').replaceAll('\n', '')))
      .map((san) => san.map((c) => new URL(c)))
      .map((urls) => urls.map((url) => {
        if (!url.searchParams.get('publicKey')) {
          throw new Error(`lite client connection ${url.toString()}, missing publicKey search param`);
        }

        return {
          host: `tcp://${url.host}`,
          publicKey: Buffer.from(url.searchParams.get('publicKey')!, 'base64'),
          client: 'tcp' as 'tcp' | 'ws',
          reconnectTimeout: url.searchParams.get('reconnectTimeout') ? parseInt(url.searchParams.get('reconnectTimeout')!, 10) : undefined,
        };
      }))
      .map((cp) => cp.map((c) => new LiteSingleEngine(c)))
      .map((singles) => new LiteRoundRobinEngine(singles, { maxAttemptsUntilFoundReadyEngine: 20, retryEnginesDelay: 100, maxErrorsBeforeThrow: 2 }));

    const engine = new LitePriorityGroupsEngine(groups, {
      errCounterTtlMs: 15_000,
      maxErrorsUntilSuppressed: 5,
      minAvailableToBeUnsuppressed: 3,
      requireContinuousAvailability: true,
      debug: (msg) => console.log(msg),
    });
    const client = new LiteClient({
      engine,
      batchSize: 100,
      cacheMap: 10,
    });

    const totalQueries = 1_000_000;
    for (let i = 0; i < totalQueries; i++) {
      await sleep(500);
      console.log(`query ${i} running`);
      try {
        await client.getMasterchainInfo({ timeout: 500 });
        console.log(`query ${i} succeed`);
      } catch (err) {
        console.log(`query ${i} failed`);
      }
    }
  }, 1000 * 60 * 60 * 24);
});
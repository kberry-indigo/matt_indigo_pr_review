import express from 'express';
import http from 'http';
import pubsub from 'pubsub-js';

import { Async, Initializable } from '@indigo-ag/common';
import { AddressInfo } from 'net';

export type ServerConfig = {
  port: number;
  keepAliveTimeout?: number;
};

export enum ServerLifeCycle {
  CONNECTORS_READY = 'CONNECTORS_READY',
  MIDDLEWARE_READY = 'MIDDLEWARE_READY',
  STARTED = 'SERVER_STARTED',
  STOPPED = 'SERVER_STOPPED',
  REMOTE_SCHEMAS_FETCHING = 'REMOTE_SCHEMAS_FETCHING',
  REMOTE_SCHEMAS_FETCHED = 'REMOTE_SCHEMAS_FETCHED'
}

export const APPLICATION_READY_FUNC = 'isApplicationReady';

export class Server {
  public readonly app: express.Application;

  private readonly connectors: Array<Initializable<void>>;
  private readonly middleware: Array<Initializable<express.Application>>;

  private lifeCycleHistory: ServerLifeCycle[];

  private lifeCyclePending: ServerLifeCycle[];

  private httpServer: http.Server;

  constructor(connectors: Array<Initializable<void>>, middleware: Array<Initializable<express.Application>>) {
    this.connectors = connectors;
    this.middleware = middleware;
    this.lifeCycleHistory = [ServerLifeCycle.STOPPED];
    this.lifeCyclePending = [];
    this.app = express();

    // can be referenced in other middleware to determine if application has initilized
    this.app.set(APPLICATION_READY_FUNC, () => this.isApplicationReady);

    this.removePendingSchema = this.removePendingSchema.bind(this);
  }

  get isApplicationReady() {
    const isSchemaPending = this.lifeCyclePending.includes(ServerLifeCycle.REMOTE_SCHEMAS_FETCHING);
    const hasMiddlewareLoaded = this.lifeCycleHistory.includes(ServerLifeCycle.MIDDLEWARE_READY);

    return !isSchemaPending && hasMiddlewareLoaded;
  }

  start(config: ServerConfig) {
    this.lifeCycleHistory = [];

    this.on(ServerLifeCycle.MIDDLEWARE_READY).do(this.logStatus);

    this.on(ServerLifeCycle.REMOTE_SCHEMAS_FETCHED).do(this.removePendingSchema);

    this.on(ServerLifeCycle.REMOTE_SCHEMAS_FETCHING).do(() => {
      log.info('Remote Schema Fetching');
      this.lifeCyclePending.push(ServerLifeCycle.REMOTE_SCHEMAS_FETCHING);
    });

    this.startHttpServer(config);
    Promise.all(this.connectors.map(c => c.initialize()))
      .then(() => {
        log.info('Connectors Ready');
        this.publish(ServerLifeCycle.CONNECTORS_READY);
      })
      .then(() => {
        return Async.initializeInSequence(this.middleware, this.app);
      })
      .then(() => {
        log.info('Middleware Ready');
        this.publish(ServerLifeCycle.MIDDLEWARE_READY);
      });
  }

  stop() {
    if (this.httpServer) {
      this.httpServer.close();
      this.lifeCycleHistory = [];
      this.publish(ServerLifeCycle.STOPPED);
    } else {
      log.warn('No http server to stop');
    }
  }

  on(lifeCycleEvent: ServerLifeCycle) {
    return {
      do: (callback: (server: Server) => void) => {
        pubsub.subscribe(`IA.api.server.${lifeCycleEvent}`, (msg: string, server: Server) => callback(server));
      }
    };
  }

  getLifeCycleStatus(): Readonly<ServerLifeCycle[]> {
    return this.lifeCycleHistory;
  }

  private removePendingSchema() {
    const fetchingPos = this.lifeCyclePending.indexOf(ServerLifeCycle.REMOTE_SCHEMAS_FETCHING);
    this.lifeCyclePending.splice(fetchingPos, 1);

    this.lifeCycleHistory.push(ServerLifeCycle.REMOTE_SCHEMAS_FETCHED);

    // If not pending schemas notify of ready
    if (!this.lifeCycleHistory.some(cycle => cycle === ServerLifeCycle.REMOTE_SCHEMAS_FETCHING)) {
      log.info('Remote Schema Ready');
    }
  }

  private startHttpServer(config: ServerConfig) {
    this.httpServer = this.app.listen(config.port);
    if (config.keepAliveTimeout) {
      log.info(`Overriding HTTP server keepalive timeout setting to ${config.keepAliveTimeout} ms`);
      this.httpServer.keepAliveTimeout = config.keepAliveTimeout;
      // Workaround for https://github.com/nodejs/node/issues/27363 so the `keepAliveTimeout`
      // change above actually works; `headersTimeout` just needs to be longer than `keepAliveTimeout`.
      // May be able to be removed when using a future version of Node.
      this.httpServer.headersTimeout = config.keepAliveTimeout + 5000;
    }
    this.httpServer.on('listening', () => {
      this.publish(ServerLifeCycle.STARTED);
    });
  }

  private publish(lifeCycleEvent: ServerLifeCycle) {
    pubsub.publish(`IA.api.server.${lifeCycleEvent}`, this);
    this.lifeCycleHistory.push(lifeCycleEvent);
  }

  private logStatus(server: Server): void {
    const address: AddressInfo = server.httpServer.address() as AddressInfo;
    log.info(`Server listening on http://localhost:${address.port}`);
    log.info(`GraphQL playground listening on http://localhost:${address.port}/graphql`);
  }
}

import * as express from 'express';

import { Initializable } from '@indigo-ag/common';

import { APPLICATION_READY_FUNC } from '../core/server';

export type HealthConfig = {
  manifest: string;
};

const APPLICATION_ERROR_MSG = 'Application not ready';

export class Health implements Initializable<express.Application> {
  constructor(private readonly config: HealthConfig) {}

  initialize(app: express.Application) {
    const isApplicationReady = app.get(APPLICATION_READY_FUNC);

    const health = (req: express.Request, res: express.Response) => {
      // determine if application is ready to accept requests
      if (!isApplicationReady()) {
        res.status(500).send({ data: APPLICATION_ERROR_MSG });
        return;
      }
      res.send({ data: this.config.manifest });
    };
    app.use('/health', health);
    return Promise.resolve();
  }
}
